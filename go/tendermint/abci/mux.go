// Package abci implements the tendermint ABCI application integration.
package abci

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tendermint/iavl"
	"github.com/tendermint/tendermint/abci/types"
	tmcmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
	tmlog "github.com/tendermint/tendermint/libs/log"

	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/logging"
	"github.com/oasislabs/ekiden/go/tendermint/api"
)

const (
	// QueryKeyP2PFilterAddr is the standard ABCI query by IP address
	// used to determine if a peer is authorized to connect.
	QueryKeyP2PFilterAddr = "p2p/filter/addr/"

	// QueryKeyP2PFilterPubkey is the standard ABCI query by public key
	// used to determine if a peer is authorized to connect.
	QueryKeyP2PFilterPubkey = "p2p/filter/pubkey/"

	stateKeyGenesisDigest = "OasisGenesisDigest"
)

// TxOutput is the result of processing a transaction.
type TxOutput struct {
	// Output data to be serialized.
	Data interface{}

	// Tags used for easy searching/filtering transactions.
	Tags []tmcmn.KVPair
}

// Application is the interface implemented by multiplexed Oasis-specific
// ABCI applications.
type Application interface {
	// Name returns the name of the Application.
	//
	// Note: The name is also used as a prefix for de-multiplexing SetOption
	// and Query calls.
	Name() string

	// TransactionTag returns the transaction tag used to disambiguate
	// CheckTx and DeliverTx calls.
	TransactionTag() byte

	// Blessed returns true iff the Application should be considered
	// "blessed", and able to alter the validation set and handle the
	// access control related standard ABCI queries.
	//
	// Only one Application instance may be Blessed per multiplexer
	// instance.
	Blessed() bool

	// OnRegister is the function that is called when the Application
	// is registered with the multiplexer instance.
	OnRegister(state *ApplicationState)

	// OnCleanup is the function that is called when the ApplicationServer
	// has been halted.
	OnCleanup()

	// SetOption sets set an application option.
	//
	// It is expected that the key is prefixed by the application name
	// followed by a '/' (eg: `foo/<some key here>`).
	SetOption(types.RequestSetOption) types.ResponseSetOption

	// Query queries for state.
	//
	// It is expected that the path is prefixed by the application name
	// followed by a '/' (eg: `foo/<some path here>`), or only contains
	// the application name if the application does not use paths.
	//
	// Implementations MUST restrict their state operations to versioned
	// Get operations with versions <= BlockHeight.
	Query(types.RequestQuery) types.ResponseQuery

	// CheckTx validates a transaction via the mempool.
	//
	// Implementations MUST only alter the ApplicationState CheckTxTree.
	CheckTx([]byte) error

	// InitChain initializes the blockchain with validators and other
	// info from TendermintCore.
	InitChain(types.RequestInitChain) types.ResponseInitChain

	// BeginBlock signals the beginning of a block.
	BeginBlock(types.RequestBeginBlock)

	// DeliverTx delivers a transaction for full processing.
	DeliverTx([]byte) (*TxOutput, error)

	// EndBlock signals the end of a block, returning changes to the
	// validator set.
	EndBlock(types.RequestEndBlock) types.ResponseEndBlock

	// Commit is omitted because Applications will work on a cache of
	// the state bound to the multiplexer.
}

// ApplicationServer implements a tendermint ABCI application + socket server,
// that multiplexes multiple Oasis-specific "applications".
type ApplicationServer struct {
	mux         *abciMux
	cleanupOnce sync.Once
	quitChannel chan struct{}
	started     bool
}

// Start starts the ApplicationServer.
func (a *ApplicationServer) Start() error {
	return nil
}

// Stop stops the ApplicationServer.
func (a *ApplicationServer) Stop() {
	close(a.quitChannel)
}

// Quit returns a channel which is closed when the ApplicationServer is
// stopped.
func (a *ApplicationServer) Quit() <-chan struct{} {
	return a.quitChannel
}

// Cleanup cleans up the state of an ApplicationServer instance.
func (a *ApplicationServer) Cleanup() {
	a.cleanupOnce.Do(func() {
		a.mux.doCleanup()
	})
}

// Mux retrieve the abci Mux (or tendermint application) served by this server.
func (a *ApplicationServer) Mux() types.Application {
	return a.mux
}

// Register registers an Oasis application with the ABCI multiplexer.
//
// All registration must be done before Start is called.  ABCI operations
// that act on every single app (InitChain, BeginBlock, EndBlock) will be
// called in registration order.
func (a *ApplicationServer) Register(app Application) error {
	if a.started {
		return errors.New("mux: multiplexer already started")
	}

	return a.mux.doRegister(app)
}

// NewApplicationServer returns a new ApplicationServer, using the provided
// directory to persist state.
func NewApplicationServer(dataDir string) (*ApplicationServer, error) {
	mux, err := newABCIMux(dataDir)
	if err != nil {
		return nil, err
	}

	return &ApplicationServer{
		mux:         mux,
		quitChannel: make(chan struct{}),
	}, nil
}

type abciMux struct {
	types.BaseApplication

	logger *logging.Logger
	state  *ApplicationState

	appsByName     map[string]Application
	appsByTxTag    map[byte]Application
	appsByRegOrder []Application
	appBlessed     Application

	lastBeginBlock int64
}

func (mux *abciMux) Info(req types.RequestInfo) types.ResponseInfo {
	return types.ResponseInfo{
		LastBlockHeight:  mux.state.BlockHeight(),
		LastBlockAppHash: mux.state.BlockHash(),
	}
}

func (mux *abciMux) SetOption(req types.RequestSetOption) types.ResponseSetOption {
	app, err := mux.extractAppFromKeyPath(req.GetKey())
	if err != nil {
		mux.logger.Error("SetOption: failed to de-multiplex",
			"req", req,
			"err", err,
		)
		return types.ResponseSetOption{
			Code: api.CodeInvalidApplication.ToInt(),
		}
	}

	mux.logger.Debug("SetOption: dispatching",
		"app", app.Name(),
		"req", req,
	)

	return app.SetOption(req)
}

func (mux *abciMux) Query(req types.RequestQuery) types.ResponseQuery {
	queryPath := req.GetPath()

	// Tendermint uses these queries to filter incoming connections
	// by source address and or link(?) public key.  Offload the
	// responsiblity onto the blessed app.
	if isP2PFilterQuery(queryPath) {
		if mux.appBlessed != nil {
			mux.logger.Debug("Query: dispatching p2p/filter query",
				"req", req,
			)
			return mux.appBlessed.Query(req)
		}

		// There's no blessed app set, blindly allow everything.
		mux.logger.Debug("Query: allowing p2p/filter query",
			"req", req,
		)
		return types.ResponseQuery{
			Code: api.CodeOK.ToInt(),
		}
	}

	if mux.state.BlockHeight() == 0 {
		mux.logger.Error("Query: no committed blocks",
			"req", req,
		)
		return types.ResponseQuery{
			Code: api.CodeNoCommittedBlocks.ToInt(),
		}
	}

	app, err := mux.extractAppFromKeyPath(queryPath)
	if err != nil {
		mux.logger.Error("Query: failed to de-multiplex",
			"req", req,
			"err", err,
		)
		return types.ResponseQuery{
			Code: api.CodeInvalidApplication.ToInt(),
		}
	}

	mux.logger.Debug("Query: dispatching",
		"app", app.Name(),
		"req", req,
	)

	return app.Query(req)
}

func (mux *abciMux) CheckTx(tx []byte) types.ResponseCheckTx {
	app, err := mux.extractAppFromTx(tx)
	if err != nil {
		mux.logger.Error("CheckTx: failed to de-multiplex",
			"tx", hex.EncodeToString(tx),
		)
		return types.ResponseCheckTx{
			Code: api.CodeInvalidApplication.ToInt(),
		}
	}

	mux.logger.Debug("CheckTx: dispatching",
		"app", app.Name(),
		"tx", hex.EncodeToString(tx),
	)

	if err := app.CheckTx(tx[1:]); err != nil {
		return types.ResponseCheckTx{
			Code: api.CodeTransactionFailed.ToInt(),
			Info: err.Error(),
		}
	}

	return types.ResponseCheckTx{
		Code: api.CodeOK.ToInt(),
	}
}

func (mux *abciMux) InitChain(req types.RequestInitChain) types.ResponseInitChain {
	mux.logger.Debug("InitChain",
		"req", req,
	)

	// Stick the digest of the genesis block (the RequestInitChain) into
	// the state.
	//
	// This serves to keep bad things from happening if absolutely
	// nothing writes to the state till the Commit() call, along with
	// clearly separating chain instances based on the initialization
	// state, forever.
	tmp := bytes.NewBuffer(nil)
	types.WriteMessage(&req, tmp)
	genesisDigest := sha512.Sum512_256(tmp.Bytes())
	mux.state.deliverTxTree.Set([]byte(stateKeyGenesisDigest), genesisDigest[:])

	resp := mux.BaseApplication.InitChain(req)
	for _, app := range mux.appsByRegOrder {
		newResp := app.InitChain(req)
		if app.Blessed() {
			resp = newResp
		}
	}

	return resp
}

func (mux *abciMux) BeginBlock(req types.RequestBeginBlock) types.ResponseBeginBlock {
	blockHeight := mux.state.BlockHeight()
	mux.logger.Debug("BeginBlock",
		"req", req,
		"block_height", blockHeight,
	)

	// 99% sure this is a protocol violation.
	if mux.lastBeginBlock == blockHeight {
		panic("mux: redundant BeginBlock")
	}
	mux.lastBeginBlock = blockHeight

	for _, app := range mux.appsByRegOrder {
		app.BeginBlock(req)
	}

	return mux.BaseApplication.BeginBlock(req)
}

func (mux *abciMux) DeliverTx(tx []byte) types.ResponseDeliverTx {
	app, err := mux.extractAppFromTx(tx)
	if err != nil {
		mux.logger.Error("DeliverTx: failed to de-multiplex",
			"tx", hex.EncodeToString(tx),
		)
		return types.ResponseDeliverTx{
			Code: api.CodeInvalidApplication.ToInt(),
		}
	}

	mux.logger.Debug("DeliverTx: dispatching",
		"app", app.Name(),
		"tx", hex.EncodeToString(tx),
	)

	output, err := app.DeliverTx(tx[1:])
	if err != nil {
		return types.ResponseDeliverTx{
			Code: api.CodeTransactionFailed.ToInt(),
			Info: err.Error(),
		}
	}

	// Append application name tag.
	output.Tags = append(output.Tags, tmcmn.KVPair{api.TagApplication, []byte(app.Name())})

	return types.ResponseDeliverTx{
		Code: api.CodeOK.ToInt(),
		Data: cbor.Marshal(output.Data),
		Tags: output.Tags,
	}
}

func (mux *abciMux) EndBlock(req types.RequestEndBlock) types.ResponseEndBlock {
	mux.logger.Debug("EndBlock",
		"req", req,
		"block_height", mux.state.BlockHeight(),
	)

	resp := mux.BaseApplication.EndBlock(req)
	for _, app := range mux.appsByRegOrder {
		newResp := app.EndBlock(req)
		if app.Blessed() {
			resp = newResp
		}
	}

	return resp
}

func (mux *abciMux) Commit() types.ResponseCommit {
	if err := mux.state.doCommit(); err != nil {
		mux.logger.Error("Commit failed",
			"err", err,
		)

		// There appears to be no way to indicate to the caller that
		// this failed.
		panic(err)
	}

	mux.logger.Debug("Commit",
		"block_height", mux.state.BlockHeight(),
		"block_hash", hex.EncodeToString(mux.state.BlockHash()),
	)

	return types.ResponseCommit{Data: mux.state.BlockHash()}
}

func (mux *abciMux) doCleanup() {
	mux.state.doCleanup()
	for _, v := range mux.appsByRegOrder {
		v.OnCleanup()
	}
}

func (mux *abciMux) doRegister(app Application) error {
	name := app.Name()
	if mux.appsByName[name] != nil {
		return fmt.Errorf("mux: application already registered: '%s'", name)
	}
	if app.Blessed() {
		// Enforce the 1 blessed app limitation.
		if mux.appBlessed != nil {
			return fmt.Errorf("mux: blessed application already exists")
		}
		mux.appBlessed = app
	}

	mux.appsByName[name] = app
	mux.appsByRegOrder = append(mux.appsByRegOrder, app)
	mux.appsByTxTag[app.TransactionTag()] = app

	app.OnRegister(mux.state)
	mux.logger.Debug("Registered new application",
		"app", app.Name(),
	)

	return nil
}

func (mux *abciMux) extractAppFromKeyPath(s string) (Application, error) {
	appName := s
	if strings.Contains(appName, "/") {
		sVec := strings.SplitN(appName, "/", 2)
		appName = sVec[0]
	}

	app, ok := mux.appsByName[appName]
	if !ok {
		return nil, fmt.Errorf("mux: unknown app: '%s'", appName)
	}

	return app, nil
}

func (mux *abciMux) extractAppFromTx(tx []byte) (Application, error) {
	if len(tx) == 0 {
		return nil, fmt.Errorf("mux: invalid transaction")
	}

	app, ok := mux.appsByTxTag[tx[0]]
	if !ok {
		return nil, fmt.Errorf("mux: unknown transaction tag: 0x%x", tx[0])
	}

	return app, nil
}

func newABCIMux(dataDir string) (*abciMux, error) {
	state, err := newApplicationState(dataDir)
	if err != nil {
		return nil, err
	}

	mux := &abciMux{
		logger:         logging.GetLogger("abci-mux"),
		state:          state,
		appsByName:     make(map[string]Application),
		appsByTxTag:    make(map[byte]Application),
		lastBeginBlock: -1,
	}

	mux.logger.Debug("ABCI multiplexer initialized",
		"block_height", state.BlockHeight(),
		"block_hash", hex.EncodeToString(state.BlockHash()),
	)

	return mux, nil
}

// LogAdapter is a log adapter to allow tendermint to use ekiden logging.
type LogAdapter struct {
	*logging.Logger
}

// With implements the correspoding call in the tendermit Logger interface.
func (a *LogAdapter) With(keyvals ...interface{}) tmlog.Logger {
	// This is a bit silly, but the Ekiden logging's With returns a
	// pointer to it's Logger struct rather than the tendermint Logger
	// interface.
	return &LogAdapter{a.Logger.With(keyvals...)}
}

// ApplicationState is the overall past, present and future state
// of all multiplexed applications.
type ApplicationState struct {
	db            dbm.DB
	deliverTxTree *iavl.MutableTree
	checkTxTree   *iavl.MutableTree

	blockHash   []byte
	blockHeight int64
}

// BlockHeight returns the last committed block height.
func (s *ApplicationState) BlockHeight() int64 {
	return s.blockHeight
}

// BlockHash returns the last commited block hash.
func (s *ApplicationState) BlockHash() []byte {
	return append([]byte{}, s.blockHash...)
}

// DeliverTxTree returns the versioned tree to be used by queries
// to view comitted data, and transactions to build the next version.
func (s *ApplicationState) DeliverTxTree() *iavl.MutableTree {
	return s.deliverTxTree
}

// CheckTxTree returns the state tree to be used for modifications
// inside CheckTx (mempool connection) calls.
//
// This state is never persisted.
func (s *ApplicationState) CheckTxTree() *iavl.MutableTree {
	return s.checkTxTree
}

func (s *ApplicationState) doCommit() error {
	// Save the new version of the persistent tree.
	blockHash, blockHeight, err := s.deliverTxTree.SaveVersion()
	if err == nil {
		s.blockHash = blockHash
		s.blockHeight = blockHeight

		// Reset CheckTx state to latest version. This is safe because Tendermint
		// holds a lock on the mempool for commit.
		s.checkTxTree.Rollback()
		loadedVersion, cerr := s.checkTxTree.LoadVersion(blockHeight)
		if cerr != nil {
			panic(cerr)
		}

		if blockHeight != loadedVersion {
			panic("state: inconsistent trees")
		}
	}

	return err
}

func (s *ApplicationState) doCleanup() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

func newApplicationState(dataDir string) (*ApplicationState, error) {
	db, err := dbm.NewGoLevelDB("abci-mux-state", dataDir)
	if err != nil {
		return nil, err
	}

	// Figure out the latest version/hash if any, and use that
	// as the block height/hash.
	deliverTxTree := iavl.NewMutableTree(db, 128)
	blockHeight, err := deliverTxTree.Load()
	if err != nil {
		db.Close()
		return nil, err
	}
	blockHash := deliverTxTree.Hash()

	checkTxTree := iavl.NewMutableTree(db, 128)
	checkTxBlockHeight, err := checkTxTree.Load()
	if err != nil {
		db.Close()
		return nil, err
	}

	if blockHeight != checkTxBlockHeight || !bytes.Equal(blockHash, checkTxTree.Hash()) {
		db.Close()
		return nil, fmt.Errorf("state: inconsistent trees")
	}

	return &ApplicationState{
		db:            db,
		deliverTxTree: deliverTxTree,
		checkTxTree:   checkTxTree,
		blockHash:     blockHash,
		blockHeight:   blockHeight,
	}, nil
}

func isP2PFilterQuery(s string) bool {
	return strings.HasPrefix(s, QueryKeyP2PFilterAddr) || strings.HasPrefix(s, QueryKeyP2PFilterPubkey)
}
