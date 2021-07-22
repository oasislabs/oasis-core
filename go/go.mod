module github.com/oasisprotocol/oasis-core/go

replace (
	// Fixes vulnerabilities in etcd v3.3.{10,13} (dependencies via viper).
	// Can be removed once there is a spf13/viper release with updated
	// etcd and other dependencies using viper are updated.
	// https://github.com/spf13/viper/issues/956
	github.com/coreos/etcd => github.com/coreos/etcd v3.3.25+incompatible
	// Updates the version used by badgerdb, because some of the Go
	// module caches apparently have a messed up copy that causes
	// build failures.
	// https://github.com/google/flatbuffers/issues/6466
	github.com/google/flatbuffers => github.com/google/flatbuffers v1.12.1

	github.com/tendermint/tendermint => github.com/oasisprotocol/tendermint v0.34.9-oasis2

	golang.org/x/crypto/curve25519 => github.com/oasisprotocol/curve25519-voi/primitives/x25519 v0.0.0-20210505121811-294cf0fbfb43
	golang.org/x/crypto/ed25519 => github.com/oasisprotocol/curve25519-voi/primitives/ed25519 v0.0.0-20210505121811-294cf0fbfb43
)

require (
	github.com/blevesearch/bleve v1.0.14
	github.com/btcsuite/btcutil v1.0.2
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/dgraph-io/badger/v2 v2.2007.3
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/eapache/channels v1.1.0
	github.com/fxamacker/cbor/v2 v2.2.1-0.20200820021930-bafca87fa6db
	github.com/go-kit/log v0.1.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/btree v1.0.1
	github.com/hashicorp/go-hclog v0.16.2
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-plugin v1.4.2
	github.com/hpcloud/tail v1.0.0
	github.com/ianbruene/go-difflib v1.2.0
	github.com/libp2p/go-libp2p v0.14.3
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-pubsub v0.4.1
	github.com/multiformats/go-multiaddr v0.3.3
	github.com/oasisprotocol/curve25519-voi v0.0.0-20210505121811-294cf0fbfb43
	github.com/oasisprotocol/deoxysii v0.0.0-20200527154044-851aec403956
	github.com/powerman/rpc-codec v1.2.2
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.29.0
	github.com/prometheus/procfs v0.7.1
	github.com/seccomp/libseccomp-golang v0.9.1
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/tendermint/tendermint v0.34.9
	github.com/tendermint/tm-db v0.6.4
	github.com/thepudds/fzgo v0.2.2
	github.com/tyler-smith/go-bip39 v1.1.0
	github.com/whyrusleeping/go-logging v0.0.1
	gitlab.com/yawning/dynlib.git v0.0.0-20210614104444-f6a90d03b144
	go.dedis.ch/kyber/v3 v3.0.13
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2
	golang.org/x/net v0.0.0-20210525063256-abc453219eb5
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c
	google.golang.org/grpc v1.39.0
	google.golang.org/grpc/security/advancedtls v0.0.0-20200902210233-8630cac324bf
	google.golang.org/protobuf v1.27.1
)

go 1.16
