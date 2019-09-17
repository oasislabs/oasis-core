package env

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/oasislabs/ekiden/go/common"
)

const (
	cfgBaseDir          = "basedir"
	cfgBaseDirNoCleanup = "basedir.no_cleanup"
)

var (
	rootDir Dir

	// Flags has the configuration flags.
	Flags = flag.NewFlagSet("", flag.ContinueOnError)
)

// Dir is a directory for test data and output.
type Dir struct {
	dir       string
	noCleanup bool
}

// String returns the string representation (path) of the Dir.
func (d *Dir) String() string {
	return d.dir
}

// Init initializes the Dir, creating it iff it does not yet exist.
func (d *Dir) Init(cmd *cobra.Command) error {
	if d.dir != "" {
		return errors.New("env: base directory already initialized")
	}

	d.dir = viper.GetString(cfgBaseDir)
	d.noCleanup = viper.GetBool(cfgBaseDirNoCleanup)

	// Create a temporary directory using a prefix derived from the
	// command's `Use` field.
	var err error
	splitUse := strings.Split(cmd.Use, " ")
	if d.dir, err = ioutil.TempDir(d.dir, splitUse[0]); err != nil {
		return errors.Wrap(err, "env: failed to create default base directory")
	}

	return nil
}

// SetNoCleanup enables/disables the removal of the Dir on Cleanup.
func (d *Dir) SetNoCleanup(v bool) {
	d.noCleanup = v
}

// NewSubDir creates a new subdirectory under a Dir, and returns the
// sub-directory's Dir.
func (d *Dir) NewSubDir(subDirName string) (*Dir, error) {
	dirName := filepath.Join(d.String(), subDirName)
	if err := common.Mkdir(dirName); err != nil {
		return nil, errors.Wrap(err, "env: failed to create sub-directory")
	}

	return &Dir{
		dir:       dirName,
		noCleanup: d.noCleanup,
	}, nil
}

// NewLogWriter creates a log file under a Dir with the provided name.
func (d *Dir) NewLogWriter(name string) (io.WriteCloser, error) {
	fn := filepath.Join(d.String(), name)
	w, err := os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "env: failed to create file for append")
	}

	return w, nil
}

// Cleanup cleans up the Dir.
func (d *Dir) Cleanup() {
	if d.dir == "" || d.noCleanup {
		return
	}

	_ = os.RemoveAll(d.dir)
	d.dir = ""
}

// GetRootDir returns the global root Dir instance.
//
// Warning: This is not guaranteed to be valid till after `Dir.Init` is
// called.  Use of this routine from outside `ekiden-test-runner/cmd` is
// strongly discouraged.
func GetRootDir() *Dir {
	return &rootDir
}

func init() {
	Flags.String(cfgBaseDir, "", "test base directory")
	Flags.Bool(cfgBaseDirNoCleanup, false, "do not cleanup test base directory")

	_ = viper.BindPFlags(Flags)
}
