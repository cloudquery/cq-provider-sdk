package module

import (
	"embed"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// InfoReader is called when the user executes a module, to get provider supported metadata about the given module
type InfoReader func(logger hclog.Logger, module string, prefferedVersions []uint32) (resp Info, err error)

// Info is what the provider returns from an InfoReader request.
type Info struct {
	// Version of the supplied "info"
	Version uint32
	// Info, in the given version
	Info map[string][]byte
	// All versions supported by the provider, if any
	SupportedVersions []uint32
}

// EmbeddedReader returns an InfoReader handler given a "moduleData" filesystem.
// The fs should have all the required files for the modules in basedir, as one subdirectory per module ID.
// Filenames are the version IDs with an .hcl extension added.
func EmbeddedReader(moduleData embed.FS, basedir string) InfoReader {
	return func(logger hclog.Logger, module string, prefferedVersions []uint32) (Info, error) {
		var resp Info

		files, err := moduleData.ReadDir(filepath.Join(basedir, module))
		if err != nil {
			return resp, err
		}

		for _, f := range files {
			if f.IsDir() {
				continue
			}
			vInt, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), ".hcl"), 10, 32)
			if err != nil {
				continue
			}
			resp.SupportedVersions = append(resp.SupportedVersions, uint32(vInt))
		}

		for _, v := range prefferedVersions {
			fn := filepath.Join(basedir, module, fmt.Sprintf("%v.hcl", v))
			data, err := moduleData.ReadFile(fn)
			if err != nil {
				continue
			}

			resp.Version = v
			resp.Info = map[string][]byte{
				"info": data,
			}
			break
		}
		if resp.Version == 0 {
			logger.Warn("received unsupported module info request", "module", module, "preferred_versions", prefferedVersions)
		}

		return resp, nil
	}
}
