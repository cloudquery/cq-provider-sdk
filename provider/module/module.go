package module

import (
	"embed"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/hashicorp/go-hclog"
)

// InfoReader is called when the user executes a module, to get provider supported metadata about the given module
type InfoReader func(logger hclog.Logger, module string, prefferedVersions []uint32) (resp InfoResponse, diags diag.Diagnostics)

// InfoResponse is what the provider returns from an InfoReader request.
type InfoResponse struct {
	// Version of the supplied "info"
	Version uint32
	// Info, in the given version
	Info map[string][]byte
	// All versions supported by the provider, if any
	SupportedVersions []uint32
}

// Serve returns an InfoReader handler given a "moduleData" filesystem.
// The fs should have all the required files for the modules in a moduledata/ directory, as one subdirectory per module ID.
// Filenames are the version IDs with an .hcl extension added.
func Serve(moduleData embed.FS) InfoReader {
	return func(logger hclog.Logger, module string, prefferedVersions []uint32) (resp InfoResponse, diags diag.Diagnostics) {
		for _, v := range prefferedVersions {
			fn := fmt.Sprintf("moduledata/%s/%v.hcl", module, v)
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

		files, err := moduleData.ReadDir(fmt.Sprintf("moduledata/%s", module))
		if err != nil {
			return resp, diag.Diagnostics{diag.NewBaseError(err, diag.INTERNAL)}
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

		return resp, nil
	}
}
