package module

import (
	"embed"
	"path/filepath"
	"strconv"

	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/hashicorp/go-hclog"
)

// InfoReader is called when the user executes a module, to get provider supported metadata about the given module
type InfoReader func(logger hclog.Logger, module string, prefferedVersions []uint32) (resp Info, err error)

// Info is what the provider returns from an InfoReader request.
type Info struct {
	// Version of the supplied "info"
	Version uint32
	// Info, in the given version
	Info map[string][]*cqproto.ModuleFile
	// All versions supported by the provider, if any
	SupportedVersions []uint32
}

// EmbeddedReader returns an InfoReader handler given a "moduleData" filesystem.
// The fs should have all the required files for the modules in basedir, as one subdirectory per module ID.
// Each subdirectory (for the module ID) should contain one subdirectory per protocol version.
// Each protocol-version subdirectory can contain multiple files.
// Example: moduledata/drift/1/file.hcl (where "drift" is the module name and "1" is the protocol version)
func EmbeddedReader(moduleData embed.FS, basedir string) InfoReader {
	return func(logger hclog.Logger, module string, prefferedVersions []uint32) (Info, error) {
		var (
			resp Info
			err  error
		)

		resp.SupportedVersions, err = supportedVersions(moduleData, filepath.Join(basedir, module))
		if err != nil {
			return resp, err
		}

		for _, v := range prefferedVersions {
			dir := filepath.Join(basedir, module, strconv.FormatInt(int64(v), 10)) // <basedir>/<module>/<version>/
			data, err := flatFiles(moduleData, dir, "")
			if err != nil {
				return resp, err
			}
			if len(data) == 0 {
				continue
			}

			resp.Version = v
			resp.Info = map[string][]*cqproto.ModuleFile{
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

func supportedVersions(moduleData embed.FS, dir string) ([]uint32, error) {
	versionDirs, err := moduleData.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	versions := make([]uint32, 0, len(versionDirs))
	for _, f := range versionDirs {
		if !f.IsDir() {
			continue
		}
		vInt, err := strconv.ParseUint(f.Name(), 10, 32)
		if err != nil {
			continue
		}
		versions = append(versions, uint32(vInt))
	}

	return versions, nil
}

func flatFiles(moduleData embed.FS, dir, prefix string) ([]*cqproto.ModuleFile, error) {
	files, err := moduleData.ReadDir(dir)
	if err != nil {
		return nil, nil
	}

	var ret []*cqproto.ModuleFile
	for _, f := range files {
		name := filepath.Join(dir, f.Name())

		if !f.IsDir() {
			data, err := moduleData.ReadFile(name)
			if err != nil {
				return nil, err
			}
			ret = append(ret, &cqproto.ModuleFile{
				Name:     filepath.Join(prefix, f.Name()),
				Contents: data,
			})
			continue
		}

		// recurse and read subdirs
		sub, err := flatFiles(moduleData, name, f.Name())
		if err != nil {
			return nil, err
		}
		ret = append(ret, sub...)
	}

	return ret, nil
}
