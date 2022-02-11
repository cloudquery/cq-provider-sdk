package module

import (
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

	// Other versions supported by the provider, if any
	OtherVersions []uint32
}
