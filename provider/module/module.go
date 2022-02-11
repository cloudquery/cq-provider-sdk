package module

import (
	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/hashicorp/go-hclog"
)

// InfoReader is called when the user executes a module, to get provider supported metadata about the given module
type InfoReader func(logger hclog.Logger, module string, prefferedVersions []uint32) (version uint32, info map[string][]byte, diags diag.Diagnostics)
