package provider

import (
	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
)

// DefaultErrorClassifier defines default error classifier for providers that don't provide custom error classification
// for errors returned from fetch execution
func DefaultErrorClassifier(_ schema.ClientMeta, resourceName string, err error) diag.Diagnostics {
	return diag.Diagnostics{
		diag.NewBaseError(err, diag.ERROR, diag.RESOLVING, resourceName, err.Error(), ""),
	}
}
