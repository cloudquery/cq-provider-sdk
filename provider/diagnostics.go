package provider

import (
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
)

// DefaultErrorClassifier defines default error classifier for providers that don't provide custom error classification
// for errors returned from fetch execution
func DefaultErrorClassifier(_ schema.ClientMeta, _ *schema.Table, err error) []diag.Diagnostic {
	return []diag.Diagnostic{
		diag.FromError(err, diag.ERROR, diag.RESOLVING, err.Error(), ""),
	}
}
