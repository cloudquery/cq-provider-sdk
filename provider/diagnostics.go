package provider

import (
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
)

func DefaultErrorClassifier(t *schema.Table, err error) []diag.Diagnostic {
	return []diag.Diagnostic {
		diag.FromError(err, diag.ERROR, diag.RESOLVING, err.Error(), ""),
	}
}