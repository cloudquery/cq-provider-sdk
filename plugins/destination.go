package plugins

import (
	"context"

	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
)

type DestinationPlugin interface {
	Configure(ctx context.Context, spec *spec.DestinationSpec) error
	Save(ctx context.Context, resources []*schema.Resource) error
	CreateTables(ctx context.Context, table []*schema.Table) error
}
