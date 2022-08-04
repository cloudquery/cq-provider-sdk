package plugins

import (
	"context"

	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
	"github.com/rs/zerolog"
)

type DestinationPluginOptions struct {
	Logger zerolog.Logger
}
type NewDestinationPluginFunc func(ctx context.Context, spec spec.DestinationSpec, opts DestinationPluginOptions) (DestinationPlugin, error)

type DestinationPlugin interface {
	Save(ctx context.Context, resources []*schema.Resource) error
	CreateTables(ctx context.Context, table []*schema.Table) error
	ExampleConfig(ctx context.Context) string
}
