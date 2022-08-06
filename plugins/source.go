package plugins

import (
	"context"
	"fmt"
	"sync"

	_ "embed"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/helpers/limit"
	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
	"github.com/rs/zerolog"
	"github.com/thoas/go-funk"
	"github.com/xeipuuv/gojsonschema"
	"golang.org/x/sync/semaphore"
)

//go:embed source_schema.json
var sourceSchema string

const ExampleSourceConfig = `
# max_goroutines to use when fetching. 0 means default and calculated by CloudQuery
# max_goroutines: 0
# By default cloudquery will fetch all tables in the source plugin
# tables: ["*"]
# skip_tables specify which tables to skip. especially useful when using "*" for tables
# skip_tables: []
`

// SourcePlugin is the base structure required to pass to sdk.serve
// We take a similar/declerative approach to API here similar to Cobra
type SourcePlugin struct {
	// Name of plugin i.e aws,gcp, azure etc'
	Name string
	// Version of the plugin
	Version string
	// Called upon configure call to validate and init configuration
	Configure func(context.Context, *SourcePlugin, spec.SourceSpec) (schema.ClientMeta, error)
	// Tables is all tables supported by this source plugin
	Tables []*schema.Table
	// Specific Spec
	Spec interface{}
	// JsonSchema for specific source plugin spec
	JsonSchema string
	// ExampleConfig is the example configuration for this plugin
	ExampleConfig string
	// Logger to call, this logger is passed to the serve.Serve Client, if not define Serve will create one instead.
	Logger zerolog.Logger

	// Internal fields set by configure
	clientMeta schema.ClientMeta
	spec       spec.SourceSpec
}

func (p *SourcePlugin) Init(ctx context.Context, s spec.SourceSpec) (*gojsonschema.Result, error) {
	res, err := spec.ValidateSpec(sourceSchema, s)
	if err != nil {
		return nil, err
	}
	if !res.Valid() {
		return res, nil
	}
	p.clientMeta, err = p.Configure(ctx, p, s)
	if err != nil {
		return nil, fmt.Errorf("failed to configure source plugin: %w", err)
	}
	p.spec = s
	return nil, nil
}

// Fetch fetches data acording to source configuration and
func (p *SourcePlugin) Fetch(ctx context.Context, res chan<- *schema.Resource) error {

	// if resources ["*"] is requested we will fetch all resources
	tables, err := p.interpolateAllResources(p.spec.Tables)
	if err != nil {
		return fmt.Errorf("failed to interpolate resources: %w", err)
	}

	// limiter used to limit the amount of resources fetched concurrently
	maxGoroutines := p.spec.MaxGoRoutines
	if maxGoroutines == 0 {
		maxGoroutines = limit.GetMaxGoRoutines()
	}
	p.Logger.Info().Uint64("max_goroutines", maxGoroutines).Msg("starting fetch")
	goroutinesSem := semaphore.NewWeighted(helpers.Uint64ToInt64(uint64(maxGoroutines)))

	wg := sync.WaitGroup{}
	for _, table := range p.Tables {
		t := table
		if funk.ContainsString(p.spec.SkipTables, table.Name) || !funk.ContainsString(tables, table.Name) {
			p.Logger.Info().Str("table", table.Name).Msg("skipping table")
			continue
		}
		clients := []schema.ClientMeta{p.clientMeta}
		if t.Multiplex != nil {
			clients = table.Multiplex(p.clientMeta)
		}
		for _, client := range clients {
			c := client
			if err := goroutinesSem.Acquire(ctx, 1); err != nil {
				// this can happen if context was cancelled so we just break out of the loop
				p.Logger.Error().Err(err).Msg("failed to acquire semaphore")
				break
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				t.Resolve(ctx, c, nil, res)
			}()
		}
	}
	wg.Wait()
	return nil
}

func (p *SourcePlugin) interpolateAllResources(tables []string) ([]string, error) {
	if !funk.ContainsString(tables, "*") {
		return tables, nil
	}

	if len(tables) > 1 {
		return nil, fmt.Errorf("invalid \"*\" resource, with explicit resources")
	}

	allResources := make([]string, 0, len(p.Tables))
	for _, k := range p.Tables {
		allResources = append(allResources, k.Name)
	}
	return allResources, nil
}

// func getTableDuplicates(resource string, table *schema.Table, tableNames map[string]string) error {
// 	for _, r := range table.Relations {
// 		if err := getTableDuplicates(resource, r, tableNames); err != nil {
// 			return err
// 		}
// 	}
// 	if existing, ok := tableNames[table.Name]; ok {
// 		return fmt.Errorf("table name %s used more than once, duplicates are in %s and %s", table.Name, existing, resource)
// 	}
// 	tableNames[table.Name] = resource
// 	return nil
// }
