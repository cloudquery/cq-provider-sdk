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
	"gopkg.in/yaml.v3"
)

//go:embed source_schema.json
var sourceConfigSchema string

const ExampleSourceConfig = `# max_goroutines to use when fetching. 0 means default and calculated by CloudQuery
max_goroutines: 0
# By default cloudquery will fetch all tables in the source plugin
tables: ["*"]
# skip_tables specify which tables to skip. especially useful when using "*" for tables
skip_tables: []
`

// Provider is the base structure required to pass and serve an sdk provider.Provider
type SourcePlugin struct {
	// Name of plugin i.e aws,gcp, azure etc'
	Name string
	// Version of the provider
	Version string
	// Configure the plugin and return the context
	Configure func(zerolog.Logger, interface{}) (schema.ClientMeta, error)
	// Tables is all tables supported by this plugin
	Tables []*schema.Table
	// Configuration decoded from configure request
	Config func() interface{}
	// ExampleConfig is the example configuration for this plugin
	ExampleConfig string
	// Logger to call, this logger is passed to the serve.Serve Client, if not define Serve will create one instead.
	Logger zerolog.Logger
}

// Fetch fetches data acording to source configuration and
func (p *SourcePlugin) Fetch(ctx context.Context, config []byte, res chan<- *schema.Resource) (*gojsonschema.Result, error) {
	sourceConfig := spec.SourceSpec{}
	if err := yaml.Unmarshal(config, &sourceConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal generic configuration: %w", err)
	}
	schemaLoader := gojsonschema.NewStringLoader(sourceConfigSchema)
	documentLoader := gojsonschema.NewGoLoader(sourceConfig)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return nil, fmt.Errorf("failed to validate configuration: %w", err)
	}
	if !result.Valid() {
		return result, nil
	}

	pluginConfig := p.Config()
	if err := sourceConfig.Configuration.Decode(pluginConfig); err != nil {
		return nil, fmt.Errorf("failed to decode specific configuration: %w", err)
	}

	// var err error
	meta, err := p.Configure(p.Logger, pluginConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure provider: %w", err)
	}
	if meta == nil {
		return nil, fmt.Errorf("failed to configure provider: Configure can't return nil")
	}

	// if resources ["*"] is requested we will fetch all resources
	tables, err := p.interpolateAllResources(sourceConfig.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate resources: %w", err)
	}

	// limiter used to limit the amount of resources fetched concurrently
	maxGoroutines := sourceConfig.MaxGoRoutines
	if maxGoroutines == 0 {
		maxGoroutines = limit.GetMaxGoRoutines()
	}
	p.Logger.Info().Uint64("max_goroutines", maxGoroutines).Msg("starting fetch")
	goroutinesSem := semaphore.NewWeighted(helpers.Uint64ToInt64(uint64(maxGoroutines)))

	wg := sync.WaitGroup{}
	for _, table := range p.Tables {
		t := table
		if funk.ContainsString(sourceConfig.SkipTables, table.Name) || !funk.ContainsString(tables, table.Name) {
			p.Logger.Info().Str("table", table.Name).Msg("skipping table")
			continue
		}
		clients := []schema.ClientMeta{meta}
		if t.Multiplex != nil {
			clients = table.Multiplex(meta)
		}
		for _, client := range clients {
			c := client
			if err := goroutinesSem.Acquire(ctx, 1); err != nil {
				// this can happen if context was cancelled so we just break out of the loop
				c.Logger().Error().Err(err).Msg("failed to acquire semaphore")
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
	return nil, nil
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
