package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/helpers/limit"
	"github.com/cloudquery/cq-provider-sdk/plugin/source/schema"
	"github.com/rs/zerolog"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/semaphore"
)

// Config Every provider implements a resources field we only want to extract that in fetch execution
type Config interface {
	// Example returns a configuration example (with comments) so user clients can generate an example config
	Example() string
}

// SourceConfig is the shared configuration for all source plugins
type SourceConfig struct {
	MaxGoRoutines int         `json:"max_goroutines" yaml:"max_goroutines"`
	Tables        []string    `json:"tables" yaml:"tables"`
	SkipTables    []string    `json:"skip_tables" yaml:"skip_tables"`
	Configuration interface{} `json:"configuration" yaml:"configuration"`
}

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
	Config func() Config
	// Logger to call, this logger is passed to the serve.Serve Client, if not define Serve will create one instead.
	Logger zerolog.Logger
}

// Fetch fetches data acording to source configuration and
func (p *SourcePlugin) Fetch(ctx context.Context, config SourceConfig, res chan<- *schema.Resource) error {
	// var err error
	meta, err := p.Configure(p.Logger, config.Configuration)
	if err != nil {
		return fmt.Errorf("failed to configure provider: %w", err)
	}

	// if resources ["*"] is requested we will fetch all resources
	tables, err := p.interpolateAllResources(config.Tables)
	if err != nil {
		return fmt.Errorf("failed to interpolate resources: %w", err)
	}

	// limiter used to limit the amount of resources fetched concurrently
	var goroutinesSem *semaphore.Weighted
	maxGoroutines := config.MaxGoRoutines
	if maxGoroutines == 0 {
		maxGoroutines = limit.GetMaxGoRoutines()
	}
	p.Logger.Info().Int("max_goroutines", config.MaxGoRoutines).Msg("starting fetch")
	goroutinesSem = semaphore.NewWeighted(helpers.Uint64ToInt64(uint64(config.MaxGoRoutines)))

	wg := sync.WaitGroup{}
	for _, table := range p.Tables {
		t := table
		if funk.ContainsString(config.SkipTables, table.Name) || !funk.ContainsString(tables, table.Name) {
			p.Logger.Info().Str("table", table.Name).Msg("skipping table")
			continue
		}
		clients := []schema.ClientMeta{meta}
		if t.Multiplex != nil {
			clients = table.Multiplex(meta)
		}
		for _, client := range clients {
			c := client
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
	for k := range p.Tables {
		allResources = append(allResources, k.Name)
	}
	return allResources, nil
}

func getTableDuplicates(resource string, table *schema.Table, tableNames map[string]string) error {
	for _, r := range table.Relations {
		if err := getTableDuplicates(resource, r, tableNames); err != nil {
			return err
		}
	}
	if existing, ok := tableNames[table.Name]; ok {
		return fmt.Errorf("table name %s used more than once, duplicates are in %s and %s", table.Name, existing, resource)
	}
	tableNames[table.Name] = resource
	return nil
}
