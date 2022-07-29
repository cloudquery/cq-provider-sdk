package source

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/helpers/limit"
	"github.com/cloudquery/cq-provider-sdk/plugin/source/pb"
	"github.com/cloudquery/cq-provider-sdk/provider/execution"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/rs/zerolog"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Config Every provider implements a resources field we only want to extract that in fetch execution
type Config interface {
	// Example returns a configuration example (with comments) so user clients can generate an example config
	Example() string
}

// Provider is the base structure required to pass and serve an sdk provider.Provider
type Provider struct {
	// Name of plugin i.e aws,gcp, azure etc'
	Name string
	// Version of the provider
	Version string
	// Configure the plugin and return the context
	Configure func(zerolog.Logger, interface{}) (schema.ClientMeta, error)
	// ResourceMap is all resources supported by this plugin
	ResourceMap map[string]*schema.Table
	// Configuration decoded from configure request
	Config func() Config
	// Logger to call, this logger is passed to the serve.Serve Client, if not define Serve will create one instead.
	Logger zerolog.Logger
	// meta is the provider's client created when configure is called
	meta schema.ClientMeta
}

func (p *Provider) GetProviderSchema(_ context.Context, _ *pb.GetProviderSchemaRequest) (*cqproto.GetProviderSchemaResponse, error) {
	return &pb.GetProviderSchemaResponse{
		Name:           p.Name,
		Version:        p.Version,
		ResourceTables: p.ResourceMap,
	}, nil
}

func (p *Provider) FetchResources(ctx context.Context, request *pb.FetchResources_Request, sender pb.FetchResources_Response) error {
	var err error
	p.meta, err = p.Configure(p.Logger, request.Config)
	if err != nil {
		return err
	}

	if helpers.HasDuplicates(request.Resources) {
		return fmt.Errorf("provider has duplicate resources requested")
	}

	// if resources ["*"] is requested we will fetch all resources
	resources, err := p.interpolateAllResources(request.Resources)
	if err != nil {
		return err
	}

	// limiter used to limit the amount of resources fetched concurrently
	var goroutinesSem *semaphore.Weighted
	maxGoroutines := request.MaxGoroutines
	if maxGoroutines == 0 {
		maxGoroutines = limit.GetMaxGoRoutines()
	}
	p.Logger.Info("calculated max goroutines for fetch execution", "max_goroutines", maxGoroutines)
	goroutinesSem = semaphore.NewWeighted(helpers.Uint64ToInt64(maxGoroutines))

	g, gctx := errgroup.WithContext(ctx)
	if request.ParallelFetchingLimit > 0 {
		g.SetLimit(helpers.Uint64ToInt(request.ParallelFetchingLimit))
	}
	finishedResources := make(map[string]bool, len(resources))
	l := &sync.Mutex{}
	var totalResourceCount uint64
	for _, resource := range resources {
		table, ok := p.ResourceMap[resource]
		if !ok {
			return fmt.Errorf("plugin %s does not provide resource %s", p.Name, resource)
		}
		tableExec := execution.NewTableExecutor(resource, p.Logger.With("table", table.Name), table, request.Metadata, goroutinesSem, request.Timeout)
		p.Logger.Debug("fetching table...", "provider", p.Name, "table", table.Name)
		// Save resource aside
		r := resource
		l.Lock()
		finishedResources[r] = false
		l.Unlock()
		g.Go(func() error {
			resourceCount, err := tableExec.Resolve(gctx, p.meta)
			if err != nil {
				return err
			}
			l.Lock()
			defer l.Unlock()
			finishedResources[r] = true
			atomic.AddUint64(&totalResourceCount, resourceCount)
			status := cqproto.ResourceFetchComplete
			if isCancelled(ctx) {
				status = cqproto.ResourceFetchCanceled
			}
			if err := sender.Send(&cqproto.FetchResourcesResponse{
				ResourceName:      r,
				FinishedResources: finishedResources,
				ResourceCount:     resourceCount,
				Summary: cqproto.ResourceFetchSummary{
					Status:        status,
					ResourceCount: resourceCount,
				},
			}); err != nil {
				return err
			}
			p.Logger.Debug("finished fetching table...", "provider", p.Name, "table", table.Name)
			return nil
		})
	}
	return g.Wait()
}

func (p *Provider) interpolateAllResources(requestedResources []string) ([]string, error) {
	if len(requestedResources) != 1 {
		if funk.ContainsString(requestedResources, "*") {
			return nil, fmt.Errorf("invalid \"*\" resource, with explicit resources")
		}
		return requestedResources, nil
	}
	if requestedResources[0] != "*" {
		return requestedResources, nil
	}
	allResources := make([]string, 0, len(p.ResourceMap))
	for k := range p.ResourceMap {
		allResources = append(allResources, k)
	}
	return allResources, nil
}

func isCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
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
