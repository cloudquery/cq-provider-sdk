package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/database"
	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/helpers/limit"
	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/cloudquery/cq-provider-sdk/provider/execution"
	"github.com/cloudquery/cq-provider-sdk/provider/module"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/creasty/defaults"
	"github.com/hashicorp/go-hclog"
	"github.com/thoas/go-funk"
	"github.com/xeipuuv/gojsonschema"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"gopkg.in/yaml.v3"
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
	// Configure the provider and return context
	Configure func(hclog.Logger, interface{}) (schema.ClientMeta, diag.Diagnostics)
	// ResourceMap is all resources supported by this plugin
	ResourceMap map[string]*schema.Table
	// Configuration decoded from configure request
	Config func() Config
	// ConfigSchema returns the JSON schema for the config
	ConfigSchema string
	// Logger to call, this logger is passed to the serve.Serve Client, if not define Serve will create one instead.
	Logger hclog.Logger
	// ErrorClassifier allows the provider to classify errors it produces during table execution, and return them as diagnostics to the user.
	// Classifier function may return empty slice if it cannot meaningfully convert the error into diagnostics. In this case
	// the error will be converted by the SDK into diagnostic at ERROR level and RESOLVING type.
	ErrorClassifier execution.ErrorClassifier
	// ModuleInfoReader is called when the user executes a module, to get provider supported metadata about the given module
	ModuleInfoReader module.InfoReader
	// Database connection string
	dbURL string
	// meta is the provider's client created when configure is called
	meta schema.ClientMeta
	// storageCreator creates a database based on requested engine
	storageCreator func(ctx context.Context, logger hclog.Logger, dbURL string) (execution.Storage, error)
}

var _ cqproto.CQProviderServer = (*Provider)(nil)

func (p *Provider) GetProviderSchema(_ context.Context, _ *cqproto.GetProviderSchemaRequest) (*cqproto.GetProviderSchemaResponse, error) {
	return &cqproto.GetProviderSchemaResponse{
		Name:           p.Name,
		Version:        p.Version,
		ResourceTables: p.ResourceMap,
	}, nil
}

func (p *Provider) GetProviderConfig(_ context.Context, req *cqproto.GetProviderConfigRequest) (*cqproto.GetProviderConfigResponse, error) {
	providerConfig := p.Config()
	if err := defaults.Set(providerConfig); err != nil {
		return &cqproto.GetProviderConfigResponse{}, err
	}

	resList := funk.Keys(p.ResourceMap).([]string)
	sort.Strings(resList)
	nodes := make([]*yaml.Node, len(resList))
	for i := range resList {
		nodes[i] = &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: resList[i],
		}
	}

	data := &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{
				Kind:  yaml.ScalarNode,
				Value: "configuration",
			},
			{
				Kind: yaml.ScalarNode,
				Tag:  "!!null",
			},
			{
				Kind: yaml.ScalarNode,
				// double newline will leave only the last block of comments
				HeadComment: strings.TrimRight(providerConfig.Example(), "\r\n") + "\n \nlist of resources to fetch",
				Value:       "resources",
			},
			{
				Kind:    yaml.SequenceNode,
				Content: nodes,
			},
		},
	}

	yb, err := yaml.Marshal(data)
	if err != nil {
		return &cqproto.GetProviderConfigResponse{}, diag.WrapError(err)
	}

	return &cqproto.GetProviderConfigResponse{
		Config: yb,
	}, nil
}

func (p *Provider) ValidateProviderConfig(_ context.Context, request *cqproto.ValidateProviderConfigRequest) (*cqproto.ValidateProviderConfigResponse, error) {
	if len(request.Config) == 0 {
		return &cqproto.ValidateProviderConfigResponse{}, nil
	}

	providerConfig := p.Config()
	if err := defaults.Set(providerConfig); err != nil {
		return &cqproto.ValidateProviderConfigResponse{
			Diagnostics: diag.FromError(err, diag.INTERNAL),
		}, nil
	}

	d := yaml.NewDecoder(bytes.NewReader(request.Config))
	d.KnownFields(true)
	if err := d.Decode(providerConfig); err != nil {
		p.Logger.Error("Failed to load configuration.", "error", err)
		return &cqproto.ValidateProviderConfigResponse{
			Diagnostics: diag.FromError(err, diag.USER),
		}, nil
	}

	if p.ConfigSchema == "" {
		// no schema: Everything is valid. Parse config succeeded, assume success
		return &cqproto.ValidateProviderConfigResponse{}, nil
	}

	// Validate using given schema
	schemaLoader := gojsonschema.NewStringLoader(p.ConfigSchema)
	documentLoader := gojsonschema.NewGoLoader(providerConfig)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return &cqproto.ValidateProviderConfigResponse{
			Diagnostics: diag.FromError(err, diag.USER, diag.WithSummary("Failed to validate config")),
		}, nil
	}
	if !result.Valid() {
		errs := result.Errors()
		if len(errs) == 0 {
			return &cqproto.ValidateProviderConfigResponse{
				Diagnostics: diag.FromError(errors.New("Failed to validate config with schema"), diag.USER, diag.WithSummary("Invalid configuration")),
			}, nil
		}
		var diags diag.Diagnostics
		for _, e := range errs {
			diags = diags.Add(
				diag.FromError(errors.New(e.String()), diag.USER, diag.WithDetails("%s", e.Description()), diag.WithSummary("Config field %q has error of type %s", e.Field(), e.Type())),
			)
		}
		return &cqproto.ValidateProviderConfigResponse{
			Diagnostics: diags,
		}, nil
	}

	// success
	return &cqproto.ValidateProviderConfigResponse{}, nil
}

func (p *Provider) ConfigureProvider(ctx context.Context, request *cqproto.ConfigureProviderRequest) (*cqproto.ConfigureProviderResponse, error) {
	if p.Logger == nil {
		return &cqproto.ConfigureProviderResponse{
			Diagnostics: diag.FromError(fmt.Errorf("provider %s logger not defined, make sure to run it with serve", p.Name), diag.INTERNAL),
		}, nil
	}

	if p.meta != nil {
		if !IsDebug() {
			return &cqproto.ConfigureProviderResponse{
				Diagnostics: diag.FromError(fmt.Errorf("provider %s was already configured", p.Name), diag.INTERNAL),
			}, nil
		}

		p.Logger.Info("Reconfiguring provider: Previous configuration has been reset.")
		p.storageCreator = nil
	}

	// set database creator
	if p.storageCreator == nil {
		p.storageCreator = func(ctx context.Context, logger hclog.Logger, dbURL string) (execution.Storage, error) {
			return database.New(ctx, logger, dbURL)
		}
	}

	var diags diag.Diagnostics

	// validate provider configuration before proceeding
	validateResponse, err := p.ValidateProviderConfig(ctx, &cqproto.ValidateProviderConfigRequest{Config: request.Config})
	if err != nil {
		return &cqproto.ConfigureProviderResponse{
			Diagnostics: diag.FromError(err, diag.INTERNAL, diag.WithSummary("ValidateProviderConfig failed")),
		}, nil
	} else if validateResponse != nil {
		diags = diags.Add(validateResponse.Diagnostics)
	}
	if diags.HasErrors() {
		return &cqproto.ConfigureProviderResponse{
			Diagnostics: diags,
		}, nil
	}

	p.dbURL = request.Connection.DSN

	providerConfig := p.Config()
	if err := defaults.Set(providerConfig); err != nil {
		return &cqproto.ConfigureProviderResponse{
			Diagnostics: diags.Add(diag.FromError(err, diag.INTERNAL)),
		}, nil
	}

	// if we received an empty config we notify in log and only use defaults.
	if len(request.Config) == 0 {
		p.Logger.Info("Received empty configuration, using only defaults")
	} else {
		d := yaml.NewDecoder(bytes.NewReader(request.Config))
		d.KnownFields(true)
		if err := d.Decode(providerConfig); err != nil {
			p.Logger.Error("Failed to load configuration.", "error", err)
			return &cqproto.ConfigureProviderResponse{
				Diagnostics: diags.Add(diag.FromError(err, diag.USER)),
			}, nil
		}
	}

	client, moreDiags := p.Configure(p.Logger, providerConfig)
	diags = diags.Add(moreDiags)
	if diags.HasErrors() {
		return &cqproto.ConfigureProviderResponse{
			Diagnostics: diags,
		}, nil
	}

	tables := make(map[string]string)
	for r, t := range p.ResourceMap {
		if err := getTableDuplicates(r, t, tables); err != nil {
			return &cqproto.ConfigureProviderResponse{
				Diagnostics: diags.Add(diag.FromError(err, diag.INTERNAL)),
			}, nil
		}
	}

	p.meta = client
	return &cqproto.ConfigureProviderResponse{
		Diagnostics: diags,
	}, nil
}

func (p *Provider) FetchResources(ctx context.Context, request *cqproto.FetchResourcesRequest, sender cqproto.FetchResourcesSender) error {
	if p.meta == nil {
		return fmt.Errorf("provider client is not configured (Hint: Try upgrading cloudquery)")
	}

	if helpers.HasDuplicates(request.Resources) {
		return fmt.Errorf("provider has duplicate resources requested")
	}

	// if resources ["*"] is requested we will fetch all resources
	resources, err := p.interpolateAllResources(request.Resources)
	if err != nil {
		return err
	}

	conn, err := p.storageCreator(ctx, p.Logger, p.dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database. %w", err)
	}

	defer conn.Close()

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
		tableExec := execution.NewTableExecutor(resource, conn, p.Logger.With("table", table.Name), table, request.Metadata, p.ErrorClassifier, goroutinesSem, request.Timeout)
		p.Logger.Debug("fetching table...", "provider", p.Name, "table", table.Name)
		// Save resource aside
		r := resource
		l.Lock()
		finishedResources[r] = false
		l.Unlock()
		g.Go(func() error {
			resourceCount, diags := tableExec.Resolve(gctx, p.meta)
			l.Lock()
			defer l.Unlock()
			finishedResources[r] = true
			atomic.AddUint64(&totalResourceCount, resourceCount)
			status := cqproto.ResourceFetchComplete
			if isCancelled(ctx) {
				status = cqproto.ResourceFetchCanceled
			} else if diags.HasErrors() {
				status = cqproto.ResourceFetchPartial
			}
			if err := sender.Send(&cqproto.FetchResourcesResponse{
				ResourceName:      r,
				FinishedResources: finishedResources,
				ResourceCount:     resourceCount,
				Summary: cqproto.ResourceFetchSummary{
					Status:        status,
					ResourceCount: resourceCount,
					Diagnostics:   diags,
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

func (p *Provider) GetModuleInfo(_ context.Context, request *cqproto.GetModuleRequest) (*cqproto.GetModuleResponse, error) {
	if p.ModuleInfoReader == nil {
		return nil, nil
	}

	if p.Logger == nil {
		return nil, fmt.Errorf("provider %s logger not defined, make sure to run it with serve", p.Name)
	}

	resp, err := p.ModuleInfoReader(p.Logger, request.Module, request.PreferredVersions)
	return &cqproto.GetModuleResponse{
		Data:              resp.Data,
		AvailableVersions: resp.AvailableVersions,
		Diagnostics:       diag.FromError(err, diag.INTERNAL),
	}, nil
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

// IsDebug checks if CQ_PROVIDER_DEBUG is turned on. In case it's true the plugin is executed in debug mode.
func IsDebug() bool {
	b, _ := strconv.ParseBool(os.Getenv("CQ_PROVIDER_DEBUG"))
	return b
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
