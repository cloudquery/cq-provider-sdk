package execution

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/modern-go/reflect2"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"

	"github.com/hashicorp/go-hclog"
	"github.com/iancoleman/strcase"
	"github.com/thoas/go-funk"
)

// executionJitter adds a -1 minute to execution of fetch, so if a user fetches only 1 resources and it finishes
// faster than the <1s it won't be deleted by remove stale.
const executionJitter = -1 * time.Minute

// TableExecutor marks all the related execution info passed to TableResolver and ColumnResolver giving access to the Runner's meta
type TableExecutor struct {
	// ResourceName name of top-level resource associated with table
	ResourceName string
	// Table this execution is associated with
	Table *schema.Table
	// Database connection to insert data into
	Db Storage
	// Logger associated with this execution
	Logger hclog.Logger
	// extraFields to be passed to each created resource in the execution
	extraFields map[string]interface{}
	// When the execution started
	executionStart time.Time
	// parent is the parent TableExecutor
	parent *TableExecutor
}

const (
// TODO: move this to the provider when it loops over all errors

)

// CreateTableExecutor creates a new TableExecutor for given schema.Table
func CreateTableExecutor(db Storage, logger hclog.Logger, table *schema.Table, extraFields map[string]interface{}) TableExecutor {
	return TableExecutor{
		Table:          table,
		Db:             db,
		Logger:         logger,
		extraFields:    extraFields,
		executionStart: time.Now().Add(executionJitter),
	}
}

func (e *TableExecutor) Resolve(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource) (uint64, diag.Diagnostics) {
	var clients []schema.ClientMeta
	clients = append(clients, meta)
	if e.Table.Multiplex != nil {
		if parent != nil {
			meta.Logger().Warn("relation client multiplexing is not allowed, skipping multiplex", "table", e.Table.Name)
		} else {
			clients = e.Table.Multiplex(meta)
			meta.Logger().Debug("multiplexing client", "count", len(clients), "table", e.Table.Name)
		}
	}
	var (
		diagsChan      chan diag.Diagnostics
		totalResources uint64
		wg             sync.WaitGroup
	)

	defer close(diagsChan)
	wg.Add(len(clients))
	for _, client := range clients {
		go func(c schema.ClientMeta) {
			count, resolveDiags := e.callTableResolve(ctx, c, parent)
			atomic.AddUint64(&totalResources, count)
			diagsChan <- resolveDiags
		}(client)
	}
	var allDiags diag.Diagnostics
	go func() {
		for dd := range diagsChan {
			allDiags = allDiags.Append(dd)
			wg.Done()
		}
	}()
	wg.Wait()
	return totalResources, allDiags
}

func (e *TableExecutor) WithTable(t *schema.Table) *TableExecutor {
	return &TableExecutor{
		Table:        t,
		ResourceName: e.ResourceName,
		Db:           e.Db,
		Logger:       e.Logger,
		extraFields:  e.extraFields,
		parent:       e,
	}
}

func (e TableExecutor) truncateTable(ctx context.Context, client schema.ClientMeta, parent *schema.Resource) error {
	if e.Table.DeleteFilter == nil {
		return nil
	}
	if !e.Table.AlwaysDelete {
		client.Logger().Debug("skipping table truncate", "table", e.Table.Name)
		return nil
	}
	// Delete previous fetch
	client.Logger().Debug("cleaning table previous fetch", "table", e.Table.Name, "always_delete", e.Table.AlwaysDelete)
	if err := e.Db.Delete(ctx, e.Table, e.Table.DeleteFilter(client, parent)); err != nil {
		return err
	}
	return nil
}

// cleanupStaleData cleans resources in table that weren't update in the latest table resolve execution
func (e TableExecutor) cleanupStaleData(ctx context.Context, client schema.ClientMeta, parent *schema.Resource) error {
	// Only clean top level tables
	if parent != nil {
		return nil
	}
	client.Logger().Debug("cleaning table table stale data", "table", e.Table.Name, "last_update", e.executionStart)

	filters := make([]interface{}, 0)
	for k, v := range e.extraFields {
		filters = append(filters, k, v)
	}
	if e.Table.DeleteFilter != nil {
		filters = append(filters, e.Table.DeleteFilter(client, parent)...)
	}
	return e.Db.RemoveStaleData(ctx, e.Table, e.executionStart, filters)
}

func (e TableExecutor) callTableResolve(ctx context.Context, client schema.ClientMeta, parent *schema.Resource) (uint64, diag.Diagnostics) {
	// set up all diagnostics to collect from resolving table
	var diags diag.Diagnostics

	if e.Table.Resolver == nil {
		return 0, diags.Append(NewError(diag.ERROR, diag.SCHEMA, e.ResourceName, "table %s missing resolver, make sure table implements the resolver", e.Table.Name))

	}
	if err := e.truncateTable(ctx, client, parent); err != nil {
		return 0, diags.Append(FromError(err, WithErrorClassifier))
	}

	res := make(chan interface{})
	var resolverErr error
	go func() {
		defer func() {
			if r := recover(); r != nil {
				client.Logger().Error("table resolver recovered from panic", "table", e.Table.Name, "stack", string(debug.Stack()))
				resolverErr = FromError(r.(error), WithResource(e.ResourceName), WithSeverity(diag.PANIC),
					WithType(diag.RESOLVING), WithSummary("panic on resource table %s fetch", e.Table.Name))
			}
			close(res)
		}()
		err := e.Table.Resolver(ctx, client, parent, res)
		if err != nil && e.Table.IgnoreError != nil && e.Table.IgnoreError(err) {
			client.Logger().Warn("ignored an error", "err", err, "table", e.Table.Name)
			resolverErr = NewError(diag.IGNORE, diag.RESOLVING, e.ResourceName, "table resolver ignored error. Error: %s", e.Table.Name)
			return
		}
		resolverErr = FromError(err, WithResource(e.ResourceName), WithSeverity(diag.ERROR), WithType(diag.RESOLVING),
			WithSummary("failed to resolver resource %s", e.ResourceName), WithErrorClassifier)
	}()

	nc := uint64(0)
	for elem := range res {
		objects := helpers.InterfaceSlice(elem)
		if len(objects) == 0 {
			continue
		}
		resolvedCount, err := e.resolveResources(ctx, client, parent, objects)
		if err != nil {
			return 0, err
		}
		nc += resolvedCount
	}
	// check if channel iteration stopped because of resolver failure
	if resolverErr != nil {
		client.Logger().Error("received resolve resources error", "table", e.Table.Name, "error", resolverErr)
		return 0, diags.Append(resolverErr)
	}
	// Print only parent resources
	if parent == nil {
		client.Logger().Info("fetched successfully", "table", e.Table.Name, "count", nc)
	}
	if err := e.cleanupStaleData(ctx, client, parent); err != nil {
		return nc, diags.Append(FromError(err, WithType(diag.DATABASE), WithSummary("failed to cleanup stale data on table %s", e.Table.Name)))
	}
	return nc, nil
}

func (e *TableExecutor) resolveResources(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, objects []interface{}) (uint64, diag.Diagnostics) {
	var (
		resources = make(schema.Resources, 0, len(objects))
		diags     diag.Diagnostics
	)

	for _, o := range objects {
		resource := schema.NewResourceData(e.Db.Dialect(), e.Table, parent, o, e.extraFields, e.executionStart)
		// Before inserting resolve all table column resolvers
		if err := e.resolveResourceValues(ctx, meta, resource); err != nil {
			e.Logger.Warn("skipping failed resolved resource", "reason", err.Error())
			diags = diags.Append(err)
			continue
		}
		resources = append(resources, resource)
	}

	// only top level tables should cascade
	shouldCascade := parent == nil
	var err error
	resources, err = e.copyDataIntoDB(ctx, resources, shouldCascade)
	if err != nil {
		return 0, diags.Append(err)
	}
	totalCount := uint64(len(resources))

	// Finally, resolve relations of each resource
	for _, rel := range e.Table.Relations {
		meta.Logger().Debug("resolving table relation", "table", e.Table.Name, "relation", rel.Name)
		for _, r := range resources {
			// ignore relation resource count
			if _, innerDiags := e.WithTable(rel).Resolve(ctx, meta, r); err != nil {
				diags = diags.Append(innerDiags)
			}
		}
	}
	return totalCount, diags
}

func (e *TableExecutor) copyDataIntoDB(ctx context.Context, resources schema.Resources, shouldCascade bool) (schema.Resources, diag.Diagnostics) {
	err := e.Db.CopyFrom(ctx, resources, shouldCascade, e.extraFields)
	if err == nil {
		return resources, nil
	}
	e.Logger.Warn("failed copy-from to db", "error", err, "table", e.Table.Name)

	// fallback insert, copy from sometimes does problems, so we fall back with bulk insert
	err = e.Db.Insert(ctx, e.Table, resources)
	if err == nil {
		return resources, nil
	}
	e.Logger.Error("failed insert to db", "error", err, "table", e.Table.Name)

	// Setup diags, adding first diagnostic that bulk insert failed
	diags := diag.Diagnostics{}.Append(FromError(err, WithType(diag.DATABASE), WithSummary("failed bulk insert on table %s", e.Table.Name)))
	// Try to insert resource by resource if partial fetch is enabled and an error occurred
	partialFetchResources := make(schema.Resources, 0)
	for id := range resources {
		if err := e.Db.Insert(ctx, e.Table, schema.Resources{resources[id]}); err != nil {
			e.Logger.Error("failed to insert resource into db", "error", err, "resource_keys", resources[id].PrimaryKeyValues(), "table", e.Table.Name)
			diags = diags.Append(FromError(err, WithType(diag.DATABASE), WithErrorClassifier))
			continue
		}
		// If there is no error we add the resource to the final result
		partialFetchResources = append(partialFetchResources, resources[id])
	}
	return partialFetchResources, diags
}

func (e *TableExecutor) resolveResourceValues(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.Logger.Error("resolve resource recovered from panic", "table", e.Table.Name, "stack", string(debug.Stack()))
			err = FromError(r.(error), WithResource(e.ResourceName), WithSeverity(diag.PANIC),
				WithType(diag.RESOLVING), WithSummary("resolve resource %s recovered from panic.", e.Table.Name))
		}
	}()
	// TODO: do this once per table
	providerCols, internalCols := e.Db.Dialect().Columns(e.Table).Sift()

	if err = e.resolveColumns(ctx, meta, resource, providerCols); err != nil {
		return err
	}
	// call PostRowResolver if defined after columns have been resolved
	if e.Table.PostResourceResolver != nil {
		if err = e.Table.PostResourceResolver(ctx, meta, resource); err != nil {
			return FromError(err, WithResource(e.ResourceName), WithSummary("failed post resource resolver"), WithErrorClassifier)
		}
	}
	// Finally, resolve columns internal to the SDK
	for _, c := range internalCols {
		if err = c.Resolver(ctx, meta, resource, c); err != nil {
			return FromError(err, WithResource(e.ResourceName), WithType(diag.INTERNAL), WithSummary("default column %s resolver execution", c.Name))
		}
	}
	return err
}

func (e *TableExecutor) resolveColumns(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, cols []schema.Column) error {
	for _, c := range cols {
		if c.Resolver != nil {
			meta.Logger().Trace("using custom column resolver", "column", c.Name, "table", e.Table.Name)
			err := c.Resolver(ctx, meta, resource, c)
			if err == nil {
				continue
			}
			// Not allowed ignoring PK resolver errors
			if funk.ContainsString(e.Db.Dialect().PrimaryKeys(e.Table), c.Name) {
				return FromError(err, WithResource(e.ResourceName), WithSummary("failed to resolve column %s@%s", e.Table.Name, c.Name), WithErrorClassifier)
			}
			// check if column resolver defined an IgnoreError function, if it does check if ignore should be ignored.
			if c.IgnoreError == nil || !c.IgnoreError(err) {
				return FromError(err, WithResource(e.ResourceName), WithSummary("failed to resolve column %s@%s", e.Table.Name, c.Name), WithErrorClassifier)
			}
			// TODO: double check logic here
			if reflect2.IsNil(c.Default) {
				continue
			}
			// Set default value if defined, otherwise it will be nil
			if err := resource.Set(c.Name, c.Default); err != nil {
				return FromError(err, WithResource(e.ResourceName), WithType(diag.INTERNAL),
					WithSummary("failed to set resource default value for %s@%s", e.Table.Name, c.Name))
			}
			continue
		}
		meta.Logger().Trace("resolving column value with path", "column", c.Name, "table", e.Table.Name)
		// base use case: try to get column with CamelCase name
		v := funk.Get(resource.Item, strcase.ToCamel(c.Name), funk.WithAllowZero())
		if v == nil {
			meta.Logger().Trace("using column default value", "column", c.Name, "default", c.Default, "table", e.Table.Name)
			v = c.Default
		}
		meta.Logger().Trace("setting column value", "column", c.Name, "value", v, "table", e.Table.Name)
		if err := resource.Set(c.Name, v); err != nil {
			return FromError(err, WithResource(e.ResourceName), WithType(diag.INTERNAL),
				WithSummary("failed to set resource value for column %s@%s", e.Table.Name, c.Name))
		}
	}
	return nil
}
