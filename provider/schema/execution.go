package schema

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/modern-go/reflect2"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"

	"github.com/hashicorp/go-hclog"
	"github.com/iancoleman/strcase"
	"github.com/thoas/go-funk"
)

// executionJitter adds a -1 minute to execution of fetch, so if a user fetches only 1 resources and it finishes
// faster than the <1s it won't be deleted by remove stale.
const executionJitter = -1 * time.Minute

//go:generate mockgen -package=mock -destination=./mock/mock_storage.go . Storage
type Storage interface {
	QueryExecer

	Insert(ctx context.Context, t *Table, instance Resources) error
	Delete(ctx context.Context, t *Table, kvFilters []interface{}) error
	RemoveStaleData(ctx context.Context, t *Table, executionStart time.Time, kvFilters []interface{}) error
	CopyFrom(ctx context.Context, resources Resources, shouldCascade bool, CascadeDeleteFilters map[string]interface{}) error
	Close()
	Dialect() Dialect
}

type QueryExecer interface {
	pgxscan.Querier

	Exec(ctx context.Context, query string, args ...interface{}) error
}

type ClientMeta interface {
	Logger() hclog.Logger
}

func NewResolverError(err error, resource string, summary string, args ...interface{}) *diag.ExecutionError {
	return diag.NewExecutionErrorWithError(
		err,
		diag.ERROR,
		diag.RESOLVING,
		resource,
		fmt.Sprintf(summary, args),
		"",
	)
}

func NewInternalError(err error, resource string, summary string, args ...interface{}) *diag.ExecutionError {
	return diag.NewExecutionErrorWithError(
		err,
		diag.ERROR,
		diag.INTERNAL,
		resource,
		fmt.Sprintf(summary, args),
		"",
	)
}

// ExecutionData marks all the related execution info passed to TableResolver and ColumnResolver giving access to the Runner's meta
type ExecutionData struct {
	// ResourceName name of top-level resource associated with table
	ResourceName string
	// Table this execution is associated with
	Table *Table
	// Database connection to insert data into
	Db Storage
	// Logger associated with this execution
	Logger hclog.Logger
	// extraFields to be passed to each created resource in the execution
	extraFields map[string]interface{}
	// partialFetch if true allows partial fetching of resources
	partialFetch bool
	// When the execution started
	executionStart time.Time
	// parent is the parent ExecutionData
	parent *ExecutionData
}

const (
	// TODO: move this to the provider when it loops over all errors 
	// fdLimitMessage defines the message for when a client isn't able to fetch because the open fd limit is hit
	fdLimitMessage = "try increasing number of available file descriptors via `ulimit -n 10240` or by increasing timeout via provider specific parameters"
)

// NewExecutionData Create a new execution data
func NewExecutionData(db Storage, logger hclog.Logger, table *Table, extraFields map[string]interface{}, partialFetch bool) ExecutionData {
	return ExecutionData{
		Table:          table,
		Db:             db,
		Logger:         logger,
		extraFields:    extraFields,
		executionStart: time.Now().Add(executionJitter),
	}
}

func (e *ExecutionData) ResolveTable(ctx context.Context, meta ClientMeta, parent *Resource) (uint64, diag.Diagnostics) {
	var clients []ClientMeta
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
		diagsChan       chan <- diag.Diagnostics
		totalResources uint64
		wg             sync.WaitGroup
	)

	defer close(diagsChan)
	wg.Add(len(clients))
	for _, client := range clients {
		go func(c ClientMeta) {
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

func (e *ExecutionData) WithTable(t *Table) *ExecutionData {
	return &ExecutionData{
		Table:        t,
		ResourceName: e.ResourceName,
		Db:           e.Db,
		Logger:       e.Logger,
		extraFields:  e.extraFields,
		partialFetch: e.partialFetch,
		parent:       e,
	}
}

func (e ExecutionData) truncateTable(ctx context.Context, client ClientMeta, parent *Resource) error {
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
func (e ExecutionData) cleanupStaleData(ctx context.Context, client ClientMeta, parent *Resource) error {
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

func (e ExecutionData) callTableResolve(ctx context.Context, client ClientMeta, parent *Resource) (uint64, diag.Diagnostics) {
	// set up all diagnostics to collect from resolving table
	var diags diag.Diagnostics

	if e.Table.Resolver == nil {
		return 0, diags.Append(diag.NewExecutionError(diag.ERROR, diag.SCHEMA, "table %s missing resolver, make sure table implements the resolver", e.Table.Name))

	}
	if err := e.truncateTable(ctx, client, parent); err != nil {
		return 0, diags.Append(diag.FromError(diag.ERROR, diag.DATABASE, err))
	}

	res := make(chan interface{})
	var resolverErr error
	go func() {
		defer func() {
			if r := recover(); r != nil {
				client.Logger().Error("table resolver recovered from panic", "table", e.Table.Name, "stack", string(debug.Stack()))
				resolverErr = diag.NewExecutionError(diag.PANIC, diag.RESOLVING, e.ResourceName, "failed table %s fetch. Error: %s", e.Table.Name, r)
			}
			close(res)
		}()
		// TODO: if resolver can return diag.Diagnostics we need to check this
		err := e.Table.Resolver(ctx, client, parent, res)
		if err != nil && e.Table.IgnoreError != nil && e.Table.IgnoreError(err) {
			client.Logger().Warn("ignored an error", "err", err, "table", e.Table.Name)
			resolverErr = diag.NewExecutionError(diag.IGNORE, diag.RESOLVING, e.ResourceName, "table resolver ignored error. Error: %s", e.Table.Name)
			return
		}
		resolverErr = diag.NewExecutionErrorWithError(err, diag.ERROR, diag.RESOLVING, e.ResourceName, "failed to resolve table", "")
	}()

	nc := uint64(0)
	for elem := range res {
		objects := interfaceSlice(elem)
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
		return nc, diags.Append(diag.FromError(diag.ERROR, diag.DATABASE, err))
	}
	return nc, nil
}

func (e *ExecutionData) resolveResources(ctx context.Context, meta ClientMeta, parent *Resource, objects []interface{}) (uint64, diag.Diagnostics) {
	var (
		resources = make(Resources, 0, len(objects))
		diags     diag.Diagnostics
	)

	for _, o := range objects {
		resource := NewResourceData(e.Db.Dialect(), e.Table, parent, o, e.extraFields, e.executionStart)
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
			if _, err := e.WithTable(rel).ResolveTable(ctx, meta, r); err != nil {
				diags = diags.Append(err)
			}
		}
	}
	return totalCount, diags
}

func (e *ExecutionData) copyDataIntoDB(ctx context.Context, resources Resources, shouldCascade bool) (Resources, diag.Diagnostics) {
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
	diags := diag.Diagnostics{}.Append(diag.FromError(diag.ERROR, diag.DATABASE, err))
	// Try to insert resource by resource if partial fetch is enabled and an error occurred
	partialFetchResources := make(Resources, 0)
	for id := range resources {
		if err := e.Db.Insert(ctx, e.Table, Resources{resources[id]}); err != nil {
			e.Logger.Error("failed to insert resource into db", "error", err, "resource_keys", resources[id].PrimaryKeyValues(), "table", e.Table.Name)
			diags = diags.Append(diag.FromError(diag.ERROR, diag.DATABASE, err))
			continue
		}
		// If there is no error we add the resource to the final result
		partialFetchResources = append(partialFetchResources, resources[id])
	}
	return partialFetchResources, diags
}

func (e *ExecutionData) resolveResourceValues(ctx context.Context, meta ClientMeta, resource *Resource) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.Logger.Error("resolve resource recovered from panic", "table", e.Table.Name, "stack", string(debug.Stack()))
			err = diag.NewExecutionError(diag.PANIC, diag.RESOLVING, e.ResourceName, "resolve resource recovered from panic.")
		}
	}()
	// TODO: do this once per table
	providerCols, internalCols := siftColumns(e.Db.Dialect().Columns(resource.table))

	if err = e.resolveColumns(ctx, meta, resource, providerCols); err != nil {
		return err
	}
	// call PostRowResolver if defined after columns have been resolved
	if resource.table.PostResourceResolver != nil {
		if err = resource.table.PostResourceResolver(ctx, meta, resource); err != nil {
			return NewResolverError(err, e.ResourceName, "failed post resource resolver")
		}
	}
	// Finally, resolve columns internal to the SDK
	for _, c := range internalCols {
		if err = c.Resolver(ctx, meta, resource, c); err != nil {
			return NewInternalError(err, e.ResourceName, "default column %s resolver execution failed: %w", c.Name, err)
		}
	}
	return err
}

func (e *ExecutionData) resolveColumns(ctx context.Context, meta ClientMeta, resource *Resource, cols []Column) error {
	for _, c := range cols {
		if c.Resolver != nil {
			meta.Logger().Trace("using custom column resolver", "column", c.Name, "table", e.Table.Name)
			err := c.Resolver(ctx, meta, resource, c)
			if err == nil {
				continue
			}
			// Not allowed ignoring PK resolver errors
			if funk.ContainsString(e.Db.Dialect().PrimaryKeys(e.Table), c.Name) {
				return NewResolverError(err, e.ResourceName, "failed to resolve column %s@%s", e.Table.Name, c.Name)
			}
			// check if column resolver defined an IgnoreError function, if it does check if ignore should be ignored.
			if c.IgnoreError == nil || !c.IgnoreError(err) {
				return NewResolverError(err, e.ResourceName, "failed to resolve column %s@%s", e.Table.Name, c.Name)
			}
			// TODO: double check logic here
			if reflect2.IsNil(c.Default) {
				continue
			}
			// Set default value if defined, otherwise it will be nil
			if err := resource.Set(c.Name, c.Default); err != nil {
				return NewInternalError(err, e.ResourceName, "failed to set resource default value for %s@%s", e.Table.Name, c.Name)
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
			return NewInternalError(err, e.ResourceName, "failed to set resource value for column %s@%s", e.Table.Name, c.Name)
		}
	}
	return nil
}

func interfaceSlice(slice interface{}) []interface{} {
	// if value is nil return nil
	if slice == nil {
		return nil
	}
	s := reflect.ValueOf(slice)
	// Keep the distinction between nil and empty slice input
	if s.Kind() == reflect.Ptr && s.Elem().Kind() == reflect.Slice && s.Elem().IsNil() {
		return nil
	}
	if s.Kind() != reflect.Slice {
		return []interface{}{slice}
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

// 	if strings.Contains(err.Error(), ": socket: too many open files") {
//		// Return a Diagnostic error so that it can be properly propagated back to the user via the CLI
//		partialFetchFailure.Err = diag.NewExecutionErrorWithError(err, diag.WARNING, diag.THROTTLE, e.ResourceName, err.Error(), fdLimitMessage)
//	}

// siftColumns gets a column list and returns a list of provider columns, and another list of internal columns, cqId column being the very last one
func siftColumns(cols []Column) ([]Column, []Column) {
	providerCols, internalCols := make([]Column, 0, len(cols)), make([]Column, 0, len(cols))

	cqIdColIndex := -1
	for i := range cols {
		if cols[i].internal {
			if cols[i].Name == cqIdColumn.Name {
				cqIdColIndex = len(internalCols)
			}

			internalCols = append(internalCols, cols[i])
		} else {
			providerCols = append(providerCols, cols[i])
		}
	}

	// resolve cqId last, as it would need other PKs to be resolved, some might be internal (cq_fetch_date)
	if lastIndex := len(internalCols) - 1; cqIdColIndex > -1 && cqIdColIndex != lastIndex {
		internalCols[cqIdColIndex], internalCols[lastIndex] = internalCols[lastIndex], internalCols[cqIdColIndex]
	}

	return providerCols, internalCols
}
