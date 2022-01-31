package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/creasty/defaults"

	"github.com/stretchr/testify/mock"

	"github.com/cloudquery/cq-provider-sdk/provider/diag"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/testlog"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ExecutionTestCase struct {
	Name        string
	Description string

	Table       *schema.Table
	ExtraFields map[string]interface{}

	SetupStorage          func(t *testing.T) Storage
	ExpectedResourceCount uint64
	ErrorExpected         bool
	ExpectedDiags         []diag.FlatDiag
}

type executionClient struct {
	l hclog.Logger
}

func (e executionClient) Logger() hclog.Logger {
	return e.l
}

var (
	commonColumns     = []schema.Column{{Name: "name", Type: schema.TypeString}}
	doNothingResolver = func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
		return nil
	}
	returnErrorResolver = func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
		return fmt.Errorf("some error")
	}

	returnValueResolver = func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
		res <- map[string]string{"name": "test"}
		return nil
	}

	panicResolver = func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
		panic("resolver panic")
	}

	simpleMultiplexer = func(meta schema.ClientMeta) []schema.ClientMeta {
		return []schema.ClientMeta{meta, meta}
	}

	postResourceResolver = func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource) error {
		return resource.Set("name", "data")
	}
	postResourceResolverError = func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource) error {
		return diag.Diagnostics{
			NewError(diag.ERROR, diag.RESOLVING, resource.TableName(), "some error"),
			NewError(diag.ERROR, diag.RESOLVING, resource.TableName(), "some error 2")}
	}
)

func TestTableExecutor_Resolve(t *testing.T) {
	testCases := []ExecutionTestCase{
		{
			Name: "simple",
			Table: &schema.Table{
				Name:     "simple",
				Resolver: doNothingResolver,
				Columns:  commonColumns,
			},
		},
		{
			Name: "multiplex",
			Table: &schema.Table{
				Name:      "simple",
				Multiplex: simpleMultiplexer,
				Resolver:  doNothingResolver,
				Columns:   commonColumns,
			},
		},
		{
			Name: "multiplex_relation",
			Table: &schema.Table{
				Name:      "multiplex_relation",
				Multiplex: simpleMultiplexer,
				Resolver:  returnValueResolver,
				Columns:   commonColumns,
				Relations: []*schema.Table{
					{
						Resolver:  doNothingResolver,
						Multiplex: simpleMultiplexer,
						Name:      "relation_with_multiplex",
						Columns:   commonColumns,
					},
				},
			},
			ExpectedResourceCount: 2,
			ErrorExpected:         true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "multiplex on relation table relation_with_multiplex is not allowed, skipping multiplex",
					Resource: "multiplex_relation",
					Severity: diag.WARNING,
					Summary:  "multiplex on relation table relation_with_multiplex is not allowed, skipping multiplex",
					Type:     diag.SCHEMA,
				},
				{
					Err:      "multiplex on relation table relation_with_multiplex is not allowed, skipping multiplex",
					Resource: "multiplex_relation",
					Severity: diag.WARNING,
					Summary:  "multiplex on relation table relation_with_multiplex is not allowed, skipping multiplex",
					Type:     diag.SCHEMA,
				},
			},
		},
		{
			// if tables don't define a resolver, an execution error by execution
			Name:        "missing_table_resolver",
			Description: "if tables don't define a resolver, an execution error by execution",
			Table: &schema.Table{
				Name:    "no-resolver",
				Columns: commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "table no-resolver missing resolver, make sure table implements the resolver",
					Resource: "missing_table_resolver",
					Severity: diag.ERROR,
					Summary:  "table no-resolver missing resolver, make sure table implements the resolver",
					Type:     diag.SCHEMA,
				},
			},
		},
		{
			// if tables don't define a resolver, an execution error by execution, we check here that a relation will cause an error
			Name: "missing_table_relation_resolver",
			Table: &schema.Table{
				Name:     "no-resolver",
				Resolver: returnValueResolver,
				Columns:  commonColumns,
				Relations: []*schema.Table{
					{
						Name:    "relation-no-resolver",
						Columns: commonColumns,
					},
				},
			},
			ExpectedResourceCount: 1,
			ErrorExpected:         true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "table relation-no-resolver missing resolver, make sure table implements the resolver",
					Resource: "missing_table_relation_resolver",
					Severity: diag.ERROR,
					Summary:  "table relation-no-resolver missing resolver, make sure table implements the resolver",
					Type:     diag.SCHEMA,
				},
			},
		},
		{
			Name: "panic_resolver",
			Table: &schema.Table{
				Name:     "panic_resolver",
				Resolver: panicResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "table resolver panic: resolver panic",
					Resource: "panic_resolver",
					Severity: diag.PANIC,
					Summary:  "panic on resource table panic_resolver fetch",
					Type:     diag.RESOLVING,
				},
			},
		},
		{
			Name: "panic_relation_resolver",
			Table: &schema.Table{
				Name:     "panic_resolver",
				Resolver: returnValueResolver,
				Columns:  commonColumns,
				Relations: []*schema.Table{
					{
						Name:     "relation_panic_resolver",
						Resolver: panicResolver,
						Columns:  commonColumns,
					},
				},
			},
			ExpectedResourceCount: 1,
			ErrorExpected:         true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "table resolver panic: resolver panic",
					Resource: "panic_relation_resolver",
					Severity: diag.PANIC,
					Summary:  "panic on resource table relation_panic_resolver fetch",
					Type:     diag.RESOLVING,
				},
			},
		},
		{
			Name: "error_returning",
			Table: &schema.Table{
				Name:     "simple",
				Resolver: returnErrorResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "some error",
					Resource: "error_returning",
					Severity: diag.ERROR,
					Summary:  "failed to resolve resource error_returning",
					Type:     diag.RESOLVING,
				},
			},
		},
		{
			Name: "error_returning_ignore_fail",
			Table: &schema.Table{
				IgnoreError: func(err error) bool {
					return false
				},
				Name:     "simple",
				Resolver: returnErrorResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "some error",
					Resource: "error_returning_ignore_fail",
					Severity: diag.ERROR,
					Summary:  "failed to resolve resource error_returning_ignore_fail",
					Type:     diag.RESOLVING,
				},
			},
		},
		{
			Name: "error_returning_ignore",
			Table: &schema.Table{
				IgnoreError: func(err error) bool {
					return true
				},
				Name:     "simple",
				Resolver: returnErrorResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "table[simple] resolver ignored error. Error: some error",
					Resource: "error_returning_ignore",
					Severity: diag.IGNORE,
					Summary:  "table[simple] resolver ignored error. Error: some error",
					Type:     diag.RESOLVING,
				},
			},
		},
		{
			Name: "always_delete",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				db.On("RemoveStaleData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return db
			},
			Table: &schema.Table{
				Name:         "simple",
				AlwaysDelete: true,
				DeleteFilter: func(meta schema.ClientMeta, parent *schema.Resource) []interface{} {
					return []interface{}{}
				},
				Resolver: doNothingResolver,
				Columns:  commonColumns,
			},
		},
		{
			Name: "always_delete_fail",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("Delete", mock.Anything, mock.Anything, mock.Anything).
					Return(FromError(errors.New("failed delete"), WithResource("always_delete_fail"), WithType(diag.DATABASE)))
				return db
			},
			Table: &schema.Table{
				Name:         "simple",
				AlwaysDelete: true,
				DeleteFilter: func(meta schema.ClientMeta, parent *schema.Resource) []interface{} {
					return []interface{}{}
				},
				Resolver: doNothingResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "failed delete",
					Resource: "always_delete_fail",
					Severity: diag.ERROR,
					Type:     diag.DATABASE,
				},
			},
		},
		{
			Name: "cleanup_stale_data_fail",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("RemoveStaleData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(FromError(errors.New("failed delete"), WithResource("cleanup_stale_data_fail"), WithType(diag.DATABASE)))
				return db
			},
			Table: &schema.Table{
				Name: "cleanup_delete",
				DeleteFilter: func(meta schema.ClientMeta, parent *schema.Resource) []interface{} {
					return []interface{}{}
				},
				Resolver: doNothingResolver,
				Columns:  commonColumns,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "failed delete",
					Resource: "cleanup_stale_data_fail",
					Severity: diag.ERROR,
					Type:     diag.DATABASE,
				},
			},
		},
		{
			Name: "post_resource_resolver",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("RemoveStaleData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				db.On("Dialect").Return(noopDialect{})
				db.On("CopyFrom", mock.Anything, mock.Anything, true, map[string]interface{}(nil)).Return(nil)
				return db
			},
			Table: &schema.Table{
				Name:                 "simple",
				Resolver:             returnValueResolver,
				Columns:              commonColumns,
				PostResourceResolver: postResourceResolver,
			},
			ExpectedResourceCount: 1,
		},
		{
			Name: "post_resource_resolver_fail",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("RemoveStaleData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				db.On("Dialect").Return(noopDialect{})
				db.On("CopyFrom", mock.Anything, mock.Anything, true, map[string]interface{}(nil)).Return(nil)
				return db
			},
			Table: &schema.Table{
				Name:                 "simple",
				Resolver:             returnValueResolver,
				Columns:              commonColumns,
				PostResourceResolver: postResourceResolverError,
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "some error",
					Resource: "simple",
					Severity: diag.ERROR,
					Type:     diag.RESOLVING,
					Summary:  "some error",
				},
				{
					Err:      "some error 2",
					Resource: "simple",
					Severity: diag.ERROR,
					Type:     diag.RESOLVING,
					Summary:  "some error 2",
				},
			},
		},
		{
			Name: "failing_column",
			SetupStorage: func(t *testing.T) Storage {
				db := new(DatabaseMock)
				db.On("RemoveStaleData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				db.On("Dialect").Return(noopDialect{})
				db.On("CopyFrom", mock.Anything, mock.Anything, true, map[string]interface{}(nil)).Return(nil)
				return db
			},
			Table: &schema.Table{
				Name:     "column",
				Resolver: returnValueResolver,
				Columns: schema.ColumnList{
					{
						Name: "name",
						Resolver: func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
							return fmt.Errorf("failed column")
						},
					},
				},
			},
			ErrorExpected: true,
			ExpectedDiags: []diag.FlatDiag{
				{
					Err:      "failed column",
					Resource: "failing_column",
					Severity: diag.ERROR,
					Type:     diag.RESOLVING,
					Summary:  "failed to resolve resource failing_column",
				},
			},
		},
		{
			Name: "internal_column",
			Table: &schema.Table{
				Name:     "column",
				Resolver: returnValueResolver,
				Columns:  commonColumns,
			},
			ExpectedResourceCount: 1,
		},
	}

	executionClient := executionClient{testlog.New(t)}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var storage Storage = noopStorage{}
			if tc.SetupStorage != nil {
				storage = tc.SetupStorage(t)
			}
			exec := NewTableExecutor(tc.Name, storage, testlog.New(t), tc.Table, tc.ExtraFields, nil)
			count, diags := exec.Resolve(context.Background(), executionClient, nil)
			assert.Equal(t, tc.ExpectedResourceCount, count)
			if tc.ErrorExpected {
				require.True(t, diags.HasDiags())
				if tc.ExpectedDiags != nil {
					assert.Equal(t, tc.ExpectedDiags, diag.FlattenDiags(diags))
				}
			} else {
				require.Nil(t, diags)
			}
		})
	}
}

var testZeroTable = &schema.Table{
	Name: "test_zero_table",
	Columns: []schema.Column{
		{
			Name: "zero_bool",
			Type: schema.TypeBool,
		},
		{
			Name: "zero_int",
			Type: schema.TypeBigInt,
		},
		{
			Name: "not_zero_bool",
			Type: schema.TypeBool,
		},
		{
			Name: "not_zero_int",
			Type: schema.TypeBigInt,
		},
		{
			Name: "zero_int_ptr",
			Type: schema.TypeBigInt,
		},
		{
			Name: "not_zero_int_ptr",
			Type: schema.TypeBigInt,
		},
		{
			Name: "zero_string",
			Type: schema.TypeString,
		},
	},
}

type zeroValuedStruct struct {
	ZeroBool      bool   `default:"false"`
	ZeroInt       int    `default:"0"`
	NotZeroInt    int    `default:"5"`
	NotZeroBool   bool   `default:"true"`
	ZeroIntPtr    *int   `default:"0"`
	NotZeroIntPtr *int   `default:"5"`
	ZeroString    string `default:""`
}

func TestTableExecutor_resolveColumns(t *testing.T) {
	object := zeroValuedStruct{}
	_ = defaults.Set(&object)
	var storage Storage = noopStorage{}
	exec := NewTableExecutor(testZeroTable.Name, storage, testlog.New(t), testZeroTable, nil, nil)
	r := schema.NewResourceData(noopDialect{}, testZeroTable, nil, object, nil, exec.executionStart)
	// columns should be resolved from ColumnResolver functions or default functions
	err := exec.resolveColumns(context.TODO(), executionClient{testlog.New(t)}, r, testZeroTable.Columns)
	assert.Nil(t, err)
	v, err := r.Values()
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{false, 0, true}, v[0:3])
	assert.Equal(t, 0, *v[4].(*int))
	assert.Equal(t, 5, *v[5].(*int))
	object.ZeroIntPtr = nil
	r = schema.NewResourceData(noopDialect{}, testZeroTable, nil, object, nil, exec.executionStart)
	// columns should be resolved from ColumnResolver functions or default functions
	err = exec.resolveColumns(context.TODO(), executionClient{testlog.New(t)}, r, testZeroTable.Columns)
	assert.Nil(t, err)
	v, _ = r.Values()
	assert.Equal(t, nil, v[4])
}
