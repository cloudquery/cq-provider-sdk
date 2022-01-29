package execution

import (
	"context"
	"fmt"
	"testing"

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
	ExpectedDiags         []diagFlat
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
)

type diagFlat struct {
	Err      string
	Resource string
	Type     diag.DiagnosticType
	Severity diag.Severity
	Summary  string
}

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
			ErrorExpected: true,
			ExpectedDiags: []diagFlat{
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
			ExpectedDiags: []diagFlat{
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
			ErrorExpected: true,
			ExpectedDiags: []diagFlat{
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
			ExpectedDiags: []diagFlat{
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
			ErrorExpected: true,
			ExpectedDiags: []diagFlat{
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
			ExpectedDiags: []diagFlat{
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
			ExpectedDiags: []diagFlat{
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
			ExpectedDiags: []diagFlat{
				{
					Err:      "table resolver ignored error. Error: simple",
					Resource: "error_returning_ignore",
					Severity: diag.IGNORE,
					Summary:  "table resolver ignored error. Error: simple",
					Type:     diag.RESOLVING,
				},
			},
		},
	}

	executionClient := executionClient{testlog.New(t)}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var storage Storage = doNothingStorage{}
			if tc.SetupStorage != nil {
				storage = tc.SetupStorage(t)
			}
			exec := CreateTableExecutor(tc.Name, storage, testlog.New(t), tc.Table, tc.ExtraFields)
			count, diags := exec.Resolve(context.Background(), executionClient, nil)
			assert.Equal(t, tc.ExpectedResourceCount, count)
			if tc.ErrorExpected {
				require.True(t, diags.HasDiags())
				if tc.ExpectedDiags != nil {
					assert.Equal(t, tc.ExpectedDiags, flattenDiags(diags))
				}
			} else {
				require.Nil(t, diags)
			}
		})
	}
}

func flattenDiags(dd diag.Diagnostics) []diagFlat {
	df := make([]diagFlat, len(dd))
	for i, d := range dd {

		df[i] = diagFlat{
			Err:      d.Error(),
			Resource: d.Description().Resource,
			Type:     d.Type(),
			Severity: d.Severity(),
			Summary:  d.Description().Summary,
		}
	}
	return df
}

//
//import (
//	"context"
//	"errors"
//	"fmt"
//	"github.com/cloudquery/cq-provider-sdk/provider/schema"
//	"testing"
//
//	"github.com/cloudquery/cq-provider-sdk/logging"
//	"github.com/creasty/defaults"
//	"github.com/hashicorp/go-hclog"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/mock"
//)
//
//var alwaysDeleteTable = &schema.Table{
//	Name:         "always_delete_test_table",
//	AlwaysDelete: true,
//	Columns:      []schema.Column{{Name: "name", Type: TypeString}},
//}
//
//var testMultiplexTable = &schema.Table{
//	Name: "test_multiplex_table",
//	Multiplex: func(meta ClientMeta) []ClientMeta {
//		return []ClientMeta{meta}
//	},
//	Resolver: func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//		return nil
//	},
//	Columns: []schema.Column{
//		{
//			Name: "name",
//			Type: schema.TypeString,
//		},
//	},
//	Relations: []*schema.Table{
//		{
//			Name: "test_relation_multiplex_table",
//			Multiplex: func(meta ClientMeta) []ClientMeta {
//				return []ClientMeta{meta}
//			},
//			Resolver: func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//				return nil
//			},
//			Columns: []schema.Column{
//				{
//					Name: "name",
//					Type: schema.TypeString,
//				},
//			},
//		},
//	},
//}
//
//var testTable = &schema.Table{
//	Name: "test_table",
//	Columns: []schema.Column{
//		{
//			Name: "name",
//			Type: schema.TypeString,
//		},
//		{
//			Name:     "name_no_prefix",
//			Type:     schema.TypeString,
//			Resolver: schema.PathResolver("Inner.NameNoPrefix"),
//		},
//		{
//			Name:     "prefix_name",
//			Type:     schema.TypeString,
//			Resolver: schema.PathResolver("Prefix.Name"),
//		},
//	},
//}
//
//type testTableStruct struct {
//	Name  string `default:"test"`
//	Inner struct {
//		NameNoPrefix string `default:"name_no_prefix"`
//	}
//	Prefix struct {
//		Name string `default:"prefix_name"`
//	}
//}
//
//var testDefaultsTable = &schema.Table{
//	Name: "test_table",
//	Columns: []schema.Column{
//		{
//			Name:    "name",
//			Type:    schema.TypeString,
//			Default: "defaultValue",
//		},
//	},
//}
//
//type testDefaultsTableData struct {
//	Name         *string
//	DefaultValue string
//}
//
//var testBadColumnResolverTable = &schema.Table{
//	Name: "test_table",
//	Columns: []schema.Column{
//		{
//			Name: "name",
//			Type: schema.TypeString,
//			Resolver: func(ctx context.Context, meta ClientMeta, resource *schema.Resource, c schema.Column) error {
//				data := resource.Item.(testDefaultsTableData)
//				if data.Name != nil && *data.Name == "noError" {
//					return nil
//				}
//				return errors.New("ERROR")
//			},
//		},
//	},
//	Resolver: func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//		res <- testDefaultsTableData{Name: nil}
//		return nil
//	},
//}
//
//var testIgnoreErrorColumnResolverTable = &schema.Table{
//	Name: "test_table",
//	Columns: []schema.Column{
//		{
//			Name:    "name",
//			Type:    schema.TypeString,
//			Default: "defaultName",
//			IgnoreError: func(err error) bool {
//				return true
//			},
//			Resolver: func(ctx context.Context, meta ClientMeta, resource *schema.Resource, c schema.Column) error {
//				return errors.New("ERROR")
//			},
//		},
//		{
//			Name: "default_value",
//			Type: schema.TypeString,
//			IgnoreError: func(err error) bool {
//				return true
//			},
//			Default: "TestValue",
//			Resolver: func(ctx context.Context, meta ClientMeta, resource *schema.Resource, c schema.Column) error {
//				return errors.New("ERROR")
//			},
//		},
//	},
//	Resolver: func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//		res <- testDefaultsTableData{Name: nil}
//		return nil
//	},
//}
//
//func failingTableResolver(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//	return fmt.Errorf("table resolve failed")
//}
//
//func doNothingResolver(_ context.Context, _ ClientMeta, _ *schema.Resource, _ chan<- interface{}) error {
//	return nil
//}
//
//func dataReturningResolver(_ context.Context, _ ClientMeta, _ *schema.Resource, res chan<- interface{}) error {
//	object := testTableStruct{}
//	_ = defaults.Set(&object)
//	res <- []testTableStruct{object, object, object}
//	return nil
//}
//
//func dataReturningSingleResolver(_ context.Context, _ ClientMeta, _ *schema.Resource, res chan<- interface{}) error {
//	object := testTableStruct{}
//	_ = defaults.Set(&object)
//	res <- object
//	return nil
//}
//
//func passingNilResolver(_ context.Context, _ ClientMeta, _ *schema.Resource, res chan<- interface{}) error {
//	res <- nil
//	return nil
//}
//
//func TestExecutionData_ResolveTable(t *testing.T) {
//
//	mockedClient := new(mockedClientMeta)
//	logger := logging.New(&hclog.LoggerOptions{
//		Name:   "test_log",
//		Level:  hclog.Error,
//		Output: nil,
//	})
//	mockedClient.On("Logger", mock.Anything).Return(logger)
//
//	t.Run("failed table resolver", func(t *testing.T) {
//		testTable.Resolver = failingTableResolver
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Error(t, err)
//	})
//
//	t.Run("failing table column resolver", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		execFailing := NewExecutionData(mockDb, logger, testBadColumnResolverTable, nil, false)
//		_, err := execFailing.Resolve(context.Background(), mockedClient, nil)
//		assert.Error(t, err)
//	})
//
//	t.Run("ignore error table column resolver w/partialFetch", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testIgnoreErrorColumnResolverTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testIgnoreErrorColumnResolverTable, exec.executionStart, mock.Anything).Return(nil)
//		var expectedResource *Resource
//		testIgnoreErrorColumnResolverTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			expectedResource = parent
//			return nil
//		}
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Len(t, exec.PartialFetchFailureResult, 0)
//		assert.Equal(t, "TestValue", expectedResource.Get("default_value"))
//		assert.Equal(t, "defaultName", expectedResource.Get("name"))
//	})
//
//	t.Run("error table column resolver w/partialFetch", func(t *testing.T) {
//		testBadColumnResolverTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *Resource, res chan<- interface{}) error {
//			someString := "noError"
//			res <- []testDefaultsTableData{{Name: &someString}, {Name: nil}, {Name: &someString}}
//			return nil
//		}
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testBadColumnResolverTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testBadColumnResolverTable, exec.executionStart, mock.Anything).Return(nil)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Len(t, exec.PartialFetchFailureResult, 1)
//	})
//
//	t.Run("doing nothing resolver", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		testTable.Resolver = doNothingResolver
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//	})
//
//	t.Run("simple returning resources insert", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		testTable.Resolver = dataReturningResolver
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		assert.Nil(t, err)
//	})
//	t.Run("simple returning single resources insert", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		testTable.Resolver = dataReturningSingleResolver
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//	})
//	t.Run("simple returning nil resources insert", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		testTable.Resolver = passingNilResolver
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 0)
//	})
//	t.Run("check post row resolver", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		testTable.Resolver = dataReturningSingleResolver
//		var expectedResource *Resource
//		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			err := parent.Set("name", "other")
//			assert.Nil(t, err)
//			expectedResource = parent
//			return nil
//		}
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Equal(t, expectedResource.data["name"], "other")
//		assert.Nil(t, err)
//		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			return errors.New("error")
//		}
//		_, err = exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Error(t, err)
//	})
//
//	t.Run("test resolving with default column values", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testDefaultsTable, nil, false)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testDefaultsTable, exec.executionStart, mock.Anything).Return(nil)
//		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *Resource, res chan<- interface{}) error {
//			res <- testDefaultsTableData{Name: nil}
//			return nil
//		}
//		var expectedResource *Resource
//		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			expectedResource = parent
//			return nil
//		}
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Equal(t, expectedResource.data["name"], "defaultValue")
//	})
//
//	t.Run("disable delete", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		testTable.Resolver = dataReturningSingleResolver
//		testTable.DeleteFilter = func(meta ClientMeta, r *Resource) []interface{} {
//			return nil
//		}
//		var expectedResource *Resource
//		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			err := parent.Set("name", "other")
//			assert.Nil(t, err)
//			expectedResource = parent
//			return nil
//		}
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		assert.Equal(t, expectedResource.data["name"], "other")
//		assert.Nil(t, err)
//
//		// new mockDb, new call counts
//		mockDb = new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		exec = NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		_, err = exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "RemoveStaleData", 1)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		assert.Nil(t, err)
//	})
//	t.Run("disable delete w/deleteFilter", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, map[string]interface{}{"test": 1}, false)
//		testTable.Resolver = dataReturningSingleResolver
//		testTable.DeleteFilter = func(meta ClientMeta, r *schema.Resource) []interface{} {
//			return []interface{}{"a", 2}
//		}
//		var expectedResource *schema.Resource
//		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			err := parent.Set("name", "other")
//			assert.Nil(t, err)
//			expectedResource = parent
//			return nil
//		}
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, []interface{}{"test", 1, "a", 2}).Return(nil)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		assert.Equal(t, expectedResource.Get("name"), "other")
//		assert.Nil(t, err)
//
//		// new mockDb, new call counts
//		mockDb = new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		exec = NewExecutionData(mockDb, logger, testTable, nil, false)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		_, err = exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "RemoveStaleData", 1)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		assert.Nil(t, err)
//	})
//
//	t.Run("disable delete failed copy from", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, testTable, nil, false)
//		testTable.Resolver = dataReturningSingleResolver
//		testTable.DeleteFilter = func(meta ClientMeta, r *schema.Resource) []interface{} {
//			return nil
//		}
//		var expectedResource *schema.Resource
//		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			err := parent.Set("name", "other")
//			assert.Nil(t, err)
//			expectedResource = parent
//			return nil
//		}
//		mockDb.On("RemoveStaleData", mock.Anything, testTable, exec.executionStart, mock.Anything).Return(nil)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(fmt.Errorf("some error"))
//		mockDb.On("Insert", mock.Anything, mock.Anything, mock.Anything).Return(nil)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "CopyFrom", 1)
//		mockDb.AssertNumberOfCalls(t, "Insert", 1)
//		assert.Equal(t, expectedResource.Get("name"), "other")
//		assert.Nil(t, err)
//	})
//
//	t.Run("always delete with disable delete", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		exec := NewExecutionData(mockDb, logger, alwaysDeleteTable, nil, false)
//		alwaysDeleteTable.Resolver = dataReturningSingleResolver
//		alwaysDeleteTable.DeleteFilter = func(meta ClientMeta, r *schema.Resource) []interface{} {
//			return nil
//		}
//		var expectedResource *schema.Resource
//		alwaysDeleteTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			err := parent.Set("name", "other")
//			assert.Nil(t, err)
//			expectedResource = parent
//			return nil
//		}
//		mockDb.On("RemoveStaleData", mock.Anything, alwaysDeleteTable, exec.executionStart, mock.Anything).Return(nil)
//		mockDb.On("Delete", mock.Anything, alwaysDeleteTable, mock.Anything).Return(nil)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.AssertNumberOfCalls(t, "Delete", 0)
//		_, err := exec.Resolve(context.Background(), mockedClient, nil)
//		mockDb.AssertNumberOfCalls(t, "Delete", 1)
//		assert.Equal(t, expectedResource.Get("name"), "other")
//		assert.Nil(t, err)
//	})
//
//	t.Run("test partial fetch post resource resolver", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		execDefault := NewExecutionData(mockDb, logger, testDefaultsTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testDefaultsTable, execDefault.executionStart, mock.Anything).Return(nil)
//		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//			res <- testDefaultsTableData{Name: nil}
//			return nil
//		}
//		var expectedResource *schema.Resource
//		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			expectedResource = parent
//			return fmt.Errorf("random failure")
//		}
//		_, err := execDefault.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Equal(t, expectedResource.Get("name"), "defaultValue")
//	})
//
//	t.Run("test partial fetch resolver", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		execDefault := NewExecutionData(mockDb, logger, testDefaultsTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testDefaultsTable, execDefault.executionStart, mock.Anything).Return(nil)
//		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *Resource, res chan<- interface{}) error {
//			res <- testDefaultsTableData{Name: nil}
//			return fmt.Errorf("random failure")
//		}
//		var expectedResource *Resource
//		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
//			expectedResource = parent
//			return nil
//		}
//		_, err := execDefault.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Equal(t, expectedResource.data["name"], "defaultValue")
//		assert.Len(t, execDefault.PartialFetchFailureResult, 1)
//		assert.Equal(t, "table resolve error: random failure", execDefault.PartialFetchFailureResult[0].Error())
//	})
//
//	t.Run("test partial fetch resolver panic", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(PostgresDialect{})
//		execDefault := NewExecutionData(mockDb, logger, testDefaultsTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//			res <- testDefaultsTableData{Name: nil}
//			panic("test panic")
//		}
//		var expectedResource *schema.Resource
//		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			expectedResource = parent
//			return nil
//		}
//		_, err := execDefault.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Equal(t, expectedResource.data["name"], "defaultValue")
//		assert.Len(t, execDefault.PartialFetchFailureResult, 1)
//		assert.Equal(t, "table resolve error: failed table test_table fetch. Error: test panic", execDefault.PartialFetchFailureResult[0].Error())
//	})
//
//	t.Run("test partial fetch post resource resolver panic", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		execDefault := NewExecutionData(mockDb, logger, testDefaultsTable, nil, true)
//		mockDb.On("CopyFrom", mock.Anything, mock.Anything, true, mock.Anything).Return(nil)
//		mockDb.On("RemoveStaleData", mock.Anything, testDefaultsTable, execDefault.executionStart, mock.Anything).Return(nil)
//		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
//			res <- testDefaultsTableData{Name: nil}
//			return nil
//		}
//		var expectedResource *schema.Resource
//		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *schema.Resource) error {
//			expectedResource = parent
//			panic("test panic")
//		}
//		_, err := execDefault.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.Equal(t, expectedResource.Get("name"), "defaultValue")
//		assert.Len(t, execDefault.PartialFetchFailureResult, 1)
//		assert.Equal(t, "failed to resolve resource: recovered from panic: test panic", execDefault.PartialFetchFailureResult[0].Error())
//	})
//
//	t.Run("test table with multiplex", func(t *testing.T) {
//		mockDb := new(DatabaseMock)
//		mockDb.On("Dialect").Return(schema.PostgresDialect{})
//		execDefault := NewExecutionData(mockDb, logger, testMultiplexTable, nil, true)
//		mockDb.On("RemoveStaleData", mock.Anything, testMultiplexTable, execDefault.executionStart, mock.Anything).Return(nil)
//		var parentMultiplexCalled, relationMultiplexCalled = false, false
//		testMultiplexTable.Multiplex = func(meta ClientMeta) []ClientMeta {
//			parentMultiplexCalled = true
//			return []ClientMeta{meta}
//		}
//		testMultiplexTable.Relations[0].Multiplex = func(meta ClientMeta) []ClientMeta {
//			relationMultiplexCalled = true
//			return []ClientMeta{meta}
//		}
//		_, err := execDefault.Resolve(context.Background(), mockedClient, nil)
//		assert.Nil(t, err)
//		assert.True(t, parentMultiplexCalled)
//		assert.False(t, relationMultiplexCalled)
//	})
//}
//
//// ClientMeta is an autogenerated mock type for the ClientMeta type
//type mockedClientMeta struct {
//	mock.Mock
//}
//
//// Logger provides a mock function with given fields:
//func (_m *mockedClientMeta) Logger() hclog.Logger {
//	ret := _m.Called()
//
//	var r0 hclog.Logger
//	if rf, ok := ret.Get(0).(func() hclog.Logger); ok {
//		r0 = rf()
//	} else {
//		if ret.Get(0) != nil {
//			r0 = ret.Get(0).(hclog.Logger)
//		}
//	}
//
//	return r0
//}
