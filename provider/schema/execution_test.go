package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/creasty/defaults"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var alwaysDeleteTable = &Table{
	Name:         "always_delete_test_table",
	AlwaysDelete: true,
	Columns: []Column{{Name: "name", Type: TypeString},
	},
}

var testTable = &Table{
	Name: "test_table",
	Columns: []Column{
		{
			Name: "name",
			Type: TypeString,
		},
		{
			Name:     "name_no_prefix",
			Type:     TypeString,
			Resolver: PathResolver("Inner.NameNoPrefix"),
		},
		{
			Name:     "prefix_name",
			Type:     TypeString,
			Resolver: PathResolver("Prefix.Name"),
		},
	},
}

type testTableStruct struct {
	Name  string `default:"test"`
	Inner struct {
		NameNoPrefix string `default:"name_no_prefix"`
	}
	Prefix struct {
		Name string `default:"prefix_name"`
	}
}

var testDefaultsTable = &Table{
	Name: "test_table",
	Columns: []Column{
		{
			Name:    "name",
			Type:    TypeString,
			Default: "defaultValue",
		},
	},
}

type testDefaultsTableData struct {
	Name *string
}

var testBadColumnResolverTable = &Table{
	Name: "test_table",
	Columns: []Column{
		{
			Name: "name",
			Type: TypeString,
			Resolver: func(ctx context.Context, meta ClientMeta, resource *Resource, c Column) error {
				return errors.New("ERROR")
			},
		},
	},
	Resolver: func(ctx context.Context, meta ClientMeta, parent *Resource, res chan interface{}) error {
		res <- testDefaultsTableData{Name: nil}
		return nil
	},
}

func failingTableResolver(ctx context.Context, meta ClientMeta, parent *Resource, res chan interface{}) error {
	return fmt.Errorf("table resolve failed")
}

func doNothingResolver(_ context.Context, _ ClientMeta, _ *Resource, _ chan interface{}) error {
	return nil
}

func dataReturningResolver(_ context.Context, _ ClientMeta, _ *Resource, res chan interface{}) error {
	object := testTableStruct{}
	_ = defaults.Set(&object)
	res <- []testTableStruct{object, object, object}
	return nil
}

func dataReturningSingleResolver(_ context.Context, _ ClientMeta, _ *Resource, res chan interface{}) error {
	object := testTableStruct{}
	_ = defaults.Set(&object)
	res <- object
	return nil
}

func passingNilResolver(_ context.Context, _ ClientMeta, _ *Resource, res chan interface{}) error {
	res <- nil
	return nil
}

func TestExecutionData_ResolveTable(t *testing.T) {

	mockDb := new(mockDatabase)
	mockedClient := new(mockedClientMeta)
	logger := logging.New(&hclog.LoggerOptions{
		Name:   "test_log",
		Level:  hclog.Error,
		Output: nil,
	})
	mockedClient.On("Logger", mock.Anything).Return(logger)
	exec := NewExecutionData(mockDb, logger, testTable, false, nil)

	t.Run("failing table resolver", func(t *testing.T) {
		testTable.Resolver = failingTableResolver
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Error(t, err)
		execFailing := NewExecutionData(mockDb, logger, testBadColumnResolverTable, false, nil)
		_, err = execFailing.ResolveTable(context.Background(), mockedClient, nil)
		assert.Error(t, err)
	})

	t.Run("doing nothing resolver", func(t *testing.T) {
		testTable.Resolver = doNothingResolver
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Nil(t, err)
	})

	t.Run("simple returning resources insert", func(t *testing.T) {
		mockDb.On("Insert", mock.Anything, testTable, mock.Anything).Return(nil)
		testTable.Resolver = dataReturningResolver
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Nil(t, err)
	})
	t.Run("simple returning single resources insert", func(t *testing.T) {
		mockDb.On("Insert", mock.Anything, testTable, mock.Anything).Return(nil)
		testTable.Resolver = dataReturningSingleResolver
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Nil(t, err)
	})
	t.Run("simple returning nil resources insert", func(t *testing.T) {
		mockDb = new(mockDatabase)
		testTable.Resolver = passingNilResolver
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Nil(t, err)
		mockDb.AssertNumberOfCalls(t, "Insert", 0)
	})
	t.Run("check post row resolver", func(t *testing.T) {
		testTable.Resolver = dataReturningSingleResolver
		var expectedResource *Resource
		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			err := parent.Set("name", "other")
			assert.Nil(t, err)
			expectedResource = parent
			return nil
		}
		mockDb.On("Insert", mock.Anything, testTable, mock.Anything).Return(nil)
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Equal(t, expectedResource.data["name"], "other")
		assert.Nil(t, err)
		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			return errors.New("error")
		}
		_, err = exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Error(t, err)
	})

	t.Run("test resolving with default column values", func(t *testing.T) {
		execDefault := NewExecutionData(mockDb, logger, testDefaultsTable, false, nil)
		mockDb.On("Insert", mock.Anything, testDefaultsTable, mock.Anything).Return(nil)
		testDefaultsTable.Resolver = func(ctx context.Context, meta ClientMeta, parent *Resource, res chan interface{}) error {
			res <- testDefaultsTableData{Name: nil}
			return nil
		}
		var expectedResource *Resource
		testDefaultsTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			expectedResource = parent
			return nil
		}
		_, err := execDefault.ResolveTable(context.Background(), mockedClient, nil)
		assert.Nil(t, err)
		assert.Equal(t, expectedResource.data["name"], "defaultValue")
	})

	t.Run("disable delete", func(t *testing.T) {
		exec := NewExecutionData(mockDb, logger, testTable, true, nil)
		testTable.Resolver = dataReturningSingleResolver
		testTable.DeleteFilter = func(meta ClientMeta, r *Resource) []interface{} {
			return nil
		}
		var expectedResource *Resource
		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			err := parent.Set("name", "other")
			assert.Nil(t, err)
			expectedResource = parent
			return nil
		}
		mockDb.On("Delete", mock.Anything, testTable, mock.Anything).Return(nil)
		mockDb.On("Insert", mock.Anything, testTable, mock.Anything).Return(nil)
		mockDb.AssertNumberOfCalls(t, "Delete", 0)
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		mockDb.AssertNumberOfCalls(t, "Delete", 0)
		assert.Equal(t, expectedResource.data["name"], "other")
		assert.Nil(t, err)
		exec = NewExecutionData(mockDb, logger, testTable, false, nil)
		_, err = exec.ResolveTable(context.Background(), mockedClient, nil)
		mockDb.AssertNumberOfCalls(t, "Delete", 1)
		assert.Nil(t, err)
	})

	t.Run("always delete with disable delete", func(t *testing.T) {
		exec := NewExecutionData(mockDb, logger, alwaysDeleteTable, true, nil)
		alwaysDeleteTable.Resolver = dataReturningSingleResolver
		alwaysDeleteTable.DeleteFilter = func(meta ClientMeta, r *Resource) []interface{} {
			return nil
		}
		var expectedResource *Resource
		alwaysDeleteTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			err := parent.Set("name", "other")
			assert.Nil(t, err)
			expectedResource = parent
			return nil
		}
		mockDb.On("Delete", mock.Anything, alwaysDeleteTable, mock.Anything).Return(nil)
		mockDb.On("Insert", mock.Anything, alwaysDeleteTable, mock.Anything).Return(nil)
		mockDb.AssertNumberOfCalls(t, "Delete", 0)
		_, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		mockDb.AssertNumberOfCalls(t, "Delete", 1)
		assert.Equal(t, expectedResource.data["name"], "other")
		assert.Nil(t, err)
	})

	t.Run("inject fields into execution", func(t *testing.T) {
		exec := NewExecutionData(mockDb, logger, testTable, false, map[string]interface{}{"injected_field": 1})
		testTable.Resolver = dataReturningSingleResolver
		var expectedResource *Resource
		testTable.PostResourceResolver = func(ctx context.Context, meta ClientMeta, parent *Resource) error {
			err := parent.Set("name", "other")
			assert.Nil(t, err)
			expectedResource = parent
			return nil
		}
		mockDb.On("Insert", mock.Anything, testTable, mock.Anything).Return(nil)
		count, err := exec.ResolveTable(context.Background(), mockedClient, nil)
		assert.Equal(t, count, uint64(1))
		assert.Nil(t, err)
		assert.Equal(t, expectedResource.data["name"], "other")
		assert.Equal(t, 1, expectedResource.extraFields["injected_field"])
		values, err := expectedResource.Values()
		assert.Nil(t, err)
		assert.Equal(t, []string{"name", "name_no_prefix", "prefix_name", "cq_id", "meta", "injected_field"}, expectedResource.columns)
		assert.Equal(t, []interface{}{"other", "name_no_prefix", "prefix_name", expectedResource.cqId, expectedResource.Get("meta"), 1}, values)
	})
}

// ClientMeta is an autogenerated mock type for the ClientMeta type
type mockedClientMeta struct {
	mock.Mock
}

// Logger provides a mock function with given fields:
func (_m *mockedClientMeta) Logger() hclog.Logger {
	ret := _m.Called()

	var r0 hclog.Logger
	if rf, ok := ret.Get(0).(func() hclog.Logger); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(hclog.Logger)
		}
	}

	return r0
}
