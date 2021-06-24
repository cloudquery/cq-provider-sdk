package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/creasty/defaults"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

var testZeroTable = &Table{
	Name: "test_zero_table",
	Columns: []Column{
		{
			Name: "zero_bool",
			Type: TypeBool,
		},
		{
			Name: "zero_int",
			Type: TypeBigInt,
		},
		{
			Name: "not_zero_bool",
			Type: TypeBool,
		},
		{
			Name: "not_zero_int",
			Type: TypeBigInt,
		},
		{
			Name: "zero_int_ptr",
			Type: TypeBigInt,
		},
		{
			Name: "not_zero_int_ptr",
			Type: TypeBigInt,
		},
		{
			Name: "zero_string",
			Type: TypeString,
		},
	},
}

var testPrimaryKeyTable = &Table{
	Name:    "test_pk_table",
	Options: TableCreationOptions{PrimaryKeys: []string{"primary_key_str"}},
	Columns: []Column{
		{
			Name: "primary_key_str",
			Type: TypeString,
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

// TestResourcePrimaryKey checks resource id generation when primary key is set on table
func TestResourcePrimaryKey(t *testing.T) {
	r := NewResourceData(testPrimaryKeyTable, nil, nil, nil)
	// save random id
	randomId := r.cqId
	assert.Error(t, r.GenerateCQId(), "Error expected, primary key value not set")
	// Id shouldn't change
	assert.Equal(t, randomId, r.cqId)
	err := r.Set("primary_key_str", "test")
	assert.Nil(t, err)
	assert.Nil(t, r.GenerateCQId())
	assert.NotEqual(t, randomId, r.cqId)
	randomId = r.cqId
	assert.Nil(t, r.GenerateCQId())
	assert.Equal(t, randomId, r.cqId)
}

// TestResourcePrimaryKey checks resource id generation when primary key is set on table
func TestResourceAddColumns(t *testing.T) {
	r := NewResourceData(testPrimaryKeyTable, nil, nil, map[string]interface{}{"new_field": 1})
	assert.Equal(t, []string{"cq_id", "primary_key_str", "new_field"}, r.columns)
}

func TestResourceColumns(t *testing.T) {

	r := NewResourceData(testTable, nil, nil, nil)
	errf := r.Set("name", "test")
	assert.Nil(t, errf)
	assert.Equal(t, r.Get("name"), "test")
	v, err := r.Values()
	assert.Nil(t, err)
	assert.Equal(t, v, []interface{}{r.Id(), "test", nil, nil})
	// Set invalid type to resource
	errf = r.Set("name", 5)
	assert.Nil(t, errf)
	v, err = r.Values()
	assert.Error(t, err)
	assert.Nil(t, v)

	// Set resource fully
	errf = r.Set("name", "test")
	assert.Nil(t, errf)
	errf = r.Set("name_no_prefix", "name_no_prefix")
	assert.Nil(t, errf)
	errf = r.Set("prefix_name", "prefix_name")
	assert.Nil(t, errf)
	v, err = r.Values()
	assert.Nil(t, err)
	assert.Equal(t, v, []interface{}{r.cqId, "test", "name_no_prefix", "prefix_name"})

	// check non existing col
	err = r.Set("non_exist_col", "test")
	assert.Error(t, err)
}

func TestResourceResolveColumns(t *testing.T) {
	mockedClient := new(mockedClientMeta)
	logger := logging.New(&hclog.LoggerOptions{
		Name:   "test_log",
		Level:  hclog.Error,
		Output: nil,
	})
	mockedClient.On("Logger", mock.Anything).Return(logger)

	t.Run("test resolve column normal", func(t *testing.T) {
		object := testTableStruct{}
		_ = defaults.Set(&object)
		r := NewResourceData(testTable, nil, object, nil)
		assert.Equal(t, r.cqId, r.Id())
		// columns should be resolved from ColumnResolver functions or default functions
		logger := logging.New(&hclog.LoggerOptions{
			Name:   "test_log",
			Level:  hclog.Error,
			Output: nil,
		})
		exec := NewExecutionData(nil, logger, testTable, false, nil)
		err := exec.resolveColumns(context.TODO(), mockedClient, r, testTable.Columns)
		assert.Nil(t, err)
		v, err := r.Values()
		assert.Nil(t, err)
		assert.Equal(t, v, []interface{}{r.cqId, "test", "name_no_prefix", "prefix_name"})
	})

	t.Run("test resolve zero columns", func(t *testing.T) {
		object := zeroValuedStruct{}
		_ = defaults.Set(&object)
		r := NewResourceData(testZeroTable, nil, object, nil)
		assert.Equal(t, r.cqId, r.Id())
		// columns should be resolved from ColumnResolver functions or default functions
		logger := logging.New(&hclog.LoggerOptions{
			Name:   "test_log",
			Level:  hclog.Error,
			Output: nil,
		})
		exec := NewExecutionData(nil, logger, testZeroTable, false, nil)
		err := exec.resolveColumns(context.TODO(), mockedClient, r, testZeroTable.Columns)
		assert.Nil(t, err)
		v, err := r.Values()
		assert.Nil(t, err)
		assert.Equal(t, []interface{}{r.cqId, false, 0, true}, v[:4])
		assert.Equal(t, 0, *v[5].(*int))
		assert.Equal(t, 5, *v[6].(*int))
		assert.Equal(t, "", v[7].(string))

		object.ZeroIntPtr = nil
		r = NewResourceData(testZeroTable, nil, object, nil)
		err = exec.resolveColumns(context.TODO(), mockedClient, r, testZeroTable.Columns)
		assert.Nil(t, err)
		v, _ = r.Values()
		assert.Equal(t, nil, v[5])
	})
}
