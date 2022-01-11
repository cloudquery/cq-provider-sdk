package schema

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

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
	Relations: []*Table{
		{
			Name:    "test_pk_rel_table",
			Options: TableCreationOptions{PrimaryKeys: []string{"primary_rel_key_str"}},
			Columns: []Column{
				{
					Name: "rel_key_str",
					Type: TypeString,
				},
			},
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
	r := NewResourceData(testPrimaryKeyTable, nil, nil, nil, time.Now())
	// save random id
	randomId := r.cqId
	// test primary table no pk
	assert.Error(t, r.GenerateCQId(), "Error expected, primary key value not set")
	// Id shouldn't change
	assert.Equal(t, randomId, r.cqId)
	err := r.Set("primary_key_str", "test")
	assert.Nil(t, err)
	assert.Nil(t, r.GenerateCQId())
	assert.NotEqual(t, randomId, r.cqId)
	randomId = r.cqId
	// validate consistency
	assert.Nil(t, r.GenerateCQId())
	assert.Equal(t, randomId, r.cqId)
	// check key length of array is as expected
	assert.Len(t, r.Keys(), 1)
}

func TestRelationResourcePrimaryKey(t *testing.T) {
	r := NewResourceData(testPrimaryKeyTable, nil, nil, nil, time.Now())
	r2 := NewResourceData(r.table.Relations[0], r, map[string]interface{}{
		"rel_key_str": "test",
	}, nil, time.Now())

	mockedClient := new(mockedClientMeta)
	logger := logging.New(&hclog.LoggerOptions{
		Name:   "test_log",
		Level:  hclog.Error,
		Output: nil,
	})
	mockedClient.On("Logger", mock.Anything).Return(logger)

	exec := NewExecutionData(nil, logger, r2.table, false, nil, false)
	err := exec.resolveResourceValues(context.TODO(), mockedClient, r2)
	assert.Nil(t, err)
	v, err := r2.Values()
	assert.Nil(t, err)
	assert.Equal(t, v[1], r2.cqId)
}

// TestResourcePrimaryKey checks resource id generation when primary key is set on table
func TestResourceAddColumns(t *testing.T) {
	r := NewResourceData(testPrimaryKeyTable, nil, nil, map[string]interface{}{"new_field": 1}, time.Now())
	assert.Equal(t, []string{"primary_key_str", "cq_id", "cq_meta", "cq_fetch_date", "new_field"}, r.columns)
}

func TestResourceColumns(t *testing.T) {
	r := NewResourceData(testTable, nil, nil, nil, time.Now())
	errf := r.Set("name", "test")
	assert.Nil(t, errf)
	assert.Equal(t, r.Get("name"), "test")
	v, err := r.Values()
	assert.Nil(t, err)
	assert.Equal(t, v, []interface{}{"test", nil, nil, nil, nil, nil})
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
	assert.Equal(t, v, []interface{}{"test", "name_no_prefix", "prefix_name", nil, nil, nil})

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
		logger := logging.New(&hclog.LoggerOptions{
			Name:   "test_log",
			Level:  hclog.Error,
			Output: nil,
		})
		exec := NewExecutionData(nil, logger, testTable, false, nil, false)
		r := NewResourceData(testTable, nil, object, nil, exec.executionStart)
		assert.Equal(t, r.cqId, r.Id())
		// columns should be resolved from ColumnResolver functions or default functions
		err := exec.resolveColumns(context.TODO(), mockedClient, r, testTable.Columns)
		assert.Nil(t, err)
		v, err := r.Values()
		assert.Nil(t, err)
		assert.Equal(t, v, []interface{}{"test", "name_no_prefix", "prefix_name", nil, nil, nil})
	})

	t.Run("test resolve zero columns", func(t *testing.T) {
		object := zeroValuedStruct{}
		_ = defaults.Set(&object)
		logger := logging.New(&hclog.LoggerOptions{
			Name:   "test_log",
			Level:  hclog.Error,
			Output: nil,
		})
		exec := NewExecutionData(nil, logger, testZeroTable, false, nil, false)

		r := NewResourceData(testZeroTable, nil, object, nil, exec.executionStart)
		assert.Equal(t, r.cqId, r.Id())
		// columns should be resolved from ColumnResolver functions or default functions
		err := exec.resolveColumns(context.TODO(), mockedClient, r, testZeroTable.Columns)
		assert.Nil(t, err)
		v, err := r.Values()
		assert.Nil(t, err)
		assert.Equal(t, []interface{}{false, 0, true}, v[:3])
		assert.Equal(t, 0, *v[4].(*int))
		assert.Equal(t, 5, *v[5].(*int))
		assert.Equal(t, "", v[6].(string))
		assert.Equal(t, nil, v[7])
		assert.Equal(t, nil, v[8])

		object.ZeroIntPtr = nil
		r = NewResourceData(testZeroTable, nil, object, nil, time.Now())
		err = exec.resolveColumns(context.TODO(), mockedClient, r, testZeroTable.Columns)
		assert.Nil(t, err)
		v, _ = r.Values()
		assert.Equal(t, nil, v[4])
	})
}

func TestResources(t *testing.T) {
	r1 := NewResourceData(testPrimaryKeyTable, nil, nil, map[string]interface{}{"new_field": 1}, time.Now())
	r2 := NewResourceData(testPrimaryKeyTable, nil, nil, map[string]interface{}{"new_field": 1}, time.Now())
	assert.Equal(t, []string{"primary_key_str", "cq_id", "cq_meta", "cq_fetch_date", "new_field"}, r1.columns)
	assert.Equal(t, []string{"primary_key_str", "cq_id", "cq_meta", "cq_fetch_date", "new_field"}, r2.columns)

	rr := Resources{r1, r2}
	assert.Equal(t, []string{"primary_key_str", "cq_id", "cq_meta", "cq_fetch_date", "new_field"}, rr.ColumnNames())
	assert.Equal(t, testPrimaryKeyTable.Name, rr.TableName())
	_ = r1.Set("primary_key_str", "test")
	_ = r2.Set("primary_key_str", "test2")
	_ = r1.GenerateCQId()
	_ = r2.GenerateCQId()
	assert.Equal(t, []uuid.UUID{r1.Id(), r2.Id()}, rr.GetIds())
}
