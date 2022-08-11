package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type jsonTestType struct {
	Name        string `json:"name"`
	Description string `json:"decription"`
	Version     int    `json:"version"`
}

type jsonNoTags struct {
	Name        string
	Description string
	Version     int
}

var (
	stringJson    = "{\"test\":true}"
	jsonTestTable = Table{
		Name: "test_table_validator",
		Columns: []Column{
			{
				Name: "test",
				Type: TypeJSON,
			},
		},
	}
	intTestTable = Table{
		Name: "test_table_validator",
		Columns: []Column{
			{
				Name: "int32",
				Type: TypeInt,
			},
			{
				Name: "int64",
				Type: TypeInt,
			},
		},
	}
	timestampTestTable = Table{
		Name: "test_table_timestamp",
		Columns: []Column{
			{
				Name: "name",
				Type: TypeString,
			},
			{
				Name: "time",
				Type: TypeTimestamp,
			},
		},
	}
	resources = []Resource{
		{
			data: map[string]interface{}{
				"test":    stringJson,
				"cq_meta": make(map[string]string),
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": &stringJson,
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": map[string]interface{}{
					"test": 1,
					"test1": map[string]interface{}{
						"test": 1,
					},
				},
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": []interface{}{
					map[string]interface{}{
						"test":  1,
						"test1": true,
					},
					map[string]interface{}{
						"test":  1,
						"test1": true,
					},
				},
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": nil,
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": []interface{}{
					nil,
				},
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": "{\"hello\":123}",
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": jsonTestType{
					Name:        "test",
					Description: "test1",
					Version:     10,
				},
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": jsonNoTags{
					Name:        "test",
					Description: "test1",
					Version:     10,
				},
			},
			table: &jsonTestTable,
		},
	}

	failResources = []Resource{
		{
			data: map[string]interface{}{
				"test": true,
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": 10.1,
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": "true_test",
			},
			table: &jsonTestTable,
		},
		{
			data: map[string]interface{}{
				"test": "{\"hello\":123}1",
			},
			table: &jsonTestTable,
		},
	}

	intResources = []Resource{
		{
			data: map[string]interface{}{
				"int32": 123,
				"int64": int64(123),
			},
			table: &intTestTable,
		},
		{
			data: map[string]interface{}{
				"int32": 123,
				"int64": int64(9223372036854775807),
			},
			table: &intTestTable,
		},
	}
)

func TestJsonColumn(t *testing.T) {
	for _, r := range resources {
		_, err := PostgresDialect{}.GetResourceValues(&r)
		assert.Nil(t, err)
	}

	for _, r := range failResources {
		_, err := PostgresDialect{}.GetResourceValues(&r)
		assert.Error(t, err)
	}
}

func TestIntColumn(t *testing.T) {
	for _, r := range intResources {
		_, err := PostgresDialect{}.GetResourceValues(&r)
		assert.Nil(t, err)
	}
}

func TestNonUtcTimestampColumn(t *testing.T) {
	exampleName := "exampleName"

	newYorkLocation, err := time.LoadLocation("America/New_York")
	if err != nil {
		assert.FailNow(t, "failed to load location")
	}

	exampleNewYorkTime := time.Date(2001, 1, 1, 1, 1, 1, 1, newYorkLocation)

	timestampResource := Resource{
		data: map[string]interface{}{
			"name": exampleName,
			"time": exampleNewYorkTime,
		},
		table: &timestampTestTable,
	}

	values, err := PostgresDialect{}.GetResourceValues(&timestampResource)
	if err != nil {
		assert.FailNow(t, "GetResourceValues failed")
	}

	// First two 'values' are cq_id and cq_meta
	assert.Equal(t, nil, values[0])
	assert.Equal(t, nil, values[1])
	assert.Equal(t, exampleName, values[2])

	concreteValueTime := values[3].(time.Time)
	assert.True(t, exampleNewYorkTime.Equal(concreteValueTime))
	assert.Equal(t, time.UTC, concreteValueTime.Location())
}
