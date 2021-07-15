package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	stringJson = "{\"test\":true}"
	r1         = Resource{
		data: map[string]interface{}{
			"test": stringJson,
			"meta": make(map[string]string),
		},
		table: &Table{
			Name: "test_table_validator",
			Columns: []Column{
				{
					Name: "test",
					Type: TypeJSON,
				},
			},
		}}
	r2 = Resource{
		data: map[string]interface{}{
			"test": &stringJson,
			"meta": make(map[string]string),
		},
		table: &Table{
			Name: "test_table_validator",
			Columns: []Column{
				{
					Name: "test",
					Type: TypeJSON,
				},
			},
		}}

	r3 = Resource{
		data: map[string]interface{}{
			"test": map[string]interface{}{
				"test": 1,
				"test1": map[string]interface{}{
					"test": 1,
				},
			},
			"meta": make(map[string]string),
		},
		table: &Table{
			Name: "test_table_validator",
			Columns: []Column{
				{
					Name: "test",
					Type: TypeJSON,
				},
			},
		}}

	r4 = Resource{
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
			"meta": make(map[string]string),
		},
		table: &Table{
			Name: "test_table_validator",
			Columns: []Column{
				{
					Name: "test",
					Type: TypeJSON,
				},
			},
		}}
)

func TestJsonColumn(t *testing.T) {
	_, err := getResourceValues(&r1)
	assert.Nil(t, err)

	_, err = getResourceValues(&r2)
	assert.Nil(t, err)

	_, err = getResourceValues(&r3)
	assert.Nil(t, err)

	_, err = getResourceValues(&r4)
	assert.Nil(t, err)
}
