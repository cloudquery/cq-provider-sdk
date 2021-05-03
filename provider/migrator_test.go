package provider

import (
	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"testing"
)

var testTable = schema.Table{
	Name: "test_table_validator",
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
	},
}

func TestMigratorTableValidators(t *testing.T) {
	logger := logging.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		JSONFormat: true,
	})
	m := NewMigrator(nil, logger)

	// table has passed all validators
	err := m.ValidateTable(&testTable)
	assert.Nil(t, err)

	// table name is too long
	tableWithLongName := testTable
	tableWithLongName.Name = "WithLongNametableWithLongNametableWithLongNametableWithLongNamet"
	err = m.ValidateTable(&tableWithLongName)
	assert.Error(t, err)

	// column name is too long
	tableWithLongColumnName := testTable
	tableWithLongName.Columns[0].Name = "tableWithLongColumnNametableWithLongColumnNametableWithLongColumnName"
	err = m.ValidateTable(&tableWithLongColumnName)
	assert.Error(t, err)
}
