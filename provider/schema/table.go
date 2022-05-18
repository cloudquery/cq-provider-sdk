package schema

import (
	"context"
	"sort"
	"strings"
)

// TableResolver is the main entry point when a table fetch is called.
//
// Table resolver has 3 main arguments:
// - meta(ClientMeta): is the client returned by the plugin.Provider Configure call
// - parent(Resource): resource is the parent resource in case this table is called via parent table (i.e. relation)
// - res(chan interface{}): is a channel to pass results fetched by the TableResolver
//
type TableResolver func(ctx context.Context, meta ClientMeta, parent *Resource, res chan<- interface{}) error

// IgnoreErrorFunc checks if returned error from table resolver should be ignored.
type IgnoreErrorFunc func(err error) bool

type RowResolver func(ctx context.Context, meta ClientMeta, resource *Resource) error

type Table struct {
	// Name of table
	Name string
	// table description
	Description string
	// Columns are the set of fields that are part of this table
	Columns ColumnList
	// Relations are a set of related tables defines
	Relations []*Table
	// Resolver is the main entry point to fetching table data and
	Resolver TableResolver
	// Ignore errors checks if returned error from table resolver should be ignored.
	IgnoreError IgnoreErrorFunc
	// Multiplex returns re-purposed meta clients. The sdk will execute the table with each of them
	Multiplex func(meta ClientMeta) []ClientMeta
	// DeleteFilter returns a list of key/value pairs to add when truncating this table's data from the database.
	DeleteFilter func(meta ClientMeta, parent *Resource) []interface{}
	// Post resource resolver is called after all columns have been resolved, and before resource is inserted to database.
	PostResourceResolver RowResolver
	// Options allow modification of how the table is defined when created
	Options TableCreationOptions
	// AlwaysDelete will always delete table data on fetch regardless if delete is disabled on run,
	// use this only in specific cases, if you are unsure contact the CloudQuery Team.
	AlwaysDelete bool

	// IgnoreInTests is used to exclude a table from integration tests.
	// By default, integration tests fetch all resources from cloudquery's test account, and verifY all tables
	// have at least one row.
	// When IgnoreInTests is true, integration tests won't fetch from this table.
	// Used when it is hard to create a reproducible environment with a row in this table.
	IgnoreInTests bool
	// Global tables are usually the same regardless of the provider fetch configuration. Global table data gets fetched
	// and doesn't produce PK conflict errors instead data is replaced
	Global bool

	// Serial is used to force a signature change, which forces new table creation and cascading removal of old table and relations
	Serial string
}

func (t Table) Column(name string) *Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return &c
		}
	}
	return nil
}

// TableCreationOptions allow modifying how table is created such as defining primary keys, indices, foreign keys and constraints.
type TableCreationOptions struct {
	// List of columns to set as primary keys. If this is empty, a random unique ID is generated.
	PrimaryKeys []string
}

func (tco TableCreationOptions) signature() string {
	return strings.Join(tco.PrimaryKeys, ";")
}

// Signature returns a comparable string about the structure of the table (columns, options, relations)
func (t Table) Signature() string {
	const sdkSignatureSerial = "" // Change this to force a change across all providers

	sigs := make([]string, 1, len(t.Relations))
	sigs[0] = strings.Join([]string{
		"t:" + sdkSignatureSerial,
		t.Serial,
		t.Name,
		t.Columns.signature(),
		t.Options.signature(),
	}, ",")

	relNames := make([]string, len(t.Relations))
	relVsTable := make(map[string]*Table, len(t.Relations))
	for i := range t.Relations {
		relNames[i] = t.Relations[i].Name
		relVsTable[t.Relations[i].Name] = t.Relations[i]
	}
	sort.Strings(relNames)

	for _, t := range relNames {
		sigs = append(sigs, relVsTable[t].Signature())
	}

	return strings.Join(sigs, "\n")
}
