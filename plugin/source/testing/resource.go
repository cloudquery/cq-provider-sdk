package testing

import (
	"context"
	"fmt"
	"testing"

	"github.com/cloudquery/cq-provider-sdk/plugin/source"
	"github.com/cloudquery/cq-provider-sdk/plugin/source/schema"
	"github.com/cloudquery/faker/v3"
	"github.com/georgysavva/scany/pgxscan"
)

type ResourceTestCase struct {
	Plugin *source.SourcePlugin
	Config string
	// we want it to be parallel by default
	NotParallel bool
	// ParallelFetchingLimit limits parallel resources fetch at a time
	ParallelFetchingLimit uint64
	// SkipIgnoreInTest flag which detects if schema.Table or schema.Column should be ignored
	SkipIgnoreInTest bool
	// Verifiers are map from resource name to its verifiers.
	// If no verifiers specified for resource (resource name is not in key set of map),
	// non emptiness check of all columns in table and its relations will be performed.
	Verifiers map[string][]Verifier
}

// Verifier verifies tables specified by table schema (main table and its relations).
type Verifier func(t *testing.T, table *schema.Table, conn pgxscan.Querier, shouldSkipIgnoreInTest bool)

type testResourceSender struct {
	Errors []string
}

func init() {
	_ = faker.SetRandomMapAndSliceMinSize(1)
	_ = faker.SetRandomMapAndSliceMaxSize(1)
}

func TestResource(t *testing.T, resource ResourceTestCase) {
	if !resource.NotParallel {
		t.Parallel()
	}
	t.Helper()

	// No need for configuration or db connection, get it out of the way first
	// testTableIdentifiersForProvider(t, resource.Provider)

	// l := testlog.New(t)
	// l.SetLevel(hclog.Info)
	// resource.Plugin.Logger = l
	resources := make(chan *schema.Resource)
	go func() {
		defer close(resources)
		resource.Plugin.Fetch(context.Background(), source.SourceConfig{}, resources)
	}()
	for resource := range resources {
		fmt.Println(resource)
		// resource.
		// resource.
		// resource.Env
	}
}

// func testResource(t *testing.T, resource ResourceTestCase, name string, table *schema.Table, conn execution.QueryExecer) error {
// 	t.Helper()

// 	if createErr := dropAndCreateTable(context.Background(), conn, table); createErr != nil {
// 		assert.FailNow(t, fmt.Sprintf("failed to create table %s", table.Name), createErr)
// 	}

// 	if !resource.SkipIgnoreInTest && table.IgnoreInTests {
// 		t.Logf("skipping fetch of resource: %s in tests", name)
// 	} else {
// 		if err := fetchResource(t, &resource, name); err != nil {
// 			return err
// 		}
// 	}

// 	if verifiers, ok := resource.Verifiers[name]; ok {
// 		for _, verifier := range verifiers {
// 			verifier(t, table, conn, resource.SkipIgnoreInTest)
// 		}
// 	} else {
// 		// fallback to default verification
// 		verifyNoEmptyColumns(t, table, conn, resource.SkipIgnoreInTest)
// 	}

// 	return nil
// }

// // fetchResource - fetches a resource from the cloud and puts them into database. database config can be specified via DATABASE_URL env variable
// func fetchResource(t *testing.T, resource *ResourceTestCase, resourceName string) error {
// 	t.Helper()

// 	t.Logf("fetch resource %v", resourceName)

// 	var resourceSender = &testResourceSender{
// 		Errors: []string{},
// 	}

// 	if err := resource.Provider.FetchResources(context.Background(),
// 		&cqproto.FetchResourcesRequest{
// 			Resources:             []string{resourceName},
// 			ParallelFetchingLimit: resource.ParallelFetchingLimit,
// 		},
// 		resourceSender,
// 	); err != nil {
// 		return err
// 	}

// 	if len(resourceSender.Errors) > 0 {
// 		return fmt.Errorf("error/s occurred during test, %s", strings.Join(resourceSender.Errors, ", "))
// 	}

// 	return nil
// }

// func verifyNoEmptyColumns(t *testing.T, table *schema.Table, conn pgxscan.Querier, shouldSkipIgnoreInTest bool) {
// 	t.Helper()
// 	t.Run(table.Name, func(t *testing.T) {
// 		t.Helper()

// 		if !shouldSkipIgnoreInTest && table.IgnoreInTests {
// 			t.Skipf("table %s marked as IgnoreInTest. Skipping...", table.Name)
// 		}
// 		s := sq.StatementBuilder.
// 			PlaceholderFormat(sq.Dollar).
// 			Select(fmt.Sprintf("json_agg(%s)", table.Name)).
// 			From(table.Name)
// 		query, args, err := s.ToSql()
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		var data []map[string]interface{}
// 		if err := pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
// 			t.Fatal(err)
// 		}

// 		if len(data) == 0 {
// 			t.Errorf("expected to have at least 1 entry at table %s got zero", table.Name)
// 			return
// 		}

// 		nilColumns := map[string]bool{}
// 		// mark all columns as nil
// 		for _, c := range table.Columns {
// 			if shouldSkipIgnoreInTest || !c.IgnoreInTests {
// 				nilColumns[c.Name] = true
// 			}
// 		}

// 		for _, row := range data {
// 			for c, v := range row {
// 				if v != nil {
// 					// as long as we had one row or result with this column not nil it means the resolver worked
// 					nilColumns[c] = false
// 				}
// 			}
// 		}

// 		var nilColumnsArr []string
// 		for c, v := range nilColumns {
// 			if v {
// 				nilColumnsArr = append(nilColumnsArr, c)
// 			}
// 		}

// 		if len(nilColumnsArr) != 0 {
// 			t.Errorf("found nil column in table %s. columns=%s", table.Name, strings.Join(nilColumnsArr, ","))
// 		}
// 		for _, childTable := range table.Relations {
// 			verifyNoEmptyColumns(t, childTable, conn, shouldSkipIgnoreInTest)
// 		}
// 	})
// }

// func (f *testResourceSender) Send(r *cqproto.FetchResourcesResponse) error {
// 	if r.Error != "" {
// 		fmt.Printf(r.Error)
// 		f.Errors = append(f.Errors, r.Error)
// 	}

// 	return nil
// }

// func getEnv(key, fallback string) string {
// 	if value, ok := os.LookupEnv(key); ok {
// 		return value
// 	}
// 	return fallback
// }
