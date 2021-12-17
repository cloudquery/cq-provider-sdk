package provider

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/huandu/go-sqlbuilder"
)

// FetchSummary includes a summarized report of fetch, such as fetch id, fetch start and finish,
// resources fetch results
type FetchSummary struct {
	//  Unique Id of fetch session
	FetchId            uuid.UUID
	Start              time.Time
	Finish             time.Time
	TotalResourceCount uint64
	ProviderName       string
	ProviderVersion    string
	ProviderMeta       interface{}
	FetchedResources   []ResourceSummary
}

type ResourceSummary struct {
	ResourceName string `json:"resource_name"`
	// map of resources that have finished fetching
	FinishedResources map[string]bool `json:"finished_resources"`
	// Amount of resources collected so far
	// Error value if any, if returned the stream will be canceled
	Error string `json:"error"`
	// list of resources where the fetching failed
	PartialFetchFailedResources []*cqproto.FailedResourceFetch `json:"partial_fetch_failed_resources"`
	// Execution status of resource
	Status string `json:"status"`
	// Total Amount of resources collected by this resource
	ResourceCount uint64 `json:"resource_count"`
	// Diagnostics of failed resource fetch, the diagnostic provides insights such as severity, summary and
	// details on how to solve this issue
	Diagnostics diag.Diagnostics `json:"diagnostics"`
}

type FetchSummarizer struct {
	log  hclog.Logger
	conn schema.Database
	meta schema.ClientMeta
}

func NewFetchSummarizer(log hclog.Logger, conn schema.Database, meta schema.ClientMeta) FetchSummarizer {
	return FetchSummarizer{
		log:  log,
		conn: conn,
		meta: meta,
	}
}

func (f *FetchSummarizer) Save(ctx context.Context, fs FetchSummary) error {
	table := cqFetches()
	// todo replace code below with upgraded table creator that uses schema.Database
	ctb := sqlbuilder.CreateTable(table.Name).IfNotExists()
	for _, c := range schema.GetDefaultSDKColumns() {
		if c.CreationOptions.Unique {
			ctb.Define(c.Name, schema.GetPgTypeFromType(c.Type), "unique")
		} else {
			ctb.Define(c.Name, schema.GetPgTypeFromType(c.Type))
		}
	}
	for _, c := range table.Columns {
		defs := []string{strconv.Quote(c.Name), schema.GetPgTypeFromType(c.Type)}
		if c.CreationOptions.Unique {
			defs = []string{strconv.Quote(c.Name), schema.GetPgTypeFromType(c.Type), "unique"}
		}
		ctb.Define(defs...)
	}
	sql, _ := ctb.BuildWithFlavor(sqlbuilder.PostgreSQL)

	f.log.Debug("creating table if not exists", "table", table.Name)

	if err := f.conn.Exec(ctx, sql); err != nil {
		return err
	}

	execData := schema.NewExecutionData(f.conn, f.log, table, false, nil, false)
	if _, err := execData.ResolveTable(ctx, f.meta, &schema.Resource{Item: fs}); err != nil {
		return err
	}
	return nil
}

func cqFetches() *schema.Table {
	return &schema.Table{
		Name:        "cq_fetches",
		Description: "Stores fetch summary data",
		Resolver: func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan interface{}) error {
			res <- parent.Item
			return nil
		},
		Columns: []schema.Column{
			{
				Name: "fetch_id",
				Type: schema.TypeUUID,
			},
			{
				Name: "start",
				Type: schema.TypeTimestamp,
			},
			{
				Name: "finish",
				Type: schema.TypeTimestamp,
			},
			{
				Name: "total_resource_count",
				Type: schema.TypeBigInt,
			},
			{
				Name: "provider_name",
				Type: schema.TypeString,
			},
			{
				Name: "provider_version",
				Type: schema.TypeString,
			},
			{
				Name: "provider_meta",
				Type: schema.TypeJSON,
				Resolver: func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
					p := resource.Item.(FetchSummary)
					j, err := json.Marshal(p.ProviderMeta)
					if err != nil {
						return err
					}
					return resource.Set(c.Name, j)
				},
			},
			{
				Name: "fetch_results",
				Type: schema.TypeJSON,
				Resolver: func(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
					p := resource.Item.(FetchSummary)
					j, err := json.Marshal(p.FetchedResources)
					if err != nil {
						return err
					}
					return resource.Set(c.Name, j)
				},
			},
		},
	}
}
