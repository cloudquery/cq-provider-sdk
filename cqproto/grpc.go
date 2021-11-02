package cqproto

import (
	"context"
	"reflect"
	"strings"

	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/cloudquery/cq-provider-sdk/cqproto/internal"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-plugin"
)

type GRPCClient struct {
	broker *plugin.GRPCBroker
	client internal.ProviderClient
}

func (g GRPCClient) GetProviderSchema(ctx context.Context, _ *GetProviderSchemaRequest) (*GetProviderSchemaResponse, error) {
	res, err := g.client.GetProviderSchema(ctx, &internal.GetProviderSchema_Request{})
	if err != nil {
		return nil, err
	}
	resp := &GetProviderSchemaResponse{
		Name:               res.GetName(),
		Version:            res.GetVersion(),
		ResourceTables:     tablesFromProto(res.GetResourceTables()),
		Migrations:         res.Migrations,
		ResourceTablesMeta: metaTablesFromProto(res.GetResourceTableMetadata()),
	}

	return resp, nil
}

func (g GRPCClient) GetProviderConfig(ctx context.Context, _ *GetProviderConfigRequest) (*GetProviderConfigResponse, error) {
	res, err := g.client.GetProviderConfig(ctx, &internal.GetProviderConfig_Request{})
	if err != nil {
		return nil, err
	}
	return &GetProviderConfigResponse{
		Config: res.GetConfig(),
	}, nil
}

func (g GRPCClient) ConfigureProvider(ctx context.Context, request *ConfigureProviderRequest) (*ConfigureProviderResponse, error) {
	fieldsData, err := msgpack.Marshal(request.ExtraFields)
	if err != nil {
		return nil, err
	}
	res, err := g.client.ConfigureProvider(ctx, &internal.ConfigureProvider_Request{
		CloudqueryVersion: request.CloudQueryVersion,
		Connection: &internal.ConnectionDetails{
			Type: internal.ConnectionType_POSTGRES,
			Dsn:  request.Connection.DSN,
		},
		Config:        request.Config,
		DisableDelete: request.DisableDelete,
		ExtraFields:   fieldsData,
	})
	if err != nil {
		return nil, err
	}
	return &ConfigureProviderResponse{res.GetError()}, nil
}

func (g GRPCClient) FetchResources(ctx context.Context, request *FetchResourcesRequest) (FetchResourcesStream, error) {
	res, err := g.client.FetchResources(ctx, &internal.FetchResources_Request{
		Resources:              request.Resources,
		PartialFetchingEnabled: request.PartialFetchingEnabled,
		ParallelFetchingLimit:  request.ParallelFetchingLimit,
	})
	if err != nil {
		return nil, err
	}
	return &GRPCFetchResponseStream{res}, nil
}

type GRPCFetchResponseStream struct {
	stream internal.Provider_FetchResourcesClient
}

func (g GRPCFetchResponseStream) Recv() (*FetchResourcesResponse, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, err
	}
	fr := &FetchResourcesResponse{
		ResourceName:                resp.GetResource(),
		FinishedResources:           resp.GetFinishedResources(),
		ResourceCount:               resp.GetResourceCount(),
		Error:                       resp.GetError(),
		PartialFetchFailedResources: partialFetchFailedResourcesFromProto(resp.GetPartialFetchFailedResources()),
	}
	if resp.GetSummary() != nil {
		fr.Summary = ResourceFetchSummary{
			Status:        ResourceFetchStatus(resp.Summary.Status),
			ResourceCount: resp.GetSummary().GetResourceCount(),
			Diagnostics:   diagnosticsFromProto(resp.GetResource(), resp.GetSummary().Diagnostics),
		}
	}
	return fr, nil
}

type GRPCServer struct {
	// This is the real implementation
	Impl CQProviderServer
	internal.UnimplementedProviderServer
}

func (g *GRPCServer) GetProviderSchema(ctx context.Context, _ *internal.GetProviderSchema_Request) (*internal.GetProviderSchema_Response, error) {
	resp, err := g.Impl.GetProviderSchema(ctx, &GetProviderSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return &internal.GetProviderSchema_Response{
		Name:                  resp.Name,
		Version:               resp.Version,
		ResourceTables:        tablesToProto(resp.ResourceTables),
		Migrations:            resp.Migrations,
		ResourceTableMetadata: tablesToTableMetaProto(resp.ResourceTables),
	}, nil

}

func (g *GRPCServer) GetProviderConfig(ctx context.Context, _ *internal.GetProviderConfig_Request) (*internal.GetProviderConfig_Response, error) {
	resp, err := g.Impl.GetProviderConfig(ctx, &GetProviderConfigRequest{})
	if err != nil {
		return nil, err
	}
	return &internal.GetProviderConfig_Response{Config: resp.Config}, nil
}

func (g *GRPCServer) ConfigureProvider(ctx context.Context, request *internal.ConfigureProvider_Request) (*internal.ConfigureProvider_Response, error) {

	var eFields = make(map[string]interface{})
	if request.GetExtraFields() != nil {
		if err := msgpack.Unmarshal(request.GetExtraFields(), &eFields); err != nil {
			return nil, err
		}
	}
	resp, err := g.Impl.ConfigureProvider(ctx, &ConfigureProviderRequest{
		CloudQueryVersion: request.GetCloudqueryVersion(),
		Connection: ConnectionDetails{
			Type: string(request.Connection.GetType()),
			DSN:  request.Connection.GetDsn(),
		},
		Config:        request.Config,
		DisableDelete: request.DisableDelete,
		ExtraFields:   eFields,
	})
	if err != nil {
		return nil, err
	}
	return &internal.ConfigureProvider_Response{Error: resp.Error}, nil

}

func (g *GRPCServer) FetchResources(request *internal.FetchResources_Request, server internal.Provider_FetchResourcesServer) error {
	return g.Impl.FetchResources(
		server.Context(),
		&FetchResourcesRequest{Resources: request.GetResources(), PartialFetchingEnabled: request.PartialFetchingEnabled, ParallelFetchingLimit: request.ParallelFetchingLimit},
		&GRPCFetchResourcesServer{server: server},
	)
}

type GRPCFetchResourcesServer struct {
	server internal.Provider_FetchResourcesServer
}

func (g GRPCFetchResourcesServer) Send(response *FetchResourcesResponse) error {
	return g.server.Send(&internal.FetchResources_Response{
		Resource:                    response.ResourceName,
		FinishedResources:           response.FinishedResources,
		ResourceCount:               response.ResourceCount,
		Error:                       response.Error,
		PartialFetchFailedResources: partialFetchFailedResourcesToProto(response.PartialFetchFailedResources),
		Summary: &internal.ResourceFetchSummary{
			Status:        internal.ResourceFetchSummary_Status(response.Summary.Status),
			ResourceCount: response.Summary.ResourceCount,
			Diagnostics:   diagnosticsToProto(response.Summary.Diagnostics),
		},
	})
}

func metaTablesFromProto(in map[string]*internal.TableMeta) map[string]*TableMeta {
	if in == nil {
		return nil
	}
	out := make(map[string]*TableMeta, len(in))
	for k, v := range in {
		out[k] = metaTableFromProto(v)
	}
	return out
}

func metaTableFromProto(in *internal.TableMeta) *TableMeta {

	cols := make([]ColumnMeta, len(in.Columns))
	for i, c := range in.Columns {

		var resolver *ResolverMeta
		if c.GetResolver() != nil {
			resolver = &ResolverMeta{
				Name:    c.Resolver.Name,
				Builtin: c.Resolver.Builtin,
			}
		}
		cols[i] = ColumnMeta{
			Resolver:     resolver,
			IgnoreExists: c.IgnoreExists,
		}
	}

	rels := make([]*TableMeta, len(in.Relations))
	for i, r := range in.Relations {
		rels[i] = metaTableFromProto(r)
	}
	var resolver *ResolverMeta
	if in.GetResolver() != nil {
		resolver = &ResolverMeta{
			Name:    in.Resolver.Name,
			Builtin: in.Resolver.Builtin,
		}
	}
	return &TableMeta{
		Resolver:           resolver,
		IgnoreExists:       in.IgnoreExists,
		MultiplexExists:    in.MultiplexExists,
		PostResolverExists: in.PostResolverExists,
		Relations:          rels,
		Columns:            cols,
	}
}

func tablesFromProto(in map[string]*internal.Table) map[string]*schema.Table {
	if in == nil {
		return nil
	}
	out := make(map[string]*schema.Table, len(in))
	for k, v := range in {
		out[k] = tableFromProto(v)
	}
	return out
}

func tableFromProto(v *internal.Table) *schema.Table {
	cols := make([]schema.Column, len(v.GetColumns()))
	for i, c := range v.GetColumns() {
		cols[i] = schema.Column{
			Name:        c.GetName(),
			Type:        schema.ValueType(c.GetType()),
			Description: c.GetDescription(),
		}
	}
	rels := make([]*schema.Table, len(v.GetRelations()))
	for i, r := range v.GetRelations() {
		rels[i] = tableFromProto(r)
	}

	var opts schema.TableCreationOptions
	if o := v.GetOptions(); o != nil {
		opts.PrimaryKeys = o.GetPrimaryKeys()
	}

	return &schema.Table{
		Name:        v.GetName(),
		Description: v.GetDescription(),
		Columns:     cols,
		Relations:   rels,
		Options:     opts,
	}
}

func tablesToProto(in map[string]*schema.Table) map[string]*internal.Table {
	if in == nil {
		return nil
	}
	out := make(map[string]*internal.Table, len(in))
	for k, v := range in {
		out[k] = tableToProto(v)
	}
	return out
}

func tableToProto(in *schema.Table) *internal.Table {
	cols := make([]*internal.Column, len(in.Columns))
	for i, c := range in.Columns {
		cols[i] = &internal.Column{
			Name:        c.Name,
			Type:        internal.ColumnType(c.Type),
			Description: c.Description,
		}
	}
	rels := make([]*internal.Table, len(in.Relations))
	for i, r := range in.Relations {
		rels[i] = tableToProto(r)
	}
	return &internal.Table{
		Name:        in.Name,
		Description: in.Description,
		Columns:     cols,
		Relations:   rels,
		Options: &internal.TableCreationOptions{
			PrimaryKeys: in.Options.PrimaryKeys,
		},
	}
}

func partialFetchFailedResourcesFromProto(in []*internal.PartialFetchFailedResource) []*FailedResourceFetch {
	if len(in) == 0 {
		return nil
	}
	failedResources := make([]*FailedResourceFetch, len(in))
	for i, p := range in {
		failedResources[i] = &FailedResourceFetch{
			TableName:            p.TableName,
			RootTableName:        p.RootTableName,
			RootPrimaryKeyValues: p.RootPrimaryKeyValues,
			Error:                p.Error,
		}
	}
	return failedResources
}

func partialFetchFailedResourcesToProto(in []*FailedResourceFetch) []*internal.PartialFetchFailedResource {
	if len(in) == 0 {
		return nil
	}
	failedResources := make([]*internal.PartialFetchFailedResource, len(in))
	for i, p := range in {
		failedResources[i] = &internal.PartialFetchFailedResource{
			TableName:            p.TableName,
			RootTableName:        p.RootTableName,
			RootPrimaryKeyValues: p.RootPrimaryKeyValues,
			Error:                p.Error,
		}
	}
	return failedResources
}

func diagnosticsToProto(in diag.Diagnostics) []*internal.Diagnostic {
	if len(in) == 0 {
		return nil
	}
	diagnostics := make([]*internal.Diagnostic, len(in))
	for i, p := range in {
		diagnostics[i] = &internal.Diagnostic{
			Type:     internal.Diagnostic_Type(p.Type()),
			Severity: internal.Diagnostic_Severity(p.Severity()),
			Summary:  p.Description().Summary,
			Detail:   p.Description().Detail,
			Resource: p.Description().Resource,
		}
	}
	return diagnostics
}

func diagnosticsFromProto(resourceName string, in []*internal.Diagnostic) diag.Diagnostics {
	if len(in) == 0 {
		return nil
	}
	diagnostics := make(diag.Diagnostics, len(in))
	for i, p := range in {
		diagnostics[i] = &ProviderDiagnostic{
			ResourceName:       resourceName,
			DiagnosticType:     diag.DiagnosticType(p.GetType()),
			DiagnosticSeverity: diag.Severity(p.GetSeverity()),
			Summary:            p.GetSummary(),
			Details:            p.GetDetail(),
		}
	}
	return diagnostics
}

// PartialFetchToCQProto converts schema partial fetch failed resources to cq-proto partial fetch resources
func PartialFetchToCQProto(in []schema.ResourceFetchError) []*FailedResourceFetch {
	if len(in) == 0 {
		return nil
	}
	failedResources := make([]*FailedResourceFetch, len(in))
	for i, p := range in {
		failedResources[i] = &FailedResourceFetch{
			TableName:            p.TableName,
			RootTableName:        p.RootTableName,
			RootPrimaryKeyValues: p.RootPrimaryKeyValues,
			Error:                p.Err.Error(),
		}
	}
	return failedResources
}

func tablesToTableMetaProto(tables map[string]*schema.Table) map[string]*internal.TableMeta {
	protoTables := make(map[string]*internal.TableMeta, len(tables))
	for n, t := range tables {
		protoTables[n] = extractTableMeta(t)
	}
	return protoTables
}

func extractTableMeta(t *schema.Table) *internal.TableMeta {
	cols := make([]*internal.ColumnMeta, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = &internal.ColumnMeta{
			Resolver:     getResolverMeta(c.Resolver),
			IgnoreExists: c.IgnoreError != nil,
		}
	}
	relMetas := make([]*internal.TableMeta, len(t.Relations))
	for i, rel := range t.Relations {
		relMetas[i] = extractTableMeta(rel)
	}

	return &internal.TableMeta{
		Resolver:           getResolverMeta(t.Resolver),
		IgnoreExists:       t.IgnoreError != nil,
		MultiplexExists:    t.Multiplex != nil,
		PostResolverExists: t.PostResourceResolver != nil,
		Columns:            cols,
	}
}

func getResolverMeta(f interface{}) *internal.ResolverMeta {
	if f == nil {
		return nil
	}
	typ := reflect.TypeOf(f)
	return &internal.ResolverMeta{
		Name:    typ.Name(),
		Builtin: strings.HasPrefix(typ.PkgPath(), "github.com/cloudquery/cq-provider-sdk"),
	}
}
