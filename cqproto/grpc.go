package cqproto

import (
	"context"
	"time"

	"github.com/cloudquery/cq-provider-sdk/cqproto/internal"
	"github.com/cloudquery/cq-provider-sdk/provider/diag"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-plugin"
	"github.com/vmihailenco/msgpack/v5"
)

type GRPCClient struct {
	broker *plugin.GRPCBroker
	client internal.ProviderClient
}

type GRPCServer struct {
	// This is the real implementation
	Impl CQProviderServer
	internal.UnimplementedProviderServer
}

type GRPCFetchResponseStream struct {
	stream internal.Provider_FetchResourcesClient
}

type GRPCFetchResourcesServer struct {
	server internal.Provider_FetchResourcesServer
}

func (g GRPCClient) GetProviderSchema(ctx context.Context, _ *GetProviderSchemaRequest) (*GetProviderSchemaResponse, error) {
	res, err := g.client.GetProviderSchema(ctx, &internal.GetProviderSchema_Request{})
	if err != nil {
		return nil, err
	}
	resp := &GetProviderSchemaResponse{
		Name:           res.GetName(),
		Version:        res.GetVersion(),
		ResourceTables: tablesFromProto(res.GetResourceTables()),
	}

	return resp, nil
}

func (g GRPCClient) GetProviderConfig(ctx context.Context, request *GetProviderConfigRequest) (*GetProviderConfigResponse, error) {
	res, err := g.client.GetProviderConfig(ctx, &internal.GetProviderConfig_Request{})
	if err != nil {
		return nil, err
	}

	return &GetProviderConfigResponse{
		Config: res.GetConfig(),
	}, nil
}

func (g GRPCClient) ConfigureProvider(ctx context.Context, request *ConfigureProviderRequest) (*ConfigureProviderResponse, error) {
	res, err := g.client.ConfigureProvider(ctx, &internal.ConfigureProvider_Request{
		CloudqueryVersion: request.CloudQueryVersion,
		Connection: &internal.ConnectionDetails{
			Type: internal.ConnectionType_POSTGRES,
			Dsn:  request.Connection.DSN,
		},
		Config: request.Config,
	})
	if err != nil {
		return nil, err
	}
	return &ConfigureProviderResponse{
		Error: res.Error,
	}, nil
}

func (g GRPCClient) FetchResources(ctx context.Context, request *FetchResourcesRequest) (FetchResourcesStream, error) {
	md, err := msgpack.Marshal(request.Metadata)
	if err != nil {
		return nil, err
	}

	res, err := g.client.FetchResources(ctx, &internal.FetchResources_Request{
		Resources:             request.Resources,
		ParallelFetchingLimit: request.ParallelFetchingLimit,
		MaxGoroutines:         request.MaxGoroutines,
		Timeout:               int64(request.Timeout.Seconds()),
		Metadata:              md,
	})
	if err != nil {
		return nil, err
	}
	return &GRPCFetchResponseStream{res}, nil
}

func (g GRPCFetchResponseStream) Recv() (*FetchResourcesResponse, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, err
	}
	fr := &FetchResourcesResponse{
		ResourceName:      resp.GetResource(),
		FinishedResources: resp.GetFinishedResources(),
		ResourceCount:     resp.GetResourceCount(),
		Error:             resp.GetError(),
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

func (g *GRPCServer) GetProviderSchema(ctx context.Context, _ *internal.GetProviderSchema_Request) (*internal.GetProviderSchema_Response, error) {
	resp, err := g.Impl.GetProviderSchema(ctx, &GetProviderSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return &internal.GetProviderSchema_Response{
		Name:           resp.Name,
		Version:        resp.Version,
		ResourceTables: tablesToProto(resp.ResourceTables),
	}, nil
}

func (g *GRPCServer) GetProviderConfig(ctx context.Context, request *internal.GetProviderConfig_Request) (*internal.GetProviderConfig_Response, error) {
	resp, err := g.Impl.GetProviderConfig(ctx, &GetProviderConfigRequest{})
	if err != nil {
		return nil, err
	}

	return &internal.GetProviderConfig_Response{
		Config: resp.Config,
	}, nil
}

func (g *GRPCServer) ConfigureProvider(ctx context.Context, request *internal.ConfigureProvider_Request) (*internal.ConfigureProvider_Response, error) {
	resp, err := g.Impl.ConfigureProvider(ctx, &ConfigureProviderRequest{
		CloudQueryVersion: request.GetCloudqueryVersion(),
		Connection: ConnectionDetails{
			Type: string(request.Connection.GetType()),
			DSN:  request.Connection.GetDsn(),
		},
		Config: request.Config,
	})
	if err != nil {
		return nil, err
	}
	return &internal.ConfigureProvider_Response{
		Error: resp.Error, // For backwards compatibility
	}, nil
}

func (g *GRPCServer) FetchResources(request *internal.FetchResources_Request, server internal.Provider_FetchResourcesServer) error {
	var md map[string]interface{}
	if mdVal := request.GetMetadata(); mdVal != nil {
		md = make(map[string]interface{})
		if err := msgpack.Unmarshal(mdVal, &md); err != nil {
			return err
		}
	}

	return g.Impl.FetchResources(
		server.Context(),
		&FetchResourcesRequest{
			Resources:             request.GetResources(),
			ParallelFetchingLimit: request.ParallelFetchingLimit,
			MaxGoroutines:         request.MaxGoroutines,
			Metadata:              md,
			Timeout:               time.Duration(request.GetTimeout()) * time.Second,
		},
		&GRPCFetchResourcesServer{server: server},
	)
}

func (g GRPCFetchResourcesServer) Send(response *FetchResourcesResponse) error {
	return g.server.Send(&internal.FetchResources_Response{
		Resource:          response.ResourceName,
		FinishedResources: response.FinishedResources,
		ResourceCount:     response.ResourceCount,
		Error:             response.Error,
		Summary: &internal.ResourceFetchSummary{
			Status:        internal.ResourceFetchSummary_Status(response.Summary.Status),
			ResourceCount: response.Summary.ResourceCount,
			Diagnostics:   diagnosticsToProto(response.Summary.Diagnostics),
		},
	})
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
		cols[i] = schema.SetColumnMeta(schema.Column{
			Name:        c.GetName(),
			Type:        schema.ValueType(c.GetType()),
			Description: c.GetDescription(),
		}, metaFromProto(c.GetMeta()))
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
		Serial:      v.GetSerial(),
	}
}

func metaFromProto(m *internal.ColumnMeta) *schema.ColumnMeta {
	if m == nil {
		return nil
	}
	var r *schema.ResolverMeta
	if m.GetResolver() != nil {
		r = &schema.ResolverMeta{
			Name:    m.Resolver.Name,
			Builtin: m.Resolver.Builtin,
		}
	}
	return &schema.ColumnMeta{
		Resolver:     r,
		IgnoreExists: m.GetIgnoreExists(),
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
			Meta:        columnMetaToProto(c.Meta()),
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
		Serial: in.Serial,
	}
}

func columnMetaToProto(m *schema.ColumnMeta) *internal.ColumnMeta {
	if m == nil {
		return nil
	}
	var r *internal.ResolverMeta
	if m.Resolver != nil {
		r = &internal.ResolverMeta{Name: m.Resolver.Name, Builtin: m.Resolver.Builtin}
	}
	return &internal.ColumnMeta{
		Resolver:     r,
		IgnoreExists: m.IgnoreExists,
	}
}

func diagnosticsToProto(in diag.Diagnostics) []*internal.Diagnostic {
	if len(in) == 0 {
		return nil
	}
	diagnostics := make([]*internal.Diagnostic, len(in))
	for i, p := range in {
		diagnostics[i] = &internal.Diagnostic{
			Type:       internal.Diagnostic_Type(p.Type()),
			Severity:   internal.Diagnostic_Severity(p.Severity()),
			Summary:    p.Description().Summary,
			Detail:     p.Description().Detail,
			Resource:   p.Description().Resource,
			ResourceId: p.Description().ResourceID,
		}
		if rd, ok := p.(diag.Redactable); ok {
			if r := rd.Redacted(); r != nil {
				diagnostics[i].Redacted = &internal.Diagnostic{
					Type:       internal.Diagnostic_Type(r.Type()),
					Severity:   internal.Diagnostic_Severity(r.Severity()),
					Summary:    r.Description().Summary,
					Detail:     r.Description().Detail,
					Resource:   r.Description().Resource,
					ResourceId: r.Description().ResourceID,
				}
			}
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
		pdiag := &ProviderDiagnostic{
			ResourceName:       resourceName,
			ResourceId:         p.GetResourceId(),
			DiagnosticType:     diag.Type(p.GetType()),
			DiagnosticSeverity: diag.Severity(p.GetSeverity()),
			Summary:            p.GetSummary(),
			Details:            p.GetDetail(),
		}
		if r := p.GetRedacted(); r != nil {
			diagnostics[i] = diag.NewRedactedDiagnostic(pdiag, &ProviderDiagnostic{
				ResourceName:       resourceName,
				ResourceId:         r.GetResourceId(),
				DiagnosticType:     diag.Type(r.GetType()),
				DiagnosticSeverity: diag.Severity(r.GetSeverity()),
				Summary:            r.GetSummary(),
				Details:            r.GetDetail(),
			})
			continue
		}

		diagnostics[i] = pdiag
	}
	return diagnostics
}
