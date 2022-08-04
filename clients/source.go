// package clients is a wrapper around grpc clients so clients can work
// with non protobuf structs and handle unmarshaling
package clients

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"text/template"

	"github.com/cloudquery/cq-provider-sdk/internal/pb"
	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
)

type SourceClient struct {
	pbClient pb.SourceClient
}

const sourcePluginExampleConfigTemplate = `kind: source
spec:
  name: {{.Name}}
  version: {{.Version}}
  configuration:
  {{.PluginExampleConfig | indent 4}}
`

func NewSourceClient(cc grpc.ClientConnInterface) *SourceClient {
	return &SourceClient{
		pbClient: pb.NewSourceClient(cc),
	}
}

func (c *SourceClient) GetTables(ctx context.Context) ([]*schema.Table, error) {
	res, err := c.pbClient.GetTables(ctx, &pb.GetTables_Request{})
	if err != nil {
		return nil, err
	}
	var tables []*schema.Table
	if err := msgpack.Unmarshal(res.Tables, &tables); err != nil {
		return nil, err
	}
	return tables, nil
}

func (c *SourceClient) GetExampleConfig(ctx context.Context) ([]byte, error) {
	res, err := c.pbClient.GetExampleConfig(ctx, &pb.GetExampleConfig_Request{})
	if err != nil {
		return nil, fmt.Errorf("failed to get example config: %w", err)
	}
	t, err := template.New("source_plugin").Funcs(templateFuncMap()).Parse(sourcePluginExampleConfigTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}
	var tpl bytes.Buffer
	if err := t.Execute(&tpl, map[string]interface{}{
		"Name":                res.Name,
		"Version":             res.Version,
		"PluginExampleConfig": string(res.Config),
	}); err != nil {
		return nil, fmt.Errorf("failed to generate example config: %w", err)
	}
	return tpl.Bytes(), nil
}

func (c *SourceClient) Fetch(ctx context.Context, spec spec.SourceSpec, res chan<- []byte) error {
	stream, err := c.pbClient.Fetch(ctx, &pb.Fetch_Request{
		Config: []byte{},
	})
	if err != nil {
		return fmt.Errorf("failed to fetch resources: %w", err)
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to fetch resources from stream: %w", err)
		}
		res <- r.Resources
	}
	return nil
}
