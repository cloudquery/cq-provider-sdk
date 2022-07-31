package source

import (
	"context"
	"fmt"

	"github.com/cloudquery/cq-provider-sdk/plugin/source/pb"
	"github.com/cloudquery/cq-provider-sdk/plugin/source/schema"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SourceServer struct {
	pb.UnimplementedSourceServer
	Plugin *SourcePlugin
}

func (s *SourceServer) GetTables(context.Context, *pb.GetTables_Request) (*pb.GetTables_Response, error) {
	b, err := msgpack.Marshal(s.Plugin.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tables: %w", err)
	}
	return &pb.GetTables_Response{
		Tables: b,
	}, nil
}

func (s *SourceServer) GetExampleConfig(context.Context, *pb.GetExampleConfig_Request) (*pb.GetExampleConfig_Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetExampleConfig not implemented")
}

func (s *SourceServer) Fetch(req *pb.Fetch_Request, stream pb.Source_FetchServer) error {
	resources := make(chan *schema.Resource)
	var fetchErr error
	var jsonSchemaResult *gojsonschema.Result
	go func() {
		var err error
		defer close(resources)
		if jsonSchemaResult, err = s.Plugin.Fetch(stream.Context(), req.Config, resources); err != nil {
			fetchErr = fmt.Errorf("failed to fetch resources: %w", err)
		}
	}()

	for resource := range resources {
		b, err := msgpack.Marshal(resource)
		if err != nil {
			return fmt.Errorf("failed to marshal resource: %w", err)
		}
		stream.Send(&pb.Fetch_Response{Resources: b})
	}
	if fetchErr != nil {
		return fetchErr
	}
	if jsonSchemaResult != nil {
		b, err := msgpack.Marshal(jsonSchemaResult)
		if err != nil {
			return fmt.Errorf("failed to marshal json schema result: %w", err)
		}
		stream.Send(&pb.Fetch_Response{
			JsonschemaResult: b,
		})
	}
	return nil
}
