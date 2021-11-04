//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/plugin.proto
package cqproto

import (
	"context"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
)

type CQProvider interface {
	// GetProviderSchema is called when CloudQuery needs to know what the
	// provider's tables and version
	GetProviderSchema(context.Context, *GetProviderSchemaRequest) (*GetProviderSchemaResponse, error)

	// GetProviderConfig is called when CloudQuery wants to generate a configuration example for a provider
	GetProviderConfig(context.Context, *GetProviderConfigRequest) (*GetProviderConfigResponse, error)

	// ConfigureProvider is called to pass the user-specified provider
	// configuration to the provider.
	ConfigureProvider(context.Context, *ConfigureProviderRequest) (*ConfigureProviderResponse, error)

	// FetchResources is called when CloudQuery requests to fetch one or more resources from the provider.
	// The provider reports back status updates on the resources fetching progress.
	FetchResources(context.Context, *FetchResourcesRequest) (FetchResourcesStream, error)
}

type CQProviderServer interface {
	// GetProviderSchema is called when CloudQuery needs to know what the
	// provider's tables and version
	GetProviderSchema(context.Context, *GetProviderSchemaRequest) (*GetProviderSchemaResponse, error)

	// GetProviderConfig is called when CloudQuery wants to generate a configuration example for a provider
	GetProviderConfig(context.Context, *GetProviderConfigRequest) (*GetProviderConfigResponse, error)

	// ConfigureProvider is called to pass the user-specified provider
	// configuration to the provider.
	ConfigureProvider(context.Context, *ConfigureProviderRequest) (*ConfigureProviderResponse, error)

	// FetchResources is called when CloudQuery requests to fetch one or more resources from the provider.
	// The provider reports back status updates on the resources fetching progress.
	FetchResources(context.Context, *FetchResourcesRequest, FetchResourcesSender) error
}

// GetProviderSchemaRequest represents a CloudQuery RPC request for provider's schemas
type GetProviderSchemaRequest struct{}

type GetProviderSchemaResponse struct {
	// Name is the name of the provider being executed
	Name string
	// Version is the current version provider being executed
	Version string
	// ResourceTables is a map of tables this provider creates
	ResourceTables map[string]*schema.Table
	// Migrations scripts available for the provider
	Migrations map[string][]byte
}

// GetProviderConfigRequest represents a CloudQuery RPC request for provider's config
type GetProviderConfigRequest struct{}

type GetProviderConfigResponse struct {
	Config []byte
}

type ConfigureProviderRequest struct {
	// CloudQueryVersion is the version of CloudQuery executing the request.
	CloudQueryVersion string
	// ConnectionDetails holds information regarding connection to the CloudQuery database
	Connection ConnectionDetails
	// Config is the configuration the user supplied for the provider
	Config []byte
	// DisableDelete configures providers to skip deletion of data before resource fetch
	DisableDelete bool
	// Fields to inject to every resource on insert
	ExtraFields map[string]interface{}
}

type ConfigureProviderResponse struct {
	// Error should be set to a string describing the error.
	// The error can be either from malformed configuration or failure to setup
	Error string
}

// FetchResourcesRequest represents a CloudQuery RPC request of one or more resources
type FetchResourcesRequest struct {
	// List of resources to fetch
	Resources []string
	// PartialFetchingEnabled if true enables partial fetching
	PartialFetchingEnabled bool
	// ParallelFetchingLimit limits parallel resources fetch at a time is more than 0
	ParallelFetchingLimit uint64
}

// FetchResourcesStream represents a CloudQuery RPC stream of fetch updates from the provider
type FetchResourcesStream interface {
	Recv() (*FetchResourcesResponse, error)
}

// FetchResourcesSender represents a CloudQuery RPC stream of fetch updates from the provider
type FetchResourcesSender interface {
	Send(*FetchResourcesResponse) error
}

// FetchResourcesResponse represents a CloudQuery RPC response of the current fetch progress of the provider
type FetchResourcesResponse struct {
	ResourceName string
	// map of resources that have finished fetching
	FinishedResources map[string]bool
	// Amount of resources collected so far
	ResourceCount uint64
	// Error value if any, if returned the stream will be canceled
	Error string
	// list of resources where the fetching failed
	PartialFetchFailedResources []*FailedResourceFetch
	// fetch summary of resource that finished execution
	Summary ResourceFetchSummary
}

// ResourceFetchStatus defines execution status of the resource fetch execution
type ResourceFetchStatus int

const (
	// ResourceFetchComplete execution was completed successfully without any errors/diagnostics
	ResourceFetchComplete ResourceFetchStatus = iota
	// ResourceFetchFailed execution failed and wasn't able to fetch any resource
	ResourceFetchFailed
	// ResourceFetchPartial execution was partial, one or more resources failed to resolve/fetch
	ResourceFetchPartial
	// ResourceFetchCanceled execution was canceled preemptively
	ResourceFetchCanceled
)

// ResourceFetchSummary includes a summarized report of a fetched resource, such as total amount of resources collected,
// status of the fetch and any diagnostics found while executing fetch on it.
type ResourceFetchSummary struct {
	// Execution status of resource
	Status ResourceFetchStatus
	// Total Amount of resources collected by this resource
	ResourceCount uint64
	// Diagnostics of failed resource fetch, the diagnostic provides insights such as severity, summary and
	// details on how to solve this issue
	Diagnostics diag.Diagnostics
}

type FailedResourceFetch struct {
	// table name of the failed resource fetch
	TableName string
	// root/parent table name
	RootTableName string
	// root/parent primary key values
	RootPrimaryKeyValues []string
	// error message for this resource fetch failure
	Error string
}

type ConnectionDetails struct {
	Type string
	DSN  string
}

type ProviderDiagnostic struct {
	ResourceName       string
	DiagnosticType     diag.DiagnosticType
	DiagnosticSeverity diag.Severity
	Summary            string
	Details            string
}

func (p ProviderDiagnostic) Severity() diag.Severity {
	return p.DiagnosticSeverity
}

func (p ProviderDiagnostic) Type() diag.DiagnosticType {
	return p.DiagnosticType
}

func (p ProviderDiagnostic) Description() diag.Description {
	return diag.Description{
		Resource: p.ResourceName,
		Summary:  p.Summary,
		Detail:   p.Details,
	}
}
