package spec

import "gopkg.in/yaml.v3"

// SourceSpec is the shared configuration for all source plugins
type SourceSpec struct {
	Name    string `json:"name" yaml:"name"`
	Version string `json:"version" yaml:"version"`
	// Path is the path in the registry
	Path string `json:"path" yaml:"path"`
	// Registry can be hub,https,local,grpc
	Registry      string    `json:"registry" yaml:"registry"`
	MaxGoRoutines uint64    `json:"max_goroutines" yaml:"max_goroutines"`
	Tables        []string  `json:"tables" yaml:"tables"`
	SkipTables    []string  `json:"skip_tables" yaml:"skip_tables"`
	Configuration yaml.Node `json:"configuration" yaml:"configuration"`
}
