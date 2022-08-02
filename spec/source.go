package spec

import "gopkg.in/yaml.v3"

// SourceSpec is the shared configuration for all source plugins
type SourceSpec struct {
	Name          string    `json:"name" yaml:"name"`
	NoSpawn       bool      `json:"no_spawn" yaml:"no_spawn"`
	Version       string    `json:"version" yaml:"version"`
	MaxGoRoutines uint64    `json:"max_goroutines" yaml:"max_goroutines"`
	Tables        []string  `json:"tables" yaml:"tables"`
	SkipTables    []string  `json:"skip_tables" yaml:"skip_tables"`
	Configuration yaml.Node `json:"configuration" yaml:"configuration"`
}
