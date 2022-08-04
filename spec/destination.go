package spec

type DestinationSpec struct {
	Name     string      `yaml:"name"`
	Version  string      `yaml:"version"`
	Path     string      `yaml:"path"`
	Registry string      `yaml:"registry"`
	Spec     interface{} `yaml:"-"`
}
