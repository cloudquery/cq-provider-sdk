package spec

type DestinationSpec struct {
	Name    string      `yaml:"name"`
	Version string      `yaml:"version"`
	Spec    interface{} `yaml:"-"`
}
