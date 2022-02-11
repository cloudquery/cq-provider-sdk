package module

import "github.com/hashicorp/go-hclog"

// InfoReader is called when the user executes a module, to get provider supported metadata about the given module
type InfoReader func(hclog.Logger, string) (map[string][]byte, error)
