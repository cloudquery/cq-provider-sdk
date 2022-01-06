package schema

import (
	"context"
	"encoding/json"
	"time"
)

type Meta struct {
	LastUpdate time.Time `json:"last_updated"`
	FetchId    string    `json:"fetch_id,omitempty"`
}

var (
	cqMeta = Column{
		Name:        "meta", // TODO rename to cq_meta
		Type:        TypeJSON,
		Description: "Meta column holds fetch information",
		Resolver: func(ctx context.Context, meta ClientMeta, resource *Resource, c Column) error {
			mi := Meta{
				LastUpdate: time.Now().UTC(),
				FetchId:    "", // TODO
			}
			b, _ := json.Marshal(mi)
			return resource.Set(c.Name, b)
		},
	}
	cqIdColumn = Column{
		Name:        "cq_id",
		Type:        TypeUUID,
		Description: "Unique CloudQuery Id added to every resource",
		Resolver: func(ctx context.Context, meta ClientMeta, resource *Resource, c Column) error {
			if err := resource.GenerateCQId(); err != nil {
				if resource.Parent == nil {
					return err
				} else {
					meta.Logger().Debug("one of the table pk is nil", "table", resource.table.Name)
				}
			}
			return resource.Set(c.Name, resource.Id())
		},
		CreationOptions: ColumnCreationOptions{
			Unique: true,
		},
	}
)

// GetDefaultSDKColumns Default columns of the SDK, these columns are added to each table by default
func GetDefaultSDKColumns() []Column {
	return []Column{cqIdColumn, cqMeta}
}
