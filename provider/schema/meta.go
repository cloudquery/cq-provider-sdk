package schema

var (
	cqIdColumn = Column{
		Name:        "cq_id",
		Type:        TypeUUID,
		Description: "Unique CloudQuery Id added to every resource",
	}
)

// GetDefaultSDKColumns Default columns of the SDK, these columns are added to each table by default
func GetDefaultSDKColumns() []Column {
	return []Column{cqIdColumn}
}
