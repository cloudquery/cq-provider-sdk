package schema

const (
	// MaxTableLength in postgres is 63 when building _fk or _pk we want to truncate the name to 60 chars max
	maxTableNamePKConstraint = 60
)

func TruncateTableConstraint(name string) string {
	if len(name) > maxTableNamePKConstraint {
		return name[:maxTableNamePKConstraint]
	}
	return name
}
