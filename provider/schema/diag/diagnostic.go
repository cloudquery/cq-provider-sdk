package diag

type Severity int

const (
	IGNORE Severity = iota
	WARNING
	ERROR
)

type DiagnosticType int

func (d DiagnosticType) String() string {
	switch d {

	case RESOLVING:
		return "Resolving"
	case ACCESS:
		return "Access"
	case THROTTLE:
		return "Throttle"
	case DATABASE:
		return "Database"
	case Unknown:
		fallthrough
	default:
		return "Unknown"
	}
}

const (
	Unknown DiagnosticType = iota
	RESOLVING
	ACCESS
	THROTTLE
	DATABASE
)

type Diagnostic interface {
	Severity() Severity
	Type() DiagnosticType
	Description() Description
}

type Description struct {
	Summary string
	Detail  string
}

type Diagnostics []Diagnostic

func (dd Diagnostics) Len() int      { return len(dd) }
func (dd Diagnostics) Swap(i, j int) { dd[i], dd[j] = dd[j], dd[i] }
func (dd Diagnostics) Less(i, j int) bool {
	return dd[i].Severity() > dd[j].Severity() && dd[i].Type() > dd[j].Type()
}