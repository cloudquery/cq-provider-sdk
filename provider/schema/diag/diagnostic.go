package diag

type Severity int

const (
	// IGNORE severity is set for diagnostics that were ignored by the SDK
	IGNORE Severity = iota
	// WARNING severity are diagnostics that should be fixed but aren't fatal to the fetch execution
	WARNING
	// ERROR severity are diagnostics that were fatal in the fetch execution and should be fixed.
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
	Resource string
	Summary  string
	Detail   string
}

type Diagnostics []Diagnostic

func (dd Diagnostics) Len() int      { return len(dd) }
func (dd Diagnostics) Swap(i, j int) { dd[i], dd[j] = dd[j], dd[i] }
func (dd Diagnostics) Less(i, j int) bool {
	return dd[i].Severity() > dd[j].Severity() && dd[i].Type() > dd[j].Type()
}
