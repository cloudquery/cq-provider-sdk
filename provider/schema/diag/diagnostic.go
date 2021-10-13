package diag

type Severity int

const (
	IGNORE Severity = iota
	WARNING
	ERROR
)

type DiagnosticType int

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
