package diag

// ExecutionError is a contextual message intended at outlining problems in execution.
type ExecutionError struct {
	// Err is the underlying go error this diagnostic wraps
	Err error

	// Severity indicates the level of the Diagnostic. Currently, can be set to
	// either Error/Warning/Ignore
	severity Severity

	// Summary is a short description of the problem
	summary string

	// Detail is an optional second message, typically used to communicate a potential fix to the user.
	detail string

	// Category indicates the classification family of this diagnostic
	diagnosticType DiagnosticType
}

func (e ExecutionError) Severity() Severity {
	return e.severity
}

func (e ExecutionError) Description() Description {
	return Description{
		e.summary,
		e.detail,
	}
}

func (e ExecutionError) Type() DiagnosticType {
	return e.diagnosticType
}

func (e ExecutionError) Error() string {
	return e.Err.Error()
}