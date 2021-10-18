package diag

func FromError(err error, severity Severity, dt DiagnosticType, summary, details string) *ExecutionError {
	return &ExecutionError{
		Err:            err,
		severity:       severity,
		summary:        summary,
		detail:         details,
		diagnosticType: dt,
	}
}
