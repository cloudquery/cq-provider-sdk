package diag

func TelemetryFromError(err error, eventType string) Diagnostic {
	return NewBaseError(
		err,
		TELEMETRY,
		WithSeverity(IGNORE),
		WithDetails(eventType),
	)
}
