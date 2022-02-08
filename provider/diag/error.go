package diag

// BaseError is a generic error returned when execution is run, satisfies Diagnostic interface
type BaseError struct {
	// Err is the underlying go error this diagnostic wraps
	Err error

	// Resource indicates the resource that failed in the execution
	resource string

	// ResourceId indicates the id of the resource that failed in the execution
	resourceId []string

	// Severity indicates the level of the Diagnostic. Currently, can be set to
	// either Error/Warning/Ignore
	severity Severity

	// Summary is a short description of the problem
	summary string

	// Detail is an optional second message, typically used to communicate a potential fix to the user.
	detail string

	// DiagnosticType indicates the classification family of this diagnostic
	diagnosticType DiagnosticType
}

// NewBaseError creates a BaseError from given error
func NewBaseError(err error, severity Severity, dt DiagnosticType, resource, summary, details string) *BaseError {
	be := &BaseError{
		Err:            err,
		severity:       severity,
		resource:       resource,
		summary:        summary,
		detail:         details,
		diagnosticType: dt,
	}
	if r := ExtractResourceId(err); r != nil {
		be.resourceId = r
	}
	return be
}

func (e BaseError) Severity() Severity {
	return e.severity
}

func (e BaseError) Description() Description {
	summary := e.summary
	if e.summary == "" {
		summary = e.Error()
	}
	return Description{
		e.resource,
		e.resourceId,
		summary,
		e.detail,
	}
}

func (e BaseError) Type() DiagnosticType {
	return e.diagnosticType
}

func (e BaseError) Error() string {
	// return original error
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.summary
}

func ExtractResourceId(err error) []string {
	ri, ok := err.(resourceIDer)
	if !ok {
		return nil
	}
	return ri.ResourceID()
}

type resourceIDer interface {
	ResourceID() []string
}
