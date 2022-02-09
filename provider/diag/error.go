package diag

import (
	"fmt"
)

// BaseError is a generic error returned when execution is run, satisfies Diagnostic interface
type BaseError struct {
	// err is the underlying go error this diagnostic wraps
	error

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
func NewBaseError(err error, dt DiagnosticType, opts ...BaseErrorOption) *BaseError {
	be := &BaseError{
		error:          err,
		diagnosticType: dt,
		severity:       ERROR,
	}
	for _, o := range opts {
		o(be)
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
	if e.error != nil {
		return e.error.Error()
	}
	if e.summary == "" {
		return "No summary"
	}
	return e.summary
}

type BaseErrorOption func(*BaseError)

func WithSeverity(s Severity) BaseErrorOption {
	return func(e *BaseError) {
		e.severity = s
	}
}

func WithType(dt DiagnosticType) BaseErrorOption {
	return func(e *BaseError) {
		e.diagnosticType = dt
	}
}

func WithSummary(summary string, args ...interface{}) BaseErrorOption {
	return func(e *BaseError) {
		e.summary = fmt.Sprintf(summary, args...)
	}
}

func WithResourceName(resource string) BaseErrorOption {
	return func(e *BaseError) {
		e.resource = resource
	}
}

func WithResourceId(id []string) BaseErrorOption {
	return func(e *BaseError) {
		e.resourceId = id
	}
}

func WithDetails(detail string, args ...interface{}) BaseErrorOption {
	return func(e *BaseError) {
		e.detail = fmt.Sprintf(detail, args...)
	}
}
