package diag

import (
	"fmt"
)

// BaseError is a generic error returned when execution is run, satisfies Diagnostic interface
type BaseError struct {
	// err is the underlying go error this diagnostic wraps
	Err error

	// Resource indicates the resource that failed in the execution
	resource string

	// ResourceId indicates the id of the resource that failed in the execution
	resourceId []string

	// Severity indicates the level of the Diagnostic. Currently, can be set to
	// either Error/Warning/Ignore
	severity Severity

	severitySet bool

	// Summary is a short description of the problem
	summary string

	// Detail is an optional second message, typically used to communicate a potential fix to the user.
	detail string

	// DiagnosticType indicates the classification family of this diagnostic
	diagnosticType DiagnosticType

	// if noOverwrite is true, further Options won't overwrite previously set values
	noOverwrite bool
}

// NewBaseError creates a BaseError from given error
func NewBaseError(err error, dt DiagnosticType, opts ...BaseErrorOption) *BaseError {
	be := baseFromError(err, dt)
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
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.summary == "" {
		return "No summary"
	}
	return e.summary
}

func (e BaseError) Unwrap() error {
	return e.Err
}

type BaseErrorOption func(*BaseError)

func WithNoOverwrite() BaseErrorOption {
	return func(e *BaseError) {
		e.noOverwrite = true
	}
}

func WithSeverity(s Severity) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || !e.severitySet {
			e.severity = s
			e.severitySet = true
		}
	}
}

func WithType(dt DiagnosticType) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || dt > e.diagnosticType {
			e.diagnosticType = dt
		}
	}
}

func WithSummary(summary string, args ...interface{}) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || e.summary == "" {
			e.summary = fmt.Sprintf(summary, args...)
		}
	}
}

func WithResourceName(resource string) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || e.resource == "" {
			e.resource = resource
		}
	}
}

func WithResourceId(id []string) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || len(e.resourceId) == 0 {
			e.resourceId = id
		}
	}
}

func WithDetails(detail string, args ...interface{}) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || e.detail != "" {
			e.detail = fmt.Sprintf(detail, args...)
		}
	}
}

// baseFromError creates a BaseError from the given error if it's not already a BaseError
func baseFromError(err error, dt DiagnosticType) *BaseError {
	d, ok := err.(*BaseError)
	if ok {
		return d
	}
	return &BaseError{
		Err:            err,
		diagnosticType: dt,
		severity:       ERROR,
	}
}
