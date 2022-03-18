package diag

import (
	"fmt"
	"path"
	"runtime"
)

// WrapError wraps error with the following string: "error at function_name[filename:line_number]: %w"
// if err is nil returns nil
func WrapError(err error) error {
	if err != nil {
		// notice that we're using 1, so it will actually log the where
		// the error happened, 0 = this function, we don't want that.
		pc, filename, line, ok := runtime.Caller(1)
		if ok {
			return fmt.Errorf("error at %s[%s:%d] %w", runtime.FuncForPC(pc).Name(), path.Base(filename), line, err)
		}
	}
	return err
}

// BaseError is a generic error returned when execution is run, satisfies Diagnostic interface
type BaseError struct {
	// err is the underlying go error this diagnostic wraps. Can be nil
	err error

	// resource indicates the resource that failed in the execution
	resource string

	// resourceId indicates the id of the resource that failed in the execution
	resourceId []string

	// accountId indicates the id of the account
	accountId string

	// severity indicates the level of the Diagnostic. Currently, can be set to
	// either Error/Warning/Ignore
	severity Severity

	// severitySet indicates that the value in severity field is valid/set or not
	severitySet bool

	// summary is a short description of the problem
	summary string

	// detail is an optional second message, typically used to communicate a potential fix to the user.
	detail string

	// diagnosticType indicates the classification family of this diagnostic
	diagnosticType DiagnosticType

	// noOverwrite if it's true, further Options won't overwrite previously set values. Valid for the duration of one "invocation"
	noOverwrite bool
}

// NewBaseError creates a BaseError from given error, except the given error is a BaseError itself
func NewBaseError(err error, dt DiagnosticType, opts ...BaseErrorOption) *BaseError {
	be, ok := err.(*BaseError)
	if !ok {
		be = &BaseError{
			err:            err,
			diagnosticType: dt,
			severity:       ERROR,
		}
	}
	for _, o := range opts {
		o(be)
	}
	be.noOverwrite = false // reset overwrite switch after every application of opts

	return be
}

func (e BaseError) Severity() Severity {
	return e.severity
}

func (e BaseError) Description() Description {
	summary := e.summary
	if e.summary == "" {
		summary = e.Error()
	} else if e.err != nil {
		if es := e.err.Error(); es != summary {
			summary += ": " + es
		}
	}

	return Description{
		e.resource,
		e.resourceId,
		summary,
		e.detail,
		e.accountId,
	}
}

func (e BaseError) Type() DiagnosticType {
	return e.diagnosticType
}

func (e BaseError) Error() string {
	// return original error
	if e.err != nil {
		return e.err.Error()
	}
	if e.summary == "" {
		return "No summary"
	}
	return e.summary
}

func (e BaseError) Unwrap() error {
	return e.err
}

type BaseErrorOption func(*BaseError)

// WithNoOverwrite sets the noOverwrite flag of BaseError, active for the duration of the application of options
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

func WithAccountId(id string) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || e.accountId == "" {
			e.accountId = id
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

func WithError(err error) BaseErrorOption {
	return func(e *BaseError) {
		if !e.noOverwrite || e.err == nil {
			e.err = err
		}
	}
}

// Convert an error to Diagnostics, or return if it's already of type diagnostic(s). nil error returns nil value.
func FromError(err error, dt DiagnosticType, opts ...BaseErrorOption) Diagnostics {
	if err == nil {
		return nil
	}

	switch d := err.(type) {
	case Diagnostics:
		return d
	case Diagnostic:
		return Diagnostics{d}
	default:
		return Diagnostics{NewBaseError(err, dt, opts...)}
	}
}
