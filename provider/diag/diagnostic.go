package diag

import (
	"fmt"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/hcl/v2"
)

type Severity int

const (
	// IGNORE severity is set for diagnostics that were ignored by the SDK
	IGNORE Severity = iota
	// WARNING severity are diagnostics that should be fixed but aren't fatal to the fetch execution
	WARNING
	// ERROR severity are diagnostics that were fatal in the fetch execution and should be fixed.
	ERROR
	// PANIC severity are diagnostics that are returned from a panic in the underlying code.
	PANIC
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
	case UNKNOWN:
		fallthrough
	default:
		return "UNKNOWN"
	}
}

const (
	UNKNOWN DiagnosticType = iota
	RESOLVING
	ACCESS
	THROTTLE
	DATABASE
	SCHEMA
	INTERNAL
)

type Diagnostic interface {
	Severity() Severity
	Type() DiagnosticType
	Description() Description
	error
}

type Description struct {
	Resource string
	Summary  string
	Detail   string
}

type Diagnostics []Diagnostic

func (diags Diagnostics) Error() string {
	panic("implement me")
}

func (diags Diagnostics) HasErrors() bool {
	return len(diags) > 0
}

func (diags Diagnostics) HasDiags() bool {
	return len(diags) > 0
}

func (diags Diagnostics) Append(new ...interface{}) Diagnostics {
	for _, item := range new {
		if item == nil {
			continue
		}

		switch ti := item.(type) {
		case Diagnostic:
			diags = append(diags, ti)
		case Diagnostics:
			diags = append(diags, ti...) // flatten
		case error:
			switch {
			case errwrap.ContainsType(ti, Diagnostics(nil)):
				// If we have an errwrap wrapper with a Diagnostics hiding
				// inside then we'll unpick it here to get access to the
				// individual diagnostics.
				diags = diags.Append(errwrap.GetType(ti, Diagnostics(nil)))
			case errwrap.ContainsType(ti, hcl.Diagnostics(nil)):
				// Likewise, if we have HCL diagnostics we'll unpick that too.
				diags = diags.Append(errwrap.GetType(ti, hcl.Diagnostics(nil)))
			default:
				diags = append(diags, nativeError{ti})
			}
		default:
			panic(fmt.Errorf("can't construct diagnostic(s) from %T", item))
		}
	}

	// Given the above, we should never end up with a non-nil empty slice
	// here, but we'll make sure of that so callers can rely on empty == nil
	if len(diags) == 0 {
		return nil
	}

	return diags
}

func (diags Diagnostics) Warnings() uint64 {
	var warningsCount uint64 = 0
	for _, d := range diags {
		if d.Severity() == WARNING {
			warningsCount++
		}
	}
	return warningsCount
}

func (diags Diagnostics) Errors() uint64 {
	var errorCount uint64 = 0
	for _, d := range diags {
		if d.Severity() == ERROR {
			errorCount++
		}
	}
	return errorCount
}

func (diags Diagnostics) Len() int      { return len(diags) }
func (diags Diagnostics) Swap(i, j int) { diags[i], diags[j] = diags[j], diags[i] }
func (diags Diagnostics) Less(i, j int) bool {
	return diags[i].Severity() > diags[j].Severity() && diags[i].Type() > diags[j].Type()
}
