package diag

import (
	"bytes"
	"strings"
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
	error
	Severity() Severity
	Type() DiagnosticType
	Description() Description
}

type Description struct {
	Resource   string
	ResourceID []string
	AccountID  string

	Summary string
	Detail  string
}

// diagLine writes the given Diagnostic as a single line to the given buf
func diagLine(buf *bytes.Buffer, d Diagnostic) {
	desc := d.Description()
	if l := len(desc.ResourceID); l > 0 || desc.AccountID != "" {
		accountAndResID := make([]string, 0, 2)
		if desc.AccountID != "" {
			accountAndResID = append(accountAndResID, desc.AccountID)
		}
		if l > 0 {
			accountAndResID = append(accountAndResID, strings.Join(desc.ResourceID, ","))
		}
		buf.WriteString("[")
		buf.WriteString(strings.Join(accountAndResID, ":"))
		buf.WriteString("] ")
	}
	buf.WriteString(desc.Summary)

	if desc.Detail != "" {
		buf.WriteString(": ")
		buf.WriteString(desc.Detail)
	}
}
