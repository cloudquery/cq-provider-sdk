package diag


type nativeError struct {
	err error
}

func (n nativeError) Severity() Severity {
	return ERROR
}

func (n nativeError) Type() DiagnosticType {
	return INTERNAL
}

func (n nativeError) Description() Description {
	return Description{
		Resource: "",
		Summary:  n.err.Error(),
		Detail:   "",
	}
}

func (n nativeError) Error() string {
	return n.err.Error()
}

