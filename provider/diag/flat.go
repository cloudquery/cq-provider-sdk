package diag

type FlatDiag struct {
	Err      string
	Resource string
	Type     DiagnosticType
	Severity Severity
	Summary  string
}

func FlattenDiags(dd Diagnostics) []FlatDiag {
	df := make([]FlatDiag, len(dd))
	for i, d := range dd {

		df[i] = FlatDiag{
			Err:      d.Error(),
			Resource: d.Description().Resource,
			Type:     d.Type(),
			Severity: d.Severity(),
			Summary:  d.Description().Summary,
		}
	}
	return df
}
