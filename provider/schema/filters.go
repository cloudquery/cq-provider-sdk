package schema

func DeleteParentFieldsFilter(fields ...string) func(meta ClientMeta, parent *Resource) []interface{} {
	return func(meta ClientMeta, parent *Resource) []interface{} {
		var filters []interface{}
		for _, f := range fields {
			filters = append(filters, f, parent.Get(f))
		}
		return filters
	}
}
