package diag

import "fmt"

type SquashedDiag struct {
	Diagnostic
	count uint64
}

func (s SquashedDiag) Description() Description {
	description := s.Diagnostic.Description()
	if s.count == 1 {
		return description
	}
	if description.Detail == "" {
		description.Detail = fmt.Sprintf("Repeated[%d]", s.count)
	} else {
		description.Detail = fmt.Sprintf("Repeated[%d]: %s", s.count, description.Detail)
	}

	return description
}

func (s SquashedDiag) Count() uint64 {
	return s.count
}

type Countable interface {
	Count() uint64
}

func CountDiag(d Diagnostic) uint64 {
	if c, ok := d.(Countable); ok {
		return c.Count()
	}

	return 1
}

var _ Countable = (*SquashedDiag)(nil)
