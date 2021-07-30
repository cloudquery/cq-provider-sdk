package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHasDuplicates(t *testing.T) {
	assert.False(t, HasDuplicates([]string{"A", "b", "c"}))
	assert.False(t, HasDuplicates([]string{"A", "a", "c"}))
	assert.True(t, HasDuplicates([]string{"a", "a", "c"}))
	assert.True(t, HasDuplicates([]string{"a", "a", "c", "c", "f"}))
}
