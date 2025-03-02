package bloom

import (
	"testing"
)

func TestFilter(t *testing.T) {
	s := "hello"
	for n := 1; n < 100000; n++ {
		filter := New(uint(n), 0.01, nil)

		if filter.PossiblyExists(s) {
			t.Errorf("filter.PossiblyExists(s) = true; want false")
		}

		filter.Add(s)
		if !filter.PossiblyExists(s) {
			t.Errorf("filter.PossiblyExists(s) = false; want true")
		}
	}
}
