package internal

import (
	"testing"
)

func TestSkiplist(t *testing.T) {
	testMemtable(t, NewSkipListMemtable)
}

func BenchmarkSkiplist(b *testing.B) {
	benchmarkMemtable(b, NewSkipListMemtable)
}
