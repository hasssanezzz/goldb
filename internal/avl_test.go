package internal

import (
	"testing"
)

func TestAVL(t *testing.T) {
	testMemtable(t, NewAVLMemtable)
}

func BenchmarkAVL(b *testing.B) {
	benchmarkMemtable(b, NewAVLMemtable)
}
