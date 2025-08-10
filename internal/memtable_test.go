package internal

import (
	"fmt"
	"reflect"
	"testing"
)

// populateMemtable is a helper to fill a memtable with N unique key-value pairs.
func populateMemtable(m Memtable, n int) {
	for i := range n {
		// Use a unique key for each N to avoid update costs in setup if possible,
		// though for b.N iterations, keys will repeat within a single benchmark run's setup.
		// The main point is to have a full memtable for Get/Contains/Items.
		// For true isolation per benchmark *run*, creating a new memtable is better.
		m.Set(KVPair{Key: fmt.Sprintf("key%d", i), Value: Position{Offset: uint32(i), Size: uint32(i)}})
	}
}

func testMemtable(t *testing.T, newMemtable func() Memtable) {
	// Expected items (assuming Items() returns a slice of KVPair)
	pairs := []KVPair{
		{Key: "x", Value: Position{30, 30}},
		{Key: "y", Value: Position{10, 10}},
		{Key: "z", Value: Position{20, 20}},
	}

	// Initialize a new Memtable for this test
	memtable := newMemtable()
	for _, pair := range pairs {
		memtable.Set(pair)
	}

	t.Run("Memtable.Size", func(t *testing.T) {
		res := memtable.Size()
		if len(pairs) != int(res) {
			t.Errorf("Size() = %v, want %v", res, len(pairs))
		}
	})

	t.Run("Memtable.Get", func(t *testing.T) {
		for _, pair := range pairs {
			res := memtable.Get(pair.Key)
			if pair.Value.Offset != res.Offset {
				t.Errorf("Get().Offset = %v, want %v", res.Size, pair.Value.Offset)
			}
			if pair.Value.Size != res.Size {
				t.Errorf("Get().Size = %v, want %v", res.Size, pair.Value.Size)
			}
		}

		res := memtable.Get("unknown key")
		if res.Offset != 0 {
			t.Errorf("Get().Offset = %v, want %v", res.Offset, 0)
		}
		if res.Size != 0 {
			t.Errorf("Get().Size = %v, want %v", res.Size, 0)
		}
	})

	t.Run("Memtable.Items", func(t *testing.T) {
		items := memtable.Items()
		if !reflect.DeepEqual(items, pairs) {
			t.Errorf("Items() = %v, want %v", items, pairs)
		}
	})

	t.Run("Memtable.Contains", func(t *testing.T) {
		for _, pair := range pairs {
			res := memtable.Contains(pair.Key)
			if !res {
				t.Errorf("Contains() = %v, want %v", res, true)
			}
		}

		res := memtable.Contains("unknown key")
		if res {
			t.Errorf("Contains() = %v, want %v", res, false)
		}
	})

	t.Run("Memtable.Reset", func(t *testing.T) {
		memtable.Reset()

		res := memtable.Size()
		if res != 0 {
			t.Errorf("after Reset() Size() = %v, want %v", res, 0)
		}
	})
}

func benchmarkMemtable(b *testing.B, newMemtable func() Memtable) {
	// Benchmark Set with a fresh memtable each time to isolate insert/update costs
	// within the measurement.
	b.Run("Set", func(b *testing.B) {
		memtable := newMemtable() // Create memtable inside the run for isolation if needed,
		// but more importantly, don't pre-populate it here.
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// This benchmarks mixed insert/update depending on key reuse within b.N
			// If you want pure inserts, ensure keys are globally unique or use a new memtable per 'op'.
			// For standard bench behavior, this is common.
			memtable.Set(KVPair{Key: fmt.Sprintf("key%d", i), Value: Position{Offset: uint32(i), Size: uint32(i)}})
		}
	})

	// Benchmark Get on a memtable populated with b.N items.
	b.Run("Get", func(b *testing.B) {
		memtable := newMemtable()
		populateMemtable(memtable, b.N) // Ensure memtable has b.N items before measuring Get
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			memtable.Get(fmt.Sprintf("key%d", i)) // Assumes keys exist
		}
	})

	// Benchmark Contains on a memtable populated with b.N items.
	b.Run("Contains", func(b *testing.B) {
		memtable := newMemtable()
		populateMemtable(memtable, b.N) // Ensure memtable has b.N items
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			memtable.Contains(fmt.Sprintf("key%d", i)) // Assumes keys exist
		}
	})

	// Benchmark Items on a memtable populated with b.N items.
	b.Run("Items", func(b *testing.B) {
		memtable := newMemtable()
		populateMemtable(memtable, b.N) // Ensure memtable has b.N items
		b.ResetTimer()
		memtable.Items()
	})
}
