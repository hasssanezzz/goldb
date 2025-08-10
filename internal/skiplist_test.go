package internal

import (
	"reflect"
	"testing"
)

func TestSkipList(t *testing.T) {
	// Expected items (assuming Items() returns a slice of KVPair)
	pairs := []KVPair{
		{Key: "x", Value: Position{30, 30}},
		{Key: "y", Value: Position{10, 10}},
		{Key: "z", Value: Position{20, 20}},
	}

	// Initialize a new Memtable for this test
	memtable := NewSkipListMemtable()
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
