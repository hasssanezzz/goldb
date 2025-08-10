package internal

import (
	"math/rand/v2"
	"sync"
)

const (
	MaxLevel    = 32
	Probability = 0.25
)

type skipNode struct {
	key     string
	value   Position // Assuming Position is defined elsewhere (e.g., shared/types.go)
	forward []*skipNode
}

// SkipList implements the Memtable interface using a skip list data structure.
// It uses a coarse-grained mutex for simplicity and correctness.
// A production implementation might use more fine-grained locking for better concurrency.
type SkipList struct {
	header *skipNode
	level  int
	size   uint32
	mu     sync.RWMutex
	// If using per-instance rand:
	// randSource *lockedRand
}

// NewSkipListMemtable creates a new SkipList implementing the Memtable interface.
func NewSkipListMemtable() Memtable {
	header := &skipNode{
		forward: make([]*skipNode, MaxLevel),
	}
	// If using per-instance rand:
	// rs := &lockedRand{r: rand.New(rand.NewSource(time.Now().UnixNano()))}
	return &SkipList{
		header: header,
		level:  0,
		size:   0,
		// randSource: rs,
	}
}

// randomLevel generates a random level for a new node.
func (sl *SkipList) randomLevel() int {
	level := 1
	// No locking needed as math/rand functions are safe for concurrent use
	// by multiple goroutines, although they serialize access internally.
	for rand.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

// Set inserts or updates a key-value pair in the skip list.
// Time Complexity: Average O(log N)
func (sl *SkipList) Set(pair KVPair) { // Correct signature from Memtable interface
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipNode, MaxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < pair.Key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && current.key == pair.Key {
		current.value = pair.Value
		return
	}

	newLevel := sl.randomLevel()

	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	newNode := &skipNode{
		key:     pair.Key,
		value:   pair.Value,
		forward: make([]*skipNode, newLevel),
	}

	for i := range newLevel {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size++
}

// Get retrieves the value associated with a key.
// Time Complexity: Average O(log N)
func (sl *SkipList) Get(key string) Position { // Correct signature from Memtable interface
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil && current.key == key {
		return current.value
	}

	return Position{} // Return zero value if not found
}

// Contains checks if a key exists in the skip list.
// Time Complexity: Average O(log N)
func (sl *SkipList) Contains(key string) bool { // Correct signature from Memtable interface
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	return current != nil && current.key == key
}

// Items returns all key-value pairs in the skip list, sorted by key.
// Time Complexity: O(N)
func (sl *SkipList) Items() []KVPair { // Correct signature from Memtable interface
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var items []KVPair
	current := sl.header.forward[0] // Start from the first actual node

	// Traverse the level 0 linked list to get all items in order.
	for current != nil {
		// Note: Items() returns all items, including potentially logically deleted ones
		// (where Position.Size might be 0), unless the Memtable contract specifies otherwise.
		// Based on the interface and typical usage, it returns all stored KV pairs.
		items = append(items, KVPair{Key: current.key, Value: current.value})
		current = current.forward[0]
	}
	return items
}

// Size returns the number of elements in the skip list.
// Time Complexity: O(1)
func (sl *SkipList) Size() uint32 { // Correct signature from Memtable interface
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// Reset clears the skip list, removing all elements, and returns the size before reset.
// Time Complexity: O(1) for resetting pointers, O(N) for garbage collection eventually.
func (sl *SkipList) Reset() { // Correct signature from Memtable interface
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Reset the header's forward pointers.
	for i := range sl.header.forward {
		sl.header.forward[i] = nil
	}
	sl.level = 0
	sl.size = 0
}
