package internal

import "sync"

type treeNode struct {
	key    string
	value  Position
	left   *treeNode
	right  *treeNode
	height int
}

type Position struct {
	Offset uint32
	Size   uint32
}

type AVLTable struct {
	size uint32
	root *treeNode
	mu   sync.RWMutex
}

func NewAVLMemtable() Memtable {
	return &AVLTable{}
}

func (t *AVLTable) Set(pair KVPair) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.get(t.root, pair.Key).Size == 0 {
		t.size++
	}
	t.root = t.insert(t.root, pair.Key, pair.Value)
}

func (t *AVLTable) Get(key string) Position {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.get(t.root, key)
}

func (t *AVLTable) Contains(key string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.get(t.root, key).Size != 0
}

func (t *AVLTable) Items() []KVPair {
	t.mu.RLock()
	defer t.mu.RUnlock()

	r := []KVPair{}
	t.inOrder(t.root, &r)
	return r
}

func (t *AVLTable) Size() uint32 {
	return t.size
}

func (t *AVLTable) height(node *treeNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func (t *AVLTable) balanceFactor(node *treeNode) int {
	if node == nil {
		return 0
	}
	return t.height(node.left) - t.height(node.right)
}

func (t *AVLTable) rightRotate(y *treeNode) *treeNode {
	x := y.left
	T2 := x.right

	// perform rotation
	x.right = y
	y.left = T2

	// update heights
	y.height = max(t.height(y.left), t.height(y.right)) + 1
	x.height = max(t.height(x.left), t.height(x.right)) + 1

	// return new root
	return x
}

func (t *AVLTable) leftRotate(x *treeNode) *treeNode {
	y := x.right
	T2 := y.left

	// perform rotation
	y.left = x
	x.right = T2

	// update heights
	x.height = max(t.height(x.left), t.height(x.right)) + 1
	y.height = max(t.height(y.left), t.height(y.right)) + 1

	// return new root
	return y
}

func (t *AVLTable) balance(node *treeNode, key string) *treeNode {
	balance := t.balanceFactor(node)

	// left left case
	if balance > 1 && key < node.left.key {
		return t.rightRotate(node)
	}

	// right right case
	if balance < -1 && key > node.right.key {
		return t.leftRotate(node)
	}

	// left right case
	if balance > 1 && key > node.left.key {
		node.left = t.leftRotate(node.left)
		return t.rightRotate(node)
	}

	// right left case
	if balance < -1 && key < node.right.key {
		node.right = t.rightRotate(node.right)
		return t.leftRotate(node)
	}

	return node
}

func (t *AVLTable) insert(node *treeNode, key string, value Position) *treeNode {
	// perform normal bst insertion
	if node == nil {
		return &treeNode{key: key, value: value, height: 1}
	}

	if key < node.key {
		node.left = t.insert(node.left, key, value)
	} else if key > node.key {
		node.right = t.insert(node.right, key, value)
	} else {
		node.value = value
		return node
	}

	node.height = 1 + max(t.height(node.left), t.height(node.right))

	return t.balance(node, key)
}

func (t *AVLTable) get(node *treeNode, key string) Position {
	if node == nil {
		return Position{}
	}

	if node.key == key {
		return node.value
	} else if node.key > key {
		return t.get(node.left, key)
	} else {
		return t.get(node.right, key)
	}
}

func (t *AVLTable) inOrder(node *treeNode, result *[]KVPair) {
	if node != nil {
		t.inOrder(node.left, result)
		*result = append(*result, KVPair{node.key, node.value})
		t.inOrder(node.right, result)
	}
}
