package memtable

type treeNode struct {
	key    string
	value  IndexNode
	left   *treeNode
	right  *treeNode
	height int
}

type Table struct {
	Size uint32
	root *treeNode
}

type KVPair struct {
	Key   string
	Value IndexNode
}

type KVPairSlice []KVPair

func (a KVPairSlice) Len() int           { return len(a) }
func (a KVPairSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a KVPairSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type IndexNode struct {
	Offset uint32
	Size   uint32
}

func New() *Table {
	return &Table{}
}

func (t *Table) height(node *treeNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func (t *Table) balanceFactor(node *treeNode) int {
	if node == nil {
		return 0
	}
	return t.height(node.left) - t.height(node.right)
}

func (t *Table) rightRotate(y *treeNode) *treeNode {
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

func (t *Table) leftRotate(x *treeNode) *treeNode {
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (t *Table) balance(node *treeNode, key string) *treeNode {
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

func (t *Table) insert(node *treeNode, key string, value IndexNode) *treeNode {
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

func (t *Table) get(node *treeNode, key string) IndexNode {
	if node == nil {
		return IndexNode{}
	}

	if node.key == key {
		return node.value
	} else if node.key > key {
		return t.get(node.left, key)
	} else {
		return t.get(node.right, key)
	}
}

func (t *Table) inOrder(node *treeNode, result *[]KVPair) {
	if node != nil {
		t.inOrder(node.left, result)
		*result = append(*result, KVPair{node.key, node.value})
		t.inOrder(node.right, result)
	}
}

// public functions

// also works as "put"
func (t *Table) Set(key string, value IndexNode) {
	if !t.Contains(key) {
		t.Size++
	}
	t.root = t.insert(t.root, key, value)
}

func (t *Table) Get(key string) IndexNode {
	return t.get(t.root, key)
}

func (t *Table) Contains(key string) bool {
	return t.get(t.root, key).Size != 0
}

func (t *Table) Items() []KVPair {
	r := []KVPair{}
	t.inOrder(t.root, &r)
	return r
}
