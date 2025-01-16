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

	// Perform rotation
	x.right = y
	y.left = T2

	// Update heights
	y.height = max(t.height(y.left), t.height(y.right)) + 1
	x.height = max(t.height(x.left), t.height(x.right)) + 1

	// Return new root
	return x
}

func (t *Table) leftRotate(x *treeNode) *treeNode {
	y := x.right
	T2 := y.left

	// Perform rotation
	y.left = x
	x.right = T2

	// Update heights
	x.height = max(t.height(x.left), t.height(x.right)) + 1
	y.height = max(t.height(y.left), t.height(y.right)) + 1

	// Return new root
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

	// Left Left Case
	if balance > 1 && key < node.left.key {
		return t.rightRotate(node)
	}

	// Right Right Case
	if balance < -1 && key > node.right.key {
		return t.leftRotate(node)
	}

	// Left Right Case
	if balance > 1 && key > node.left.key {
		node.left = t.leftRotate(node.left)
		return t.rightRotate(node)
	}

	// Right Left Case
	if balance < -1 && key < node.right.key {
		node.right = t.rightRotate(node.right)
		return t.leftRotate(node)
	}

	return node
}

func (t *Table) insert(node *treeNode, key string, value IndexNode) *treeNode {
	// Perform normal BST insertion
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

func (t *Table) findMinNode(node *treeNode) *treeNode {
	current := node
	// Find the leftmost node
	for current.left != nil {
		current = current.left
	}
	return current
}

func (t *Table) delete(node *treeNode, key string) *treeNode {
	if node == nil {
		return nil
	}

	if key < node.key {
		node.left = t.delete(node.left, key)
	} else if key > node.key {
		node.right = t.delete(node.right, key)
	} else {
		// Node with the key we want to delete is found

		// Case 1: Node with only one child or no child
		if node.left == nil {
			return node.right
		} else if node.right == nil {
			return node.left
		}

		// Case 2: Node with two children, get the inorder successor
		// (smallest in the right subtree)
		temp := t.findMinNode(node.right)
		node.key = temp.key
		node.value = temp.value

		// Delete the inorder successor
		node.right = t.delete(node.right, temp.key)
	}

	node.height = 1 + max(t.height(node.left), t.height(node.right))
	return t.balance(node, key)
}

// Public functions

// also works as "Put"
func (t *Table) Set(key string, value IndexNode) {
	if !t.Contains(key) {
		t.Size++
	}
	t.root = t.insert(t.root, key, value)
}

func (t *Table) Get(key string) IndexNode {
	return t.get(t.root, key)
}

func (t *Table) Delete(key string) {
	if !t.Contains(key) {
		return
	}

	if t.Size <= 0 {
		panic("avl size can not be decremented")
	}

	t.Size--
	t.root = t.delete(t.root, key)
}

func (t *Table) Contains(key string) bool {
	return t.get(t.root, key).Size != 0
}

func (t *Table) Items() []KVPair {
	r := []KVPair{}
	t.inOrder(t.root, &r)
	return r
}
