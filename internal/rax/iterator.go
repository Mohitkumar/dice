package rax

import (
	"bytes"
)

type iterator struct {
	tree     *Tree
	nextNode *Node
	levelIdx int
	levels   []*level
	eof      bool
	reverse  bool
}

type Iterator interface {
	HasNext() bool
	Next() *Node
	Seek(key []byte) bool
	SeekWithOperation(key []byte, operation string) bool
	EOF() bool
	SeekToFirst()
	SeekToLast()
	// Reverse iteration methods
	Reverse() Iterator
	SeekToFirstReverse() bool
	SeekToLastReverse() bool
	SeekReverse(key []byte) bool
	SeekWithOperationReverse(key []byte, operation string) bool
}

// Seek operations
const (
	SEEK_EQ = iota // Exact match
	SEEK_GE        // Greater than or equal (>=)
	SEEK_LE        // Less than or equal (<=)
	SEEK_GT        // Greater than (>)
	SEEK_LT        // Less than (<)
)

// Seek operation strings
const (
	OP_EQ    = "="  // Exact match
	OP_GE    = ">=" // Greater than or equal
	OP_LE    = "<=" // Less than or equal
	OP_GT    = ">"  // Greater than
	OP_LT    = "<"  // Less than
	OP_START = "^"  // Start of range (first element)
	OP_END   = "$"  // End of range (last element)
)

// Iterator pattern
func (t *Tree) Iterator() Iterator {
	return &iterator{
		tree:     t,
		nextNode: t.root,
		levelIdx: 0,
		levels:   []*level{{t.root, nullIdx}},
		eof:      false,
		reverse:  false,
	}
}

// Reverse returns a reverse iterator
func (t *Tree) ReverseIterator() Iterator {
	var startNode *Node
	if t.root != nil {
		startNode = t.root.maximum()
	}
	return &iterator{
		tree:     t,
		nextNode: startNode,
		levelIdx: 0,
		levels:   []*level{{t.root, nullIdx}},
		eof:      false,
		reverse:  true,
	}
}

func (ti *iterator) HasNext() bool {
	return ti != nil && ti.nextNode != nil && !ti.eof
}

func (ti *iterator) Next() *Node {
	if !ti.HasNext() {
		return nil
	}

	cur := ti.nextNode
	if ti.reverse {
		ti.nextReverse()
	} else {
		ti.next()
	}

	return cur
}

func (ti *iterator) addLevel() {
	newlevel := make([]*level, ti.levelIdx+10)
	copy(newlevel, ti.levels)
	ti.levels = newlevel
}

func (ti *iterator) next() {
	for {
		var nextNode *Node
		nextIdx := nullIdx

		curNode := ti.levels[ti.levelIdx].node
		curIndex := ti.levels[ti.levelIdx].index

		in := curNode.innerNode
		switch curNode.Type() {
		case Node4:
			nextIdx, nextNode = nextChild(in.children, curIndex)
		case Node16:
			nextIdx, nextNode = nextChild(in.children, curIndex)
		case Node48:
			for i := curIndex; i < len(in.keys); i++ {
				index := in.keys[byte(i)]
				child := in.children[index]
				if child != nil {
					nextIdx = i + 1
					nextNode = child
					break
				}
			}
		case Node256:
			nextIdx, nextNode = nextChild(in.children, curIndex)
		}

		if nextNode == nil {
			if ti.levelIdx > 0 {
				ti.levelIdx--
			} else {
				ti.nextNode = nil
				ti.eof = true
				return
			}
		} else {
			ti.levels[ti.levelIdx].index = nextIdx
			ti.nextNode = nextNode

			if ti.levelIdx+1 >= cap(ti.levels) {
				ti.addLevel()
			}

			ti.levelIdx++
			ti.levels[ti.levelIdx] = &level{nextNode, nullIdx}
			return
		}
	}
}

// nextReverse implements reverse iteration
func (ti *iterator) nextReverse() {
	for {
		var nextNode *Node
		nextIdx := nullIdx

		curNode := ti.levels[ti.levelIdx].node
		curIndex := ti.levels[ti.levelIdx].index

		if curNode.IsLeaf() {
			if ti.levelIdx > 0 {
				ti.levelIdx--
			} else {
				ti.nextNode = nil
				ti.eof = true
				return
			}
			continue
		}

		in := curNode.innerNode
		switch curNode.Type() {
		case Node4, Node16:
			if curIndex == nullIdx {
				curIndex = in.size - 1
			}
			for i := curIndex; i >= 0; i-- {
				child := in.children[i]
				if child != nil {
					nextIdx = i - 1
					nextNode = child
					break
				}
			}
		case Node48:
			if curIndex == nullIdx {
				curIndex = len(in.keys) - 1
			}
			for i := curIndex; i >= 0; i-- {
				index := in.keys[byte(i)]
				if index > 0 {
					child := in.children[index-1]
					if child != nil {
						nextIdx = i - 1
						nextNode = child
						break
					}
				}
			}
		case Node256:
			if curIndex == nullIdx {
				curIndex = len(in.children) - 1
			}
			for i := curIndex; i >= 0; i-- {
				child := in.children[i]
				if child != nil {
					nextIdx = i - 1
					nextNode = child
					break
				}
			}
		}

		if nextNode == nil {
			if ti.levelIdx > 0 {
				ti.levelIdx--
			} else {
				ti.nextNode = nil
				ti.eof = true
				return
			}
		} else {
			ti.levels[ti.levelIdx].index = nextIdx
			ti.nextNode = nextNode

			if ti.levelIdx+1 >= cap(ti.levels) {
				ti.addLevel()
			}

			ti.levelIdx++
			ti.levels[ti.levelIdx] = &level{nextNode, nullIdx}
			return
		}
	}
}

// EOF returns true if the iterator has reached the end
func (ti *iterator) EOF() bool {
	return ti.eof || ti.nextNode == nil
}

// SeekToFirst positions the iterator at the first element
func (ti *iterator) SeekToFirst() {
	if ti.tree.root == nil {
		ti.eof = true
		ti.nextNode = nil
		return
	}

	ti.reset()
	ti.nextNode = ti.tree.root.minimum()
	ti.eof = false
}

// SeekToLast positions the iterator at the last element
func (ti *iterator) SeekToLast() {
	if ti.tree.root == nil {
		ti.eof = true
		ti.nextNode = nil
		return
	}

	ti.reset()
	ti.nextNode = ti.tree.root.maximum()
	ti.eof = false
}

// Reverse returns a reverse iterator
func (ti *iterator) Reverse() Iterator {
	reverseIter := &iterator{
		tree:     ti.tree,
		levelIdx: 0,
		levels:   []*level{{ti.tree.root, nullIdx}},
		eof:      false,
		reverse:  !ti.reverse,
	}
	if !ti.reverse {
		// Forward to reverse: start at maximum
		if ti.tree.root != nil {
			reverseIter.nextNode = ti.tree.root.maximum()
		}
	} else {
		// Reverse to forward: start at minimum
		if ti.tree.root != nil {
			reverseIter.nextNode = ti.tree.root.minimum()
		}
	}
	return reverseIter
}

// SeekToFirstReverse positions the iterator at the first element in reverse order (i.e., the maximum)
func (ti *iterator) SeekToFirstReverse() bool {
	if ti.tree.root == nil {
		ti.eof = true
		ti.nextNode = nil
		return false
	}

	ti.reset()
	ti.reverse = true
	ti.nextNode = ti.tree.root.maximum()
	ti.eof = false
	return true
}

// SeekToLastReverse positions the iterator at the last element in reverse order (i.e., the minimum)
func (ti *iterator) SeekToLastReverse() bool {
	if ti.tree.root == nil {
		ti.eof = true
		ti.nextNode = nil
		return false
	}

	ti.reset()
	ti.reverse = true
	ti.nextNode = ti.tree.root.minimum()
	ti.eof = false
	return true
}

// SeekReverse positions the iterator at the specified key in reverse order
func (ti *iterator) SeekReverse(key []byte) bool {
	return ti.SeekWithOperationReverse(key, OP_LE)
}

// SeekWithOperationReverse positions the iterator based on the specified operation in reverse order
func (ti *iterator) SeekWithOperationReverse(key []byte, operation string) bool {
	// Convert operations for reverse iteration
	reverseOp := operation
	switch operation {
	case OP_GE:
		reverseOp = OP_LE
	case OP_LE:
		reverseOp = OP_GE
	case OP_GT:
		reverseOp = OP_LT
	case OP_LT:
		reverseOp = OP_GT
	case OP_START:
		reverseOp = OP_END
	case OP_END:
		reverseOp = OP_START
	}

	ti.reverse = true
	return ti.seekWithOperation(key, ti.getReverseOperation(reverseOp))
}

// getReverseOperation converts an operation to its reverse equivalent
func (ti *iterator) getReverseOperation(operation string) int {
	switch operation {
	case OP_START:
		return SEEK_GE // In reverse, start means find the last element
	case OP_END:
		return SEEK_LE // In reverse, end means find the first element
	case OP_EQ:
		return SEEK_EQ
	case OP_GE:
		return SEEK_LE // In reverse, >= becomes <=
	case OP_LE:
		return SEEK_GE // In reverse, <= becomes >=
	case OP_GT:
		return SEEK_LT // In reverse, > becomes <
	case OP_LT:
		return SEEK_GT // In reverse, < becomes >
	default:
		return SEEK_LE // Default to less than or equal for reverse
	}
}

// Seek positions the iterator at the specified key
// Returns true if the key was found, false otherwise
func (ti *iterator) Seek(key []byte) bool {
	return ti.seekWithOperation(key, SEEK_GE)
}

// SeekGreaterOrEqual seeks to the first key >= target
func (ti *iterator) SeekGreaterOrEqual(key []byte) bool {
	return ti.seekWithOperation(key, SEEK_GE)
}

// SeekLessOrEqual seeks to the first key <= target
func (ti *iterator) SeekLessOrEqual(key []byte) bool {
	return ti.seekWithOperation(key, SEEK_LE)
}

// SeekGreaterThan seeks to the first key > target
func (ti *iterator) SeekGreaterThan(key []byte) bool {
	return ti.seekWithOperation(key, SEEK_GT)
}

// SeekLessThan seeks to the first key < target
func (ti *iterator) SeekLessThan(key []byte) bool {
	return ti.seekWithOperation(key, SEEK_LT)
}

// seekWithOperation implements the core seeking logic with different operations
func (ti *iterator) seekWithOperation(key []byte, operation int) bool {
	if ti.tree.root == nil {
		ti.eof = true
		ti.nextNode = nil
		return false
	}

	ti.reset()

	// Add null terminator if not present
	searchKey := terminate(key)

	// Find the target node
	targetNode := ti.findTargetNode(ti.tree.root, searchKey, 0)

	if targetNode == nil {
		// Key not found, handle based on operation
		switch operation {
		case SEEK_GE, SEEK_GT:
			// Find the next key greater than the target
			ti.nextNode = ti.findNextGreater(ti.tree.root, searchKey, 0)
		case SEEK_LE, SEEK_LT:
			// Find the previous key less than the target
			ti.nextNode = ti.findPrevLess(ti.tree.root, searchKey, 0)
		}
	} else {
		// Key found, handle based on operation
		switch operation {
		case SEEK_EQ:
			ti.nextNode = targetNode
		case SEEK_GE:
			ti.nextNode = targetNode
		case SEEK_GT:
			// Find the next key greater than the target
			ti.nextNode = ti.findNextGreater(ti.tree.root, searchKey, 0)
		case SEEK_LE:
			ti.nextNode = targetNode
		case SEEK_LT:
			// Find the previous key less than the target
			ti.nextNode = ti.findPrevLess(ti.tree.root, searchKey, 0)
		}
	}

	if ti.nextNode == nil {
		ti.eof = true
		return false
	}

	ti.eof = false
	return true
}

// reset resets the iterator state
func (ti *iterator) reset() {
	ti.levelIdx = 0
	ti.levels = []*level{{ti.tree.root, nullIdx}}
}

// findTargetNode finds the exact node for the given key
func (ti *iterator) findTargetNode(current *Node, key []byte, depth int) *Node {
	for current != nil {
		if current.IsLeaf() {
			if current.leaf.IsMatch(key) {
				return current
			}
			return nil
		}

		in := current.innerNode
		if current.prefixMatchIndex(key, depth) != in.prefixLen {
			return nil
		} else {
			depth += in.prefixLen
		}

		v := in.findChild(key[depth])
		if v == nil {
			return nil
		}
		current = *(v)
		depth++
	}

	return nil
}

// findNextGreater finds the next key greater than the target
func (ti *iterator) findNextGreater(current *Node, key []byte, depth int) *Node {
	if current == nil {
		return nil
	}

	if current.IsLeaf() {
		if bytes.Compare(current.leaf.key, key) > 0 {
			return current
		}
		return nil
	}

	in := current.innerNode
	prefixMatch := current.prefixMatchIndex(key, depth)

	if prefixMatch < in.prefixLen {
		// Prefix doesn't match, find the next greater key
		if bytes.Compare(in.prefix[prefixMatch:], key[depth+prefixMatch:]) > 0 {
			return current.minimum()
		}
		return nil
	}

	depth += in.prefixLen

	// Find the next child greater than or equal to key[depth]
	var nextChild *Node
	var nextKey byte

	switch in.nodeType {
	case Node4, Node16:
		for i := 0; i < in.size; i++ {
			if in.keys[i] >= key[depth] {
				nextChild = in.children[i]
				nextKey = in.keys[i]
				break
			}
		}
	case Node48:
		for i := int(key[depth]); i < len(in.keys); i++ {
			index := in.keys[byte(i)]
			if index > 0 {
				nextChild = in.children[index-1]
				nextKey = byte(i)
				break
			}
		}
	case Node256:
		for i := int(key[depth]); i < len(in.children); i++ {
			if in.children[byte(i)] != nil {
				nextChild = in.children[byte(i)]
				nextKey = byte(i)
				break
			}
		}
	}

	if nextChild == nil {
		return nil
	}

	if nextKey > key[depth] {
		return nextChild.minimum()
	}

	// Continue searching in the child
	result := ti.findNextGreater(nextChild, key, depth+1)
	if result != nil {
		return result
	}

	// If no result in this child, try the next child
	return ti.findNextChildGreater(in, nextKey)
}

// findNextChildGreater finds the next child greater than the given key
func (ti *iterator) findNextChildGreater(in *innerNode, key byte) *Node {
	switch in.nodeType {
	case Node4, Node16:
		for i := 0; i < in.size; i++ {
			if in.keys[i] > key {
				return in.children[i].minimum()
			}
		}
	case Node48:
		for i := int(key) + 1; i < len(in.keys); i++ {
			index := in.keys[byte(i)]
			if index > 0 {
				return in.children[index-1].minimum()
			}
		}
	case Node256:
		for i := int(key) + 1; i < len(in.children); i++ {
			if in.children[byte(i)] != nil {
				return in.children[byte(i)].minimum()
			}
		}
	}
	return nil
}

// findPrevLess finds the previous key less than the target
func (ti *iterator) findPrevLess(current *Node, key []byte, depth int) *Node {
	if current == nil {
		return nil
	}

	if current.IsLeaf() {
		if bytes.Compare(current.leaf.key, key) < 0 {
			return current
		}
		return nil
	}

	in := current.innerNode
	prefixMatch := current.prefixMatchIndex(key, depth)

	if prefixMatch < in.prefixLen {
		// Prefix doesn't match, find the previous less key
		if bytes.Compare(in.prefix[prefixMatch:], key[depth+prefixMatch:]) < 0 {
			return current.maximum()
		}
		return nil
	}

	depth += in.prefixLen

	// Find the previous child less than or equal to key[depth]
	var prevChild *Node
	var prevKey byte

	switch in.nodeType {
	case Node4, Node16:
		for i := in.size - 1; i >= 0; i-- {
			if in.keys[i] <= key[depth] {
				prevChild = in.children[i]
				prevKey = in.keys[i]
				break
			}
		}
	case Node48:
		for i := int(key[depth]); i >= 0; i-- {
			index := in.keys[byte(i)]
			if index > 0 {
				prevChild = in.children[index-1]
				prevKey = byte(i)
				break
			}
		}
	case Node256:
		for i := int(key[depth]); i >= 0; i-- {
			if in.children[byte(i)] != nil {
				prevChild = in.children[byte(i)]
				prevKey = byte(i)
				break
			}
		}
	}

	if prevChild == nil {
		return nil
	}

	if prevKey < key[depth] {
		return prevChild.maximum()
	}

	// Continue searching in the child
	result := ti.findPrevLess(prevChild, key, depth+1)
	if result != nil {
		return result
	}

	// If no result in this child, try the previous child
	return ti.findPrevChildLess(in, prevKey)
}

// findPrevChildLess finds the previous child less than the given key
func (ti *iterator) findPrevChildLess(in *innerNode, key byte) *Node {
	switch in.nodeType {
	case Node4, Node16:
		for i := in.size - 1; i >= 0; i-- {
			if in.keys[i] < key {
				return in.children[i].maximum()
			}
		}
	case Node48:
		for i := int(key) - 1; i >= 0; i-- {
			index := in.keys[byte(i)]
			if index > 0 {
				return in.children[index-1].maximum()
			}
		}
	case Node256:
		for i := int(key) - 1; i >= 0; i-- {
			if in.children[byte(i)] != nil {
				return in.children[byte(i)].maximum()
			}
		}
	}
	return nil
}

// SeekWithOperation positions the iterator based on the specified operation
// Operations: =, >=, <=, >, <, ^ (start), $ (end)
func (ti *iterator) SeekWithOperation(key []byte, operation string) bool {
	switch operation {
	case OP_START:
		if ti.reverse {
			ti.SeekToLast()
		} else {
			ti.SeekToFirst()
		}
		return !ti.EOF()
	case OP_END:
		if ti.reverse {
			ti.SeekToFirst()
		} else {
			ti.SeekToLast()
		}
		return !ti.EOF()
	case OP_EQ:
		return ti.seekWithOperation(key, SEEK_EQ)
	case OP_GE:
		return ti.seekWithOperation(key, SEEK_GE)
	case OP_LE:
		return ti.seekWithOperation(key, SEEK_LE)
	case OP_GT:
		return ti.seekWithOperation(key, SEEK_GT)
	case OP_LT:
		return ti.seekWithOperation(key, SEEK_LT)
	default:
		// Default to greater than or equal
		return ti.seekWithOperation(key, SEEK_GE)
	}
}
