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
	it := &iterator{
		tree:     t,
		nextNode: nil,
		levelIdx: 0,
		levels:   nil,
		eof:      false,
		reverse:  false,
	}
	it.seekToExtreme(false)
	return it
}

// Reverse returns a reverse iterator
func (t *Tree) ReverseIterator() Iterator {
	it := &iterator{
		tree:     t,
		nextNode: nil,
		levelIdx: 0,
		levels:   nil,
		eof:      false,
		reverse:  true,
	}
	it.seekToExtreme(true)
	return it
}

func (ti *iterator) HasNext() bool {
	return ti != nil && ti.nextNode != nil && !ti.eof
}

// seekToExtreme sets up the stack to the min (reverse=false) or max (reverse=true) leaf
func (ti *iterator) seekToExtreme(reverse bool) {
	ti.levels = nil
	ti.levelIdx = 0
	cur := ti.tree.root
	if cur == nil {
		ti.nextNode = nil
		ti.eof = true
		return
	}
	for !cur.IsLeaf() {
		in := cur.innerNode
		var child *Node
		var idx int
		if reverse {
			switch cur.Type() {
			case Node4, Node16:
				idx = in.size - 1
				child = in.children[idx]
			case Node48:
				idx = len(in.keys) - 1
				for idx >= 0 && in.keys[byte(idx)] == 0 {
					idx--
				}
				if idx >= 0 {
					child = in.children[in.keys[byte(idx)]-1]
				}
			case Node256:
				idx = len(in.children) - 1
				for idx >= 0 && in.children[idx] == nil {
					idx--
				}
				if idx >= 0 {
					child = in.children[idx]
				}
			}
		} else {
			switch cur.Type() {
			case Node4, Node16:
				idx = 0
				child = in.children[idx]
			case Node48:
				idx = 0
				for idx < len(in.keys) && in.keys[byte(idx)] == 0 {
					idx++
				}
				if idx < len(in.keys) {
					child = in.children[in.keys[byte(idx)]-1]
				}
			case Node256:
				idx = 0
				for idx < len(in.children) && in.children[idx] == nil {
					idx++
				}
				if idx < len(in.children) {
					child = in.children[idx]
				}
			}
		}
		ti.levels = append(ti.levels, &level{cur, idx})
		cur = child
	}
	ti.levels = append(ti.levels, &level{cur, nullIdx})
	ti.levelIdx = len(ti.levels) - 1
	ti.nextNode = cur
	ti.eof = false
}

func (ti *iterator) Next() *Node {
	if ti.eof || ti.nextNode == nil {
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

func (ti *iterator) next() {
	for ti.levelIdx >= 0 {
		lvl := ti.levels[ti.levelIdx]
		node := lvl.node
		if node.IsLeaf() {
			ti.levelIdx--
			continue
		}
		in := node.innerNode
		var found bool
		var nextIdx int
		var nextNode *Node
		switch node.Type() {
		case Node4, Node16:
			for i := lvl.index + 1; i < in.size; i++ {
				if in.children[i] != nil {
					nextIdx = i
					nextNode = in.children[i]
					found = true
					break
				}
			}
		case Node48:
			for i := lvl.index + 1; i < len(in.keys); i++ {
				idx := in.keys[byte(i)]
				if idx > 0 {
					child := in.children[idx-1]
					if child != nil {
						nextIdx = i
						nextNode = child
						found = true
						break
					}
				}
			}
		case Node256:
			for i := lvl.index + 1; i < len(in.children); i++ {
				if in.children[i] != nil {
					nextIdx = i
					nextNode = in.children[i]
					found = true
					break
				}
			}
		}
		if found {
			ti.levels[ti.levelIdx].index = nextIdx
			ti.levelIdx++
			if ti.levelIdx < len(ti.levels) {
				ti.levels[ti.levelIdx] = &level{nextNode, nullIdx}
			} else {
				ti.levels = append(ti.levels, &level{nextNode, nullIdx})
			}
			ti.nextNode = ti.descendToLeaf()
			return
		} else {
			ti.levelIdx--
		}
	}
	ti.nextNode = nil
	ti.eof = true
}

func (ti *iterator) nextReverse() {
	for ti.levelIdx >= 0 {
		lvl := ti.levels[ti.levelIdx]
		node := lvl.node
		if node.IsLeaf() {
			ti.levelIdx--
			continue
		}
		in := node.innerNode
		var found bool
		var prevIdx int
		var prevNode *Node
		switch node.Type() {
		case Node4, Node16:
			for i := lvl.index - 1; i >= 0; i-- {
				if in.children[i] != nil {
					prevIdx = i
					prevNode = in.children[i]
					found = true
					break
				}
			}
		case Node48:
			for i := lvl.index - 1; i >= 0; i-- {
				idx := in.keys[byte(i)]
				if idx > 0 {
					child := in.children[idx-1]
					if child != nil {
						prevIdx = i
						prevNode = child
						found = true
						break
					}
				}
			}
		case Node256:
			for i := lvl.index - 1; i >= 0; i-- {
				if in.children[i] != nil {
					prevIdx = i
					prevNode = in.children[i]
					found = true
					break
				}
			}
		}
		if found {
			ti.levels[ti.levelIdx].index = prevIdx
			ti.levelIdx++
			if ti.levelIdx < len(ti.levels) {
				ti.levels[ti.levelIdx] = &level{prevNode, nullIdx}
			} else {
				ti.levels = append(ti.levels, &level{prevNode, nullIdx})
			}
			ti.nextNode = ti.descendToLeaf()
			return
		} else {
			ti.levelIdx--
		}
	}
	ti.nextNode = nil
	ti.eof = true
}

// descendToLeaf walks down from the current stack top to the next leaf, updating the stack
func (ti *iterator) descendToLeaf() *Node {
	cur := ti.levels[ti.levelIdx].node
	for !cur.IsLeaf() {
		in := cur.innerNode
		var child *Node
		var idx int
		switch cur.Type() {
		case Node4, Node16:
			idx = 0
			if ti.reverse {
				idx = in.size - 1
			}
			child = in.children[idx]
		case Node48:
			if ti.reverse {
				idx = len(in.keys) - 1
				for idx >= 0 && in.keys[byte(idx)] == 0 {
					idx--
				}
				if idx >= 0 {
					child = in.children[in.keys[byte(idx)]-1]
				}
			} else {
				idx = 0
				for idx < len(in.keys) && in.keys[byte(idx)] == 0 {
					idx++
				}
				if idx < len(in.keys) {
					child = in.children[in.keys[byte(idx)]-1]
				}
			}
		case Node256:
			if ti.reverse {
				idx = len(in.children) - 1
				for idx >= 0 && in.children[idx] == nil {
					idx--
				}
				if idx >= 0 {
					child = in.children[idx]
				}
			} else {
				idx = 0
				for idx < len(in.children) && in.children[idx] == nil {
					idx++
				}
				if idx < len(in.children) {
					child = in.children[idx]
				}
			}
		}
		ti.levelIdx++
		if ti.levelIdx < len(ti.levels) {
			ti.levels[ti.levelIdx] = &level{child, nullIdx}
		} else {
			ti.levels = append(ti.levels, &level{child, nullIdx})
		}
		cur = child
	}
	return cur
}

// EOF returns true if the iterator has reached the end
func (ti *iterator) EOF() bool {
	return ti.eof || ti.nextNode == nil
}

// SeekToFirst positions the iterator at the first element
func (ti *iterator) SeekToFirst() {
	ti.seekToExtreme(false)
}

// SeekToLast positions the iterator at the last element
func (ti *iterator) SeekToLast() {
	ti.seekToExtreme(true)
}

// Reverse returns a reverse iterator
func (ti *iterator) Reverse() Iterator {
	it := &iterator{
		tree:     ti.tree,
		nextNode: nil,
		levelIdx: 0,
		levels:   nil,
		eof:      false,
		reverse:  !ti.reverse,
	}
	if ti.nextNode == nil {
		if !ti.reverse {
			it.seekToExtreme(true)
		} else {
			it.seekToExtreme(false)
		}
		return it
	}
	// Set up the stack to the current node
	it.levels = make([]*level, len(ti.levels))
	for i, lvl := range ti.levels {
		it.levels[i] = &level{lvl.node, lvl.index}
	}
	it.levelIdx = ti.levelIdx
	it.eof = ti.eof
	// For reverse, set nextNode to the previous node in reverse order
	if !ti.reverse {
		// Forward to reverse: set up so that the next call to Next yields the previous node
		it.reverse = true
		it.nextNode = ti.levels[it.levelIdx].node
		it.nextReverse() // move to previous node
	} else {
		// Reverse to forward: set up so that the next call to Next yields the next node
		it.reverse = false
		it.nextNode = ti.levels[it.levelIdx].node
		it.next() // move to next node
	}
	return it
}

// SeekToFirstReverse positions the iterator at the first element in reverse order (i.e., the maximum)
func (ti *iterator) SeekToFirstReverse() bool {
	ti.seekToExtreme(true)
	return !ti.EOF()
}

// SeekToLastReverse positions the iterator at the last element in reverse order (i.e., the minimum)
func (ti *iterator) SeekToLastReverse() bool {
	ti.seekToExtreme(false)
	return !ti.EOF()
}

// SeekReverse positions the iterator at the specified key in reverse order
func (ti *iterator) SeekReverse(key []byte) bool {
	return ti.SeekWithOperationReverse(key, OP_LE)
}

// SeekWithOperationReverse positions the iterator based on the specified operation in reverse order
func (ti *iterator) SeekWithOperationReverse(key []byte, operation string) bool {
	ti.reverse = true
	if operation == OP_START {
		ti.seekToExtreme(true)
		return !ti.EOF()
	} else if operation == OP_END {
		ti.seekToExtreme(false)
		return !ti.EOF()
	}
	return ti.seekWithOperation(key, ti.getReverseOperation(operation))
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

// Helper to reconstruct the stack from root to a given node
func (ti *iterator) buildStackToNode(target *Node) {
	ti.levels = nil
	ti.levelIdx = 0
	cur := ti.tree.root
	if cur == nil || target == nil {
		ti.nextNode = nil
		ti.eof = true
		return
	}
	for !cur.IsLeaf() {
		in := cur.innerNode
		var idx int
		var found bool
		switch cur.Type() {
		case Node4, Node16:
			for i := 0; i < in.size; i++ {
				if in.children[i] == nil {
					continue
				}
				if in.children[i] == target || isDescendant(in.children[i], target) {
					idx = i
					found = true
					break
				}
			}
		case Node48:
			for i := 0; i < len(in.keys); i++ {
				ix := in.keys[byte(i)]
				if ix > 0 {
					child := in.children[ix-1]
					if child == nil {
						continue
					}
					if child == target || isDescendant(child, target) {
						idx = i
						found = true
						break
					}
				}
			}
		case Node256:
			for i := 0; i < len(in.children); i++ {
				child := in.children[i]
				if child == nil {
					continue
				}
				if child == target || isDescendant(child, target) {
					idx = i
					found = true
					break
				}
			}
		}
		if !found {
			break
		}
		ti.levels = append(ti.levels, &level{cur, idx})
		cur = getChildByIndex(cur, idx)
	}
	ti.levels = append(ti.levels, &level{target, nullIdx})
	ti.levelIdx = len(ti.levels) - 1
	// Set nextNode to the target
	ti.nextNode = target
	ti.eof = false
}

// Helper to check if descendant
func isDescendant(node, target *Node) bool {
	if node == nil {
		return false
	}
	if node == target {
		return true
	}
	if node.IsLeaf() {
		return false
	}
	in := node.innerNode
	for _, child := range in.children {
		if isDescendant(child, target) {
			return true
		}
	}
	return false
}

// Helper to get child by index for a node
func getChildByIndex(node *Node, idx int) *Node {
	if node == nil || node.IsLeaf() {
		return nil
	}
	in := node.innerNode
	switch node.Type() {
	case Node4, Node16:
		if idx >= 0 && idx < in.size {
			return in.children[idx]
		}
	case Node48:
		if idx >= 0 && idx < len(in.keys) {
			ix := in.keys[byte(idx)]
			if ix > 0 {
				return in.children[ix-1]
			}
		}
	case Node256:
		if idx >= 0 && idx < len(in.children) {
			return in.children[idx]
		}
	}
	return nil
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
			n := ti.findNextGreater(ti.tree.root, searchKey, 0)
			ti.buildStackToNode(n)
			return n != nil
		case SEEK_LE:
			n := ti.findPrevLess(ti.tree.root, searchKey, 0, true)
			ti.buildStackToNode(n)
			return n != nil
		case SEEK_LT:
			n := ti.findPrevLess(ti.tree.root, searchKey, 0, false)
			ti.buildStackToNode(n)
			return n != nil
		}
	} else {
		// Key found, handle based on operation
		switch operation {
		case SEEK_EQ:
			ti.buildStackToNode(targetNode)
			return true
		case SEEK_GE:
			ti.buildStackToNode(targetNode)
			return true
		case SEEK_GT:
			n := ti.findNextGreater(ti.tree.root, searchKey, 0)
			ti.buildStackToNode(n)
			return n != nil
		case SEEK_LE:
			ti.buildStackToNode(targetNode)
			return true
		case SEEK_LT:
			n := ti.findPrevLess(ti.tree.root, searchKey, 0, false)
			ti.buildStackToNode(n)
			return n != nil
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
func (ti *iterator) findPrevLess(current *Node, key []byte, depth int, allowEqual bool) *Node {
	if current == nil {
		return nil
	}

	if current.IsLeaf() {
		cmp := bytes.Compare(current.leaf.key, key)
		if cmp < 0 {
			return current
		} else if allowEqual && cmp == 0 {
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
	result := ti.findPrevLess(prevChild, key, depth+1, allowEqual)
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
