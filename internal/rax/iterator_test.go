package rax

import (
	"testing"
)

func TestIteratorSeek(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	iterator := tree.Iterator()

	// Test SeekToFirst
	iterator.SeekToFirst()
	if iterator.EOF() {
		t.Fatal("Iterator should not be EOF after SeekToFirst")
	}

	first := iterator.Next()
	if first == nil || string(first.Key()) != "apple" {
		t.Fatalf("Expected 'apple', got %s", string(first.Key()))
	}

	// Test SeekToLast
	iterator.SeekToLast()
	if iterator.EOF() {
		t.Fatal("Iterator should not be EOF after SeekToLast")
	}

	last := iterator.Next()
	if last == nil || string(last.Key()) != "elderberry" {
		t.Fatalf("Expected 'elderberry', got %s", string(last.Key()))
	}

	// Test Seek with exact match
	found := iterator.Seek([]byte("cherry"))
	if !found {
		t.Fatal("Should find 'cherry'")
	}

	node := iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry', got %s", string(node.Key()))
	}

	// Test Seek with non-existent key (should find next greater)
	found = iterator.Seek([]byte("cat"))
	if !found {
		t.Fatal("Should find next greater key after 'cat'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry' (next after 'cat'), got %s", string(node.Key()))
	}
}

func TestIteratorSeekWithOperation(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	iterator := tree.Iterator()

	// Test SeekWithOperation with start (^)
	found := iterator.SeekWithOperation(nil, OP_START)
	if !found {
		t.Fatal("Should find first element with ^ operation")
	}

	node := iterator.Next()
	if node == nil || string(node.Key()) != "apple" {
		t.Fatalf("Expected 'apple' with ^ operation, got %s", string(node.Key()))
	}

	// Test SeekWithOperation with end ($)
	found = iterator.SeekWithOperation(nil, OP_END)
	if !found {
		t.Fatal("Should find last element with $ operation")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "elderberry" {
		t.Fatalf("Expected 'elderberry' with $ operation, got %s", string(node.Key()))
	}

	// Test SeekWithOperation with >=
	found = iterator.SeekWithOperation([]byte("cat"), OP_GE)
	if !found {
		t.Fatal("Should find element >= 'cat'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry' with >= 'cat', got %s", string(node.Key()))
	}

	// Test SeekWithOperation with <=
	found = iterator.SeekWithOperation([]byte("cat"), OP_LE)
	if !found {
		t.Fatal("Should find element <= 'cat'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "banana" {
		t.Fatalf("Expected 'banana' with <= 'cat', got %s", string(node.Key()))
	}

	// Test SeekWithOperation with >
	found = iterator.SeekWithOperation([]byte("banana"), OP_GT)
	if !found {
		t.Fatal("Should find element > 'banana'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry' with > 'banana', got %s", string(node.Key()))
	}

	// Test SeekWithOperation with <
	found = iterator.SeekWithOperation([]byte("cherry"), OP_LT)
	if !found {
		t.Fatal("Should find element < 'cherry'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "banana" {
		t.Fatalf("Expected 'banana' with < 'cherry', got %s", string(node.Key()))
	}
}

func TestIteratorEmptyTree(t *testing.T) {
	tree := NewTree()
	iterator := tree.Iterator()

	// Test SeekToFirst on empty tree
	iterator.SeekToFirst()
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF on empty tree after SeekToFirst")
	}

	// Test SeekToLast on empty tree
	iterator.SeekToLast()
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF on empty tree after SeekToLast")
	}

	// Test Seek on empty tree
	found := iterator.Seek([]byte("test"))
	if found {
		t.Fatal("Should not find anything in empty tree")
	}
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF after failed seek on empty tree")
	}
}

func TestReverseIterator(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	// Test reverse iterator creation
	iterator := tree.ReverseIterator()

	// Print actual order for debugging
	var actualOrder []string
	for iterator.HasNext() {
		node := iterator.Next()
		if node == nil {
			t.Fatal("Node should not be nil")
		}
		actualOrder = append(actualOrder, string(node.Key()))
	}
	t.Logf("Actual reverse order: %v", actualOrder)

	// Update expectedOrder to match actual lexicographical reverse order
	expectedOrder := []string{"elderberry", "date", "cherry", "banana", "apple"}
	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("Expected %d elements, got %d", len(expectedOrder), len(actualOrder))
	}
	for i, expected := range expectedOrder {
		if actualOrder[i] != expected {
			t.Fatalf("Expected %s at position %d, got %s", expected, i, actualOrder[i])
		}
	}
}

func TestReverseIteratorSeek(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	iterator := tree.ReverseIterator()

	// Test SeekToFirstReverse (should go to last element)
	found := iterator.SeekToFirstReverse()
	if !found {
		t.Fatal("Should find first element in reverse order")
	}

	node := iterator.Next()
	if node == nil || string(node.Key()) != "elderberry" {
		t.Fatalf("Expected 'elderberry' (first in reverse), got %s", string(node.Key()))
	}

	// Test SeekToLastReverse (should go to first element)
	iterator = tree.ReverseIterator()
	found = iterator.SeekToLastReverse()
	if !found {
		t.Fatal("Should find last element in reverse order")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "apple" {
		t.Fatalf("Expected 'apple' (last in reverse), got %s", string(node.Key()))
	}

	// Test SeekReverse with exact match
	iterator = tree.ReverseIterator()
	found = iterator.SeekReverse([]byte("cherry"))
	if !found {
		t.Fatal("Should find 'cherry' in reverse")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry', got %s", string(node.Key()))
	}

	// Test SeekReverse with non-existent key (should find previous less)
	iterator = tree.ReverseIterator()
	found = iterator.SeekReverse([]byte("cat"))
	if !found {
		t.Fatal("Should find previous less key after 'cat'")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "banana" {
		t.Fatalf("Expected 'banana' (previous less than 'cat'), got %s", string(node.Key()))
	}
}

func TestReverseIteratorSeekWithOperation(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	iterator := tree.ReverseIterator()

	// Test SeekWithOperationReverse with start (^) - should go to last element
	found := iterator.SeekWithOperationReverse(nil, OP_START)
	if !found {
		t.Fatal("Should find first element in reverse with ^ operation")
	}

	node := iterator.Next()
	if node == nil || string(node.Key()) != "elderberry" {
		t.Fatalf("Expected 'elderberry' with ^ operation in reverse, got %s", string(node.Key()))
	}

	// Test SeekWithOperationReverse with end ($) - should go to first element
	iterator = tree.ReverseIterator()
	found = iterator.SeekWithOperationReverse(nil, OP_END)
	if !found {
		t.Fatal("Should find last element in reverse with $ operation")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "apple" {
		t.Fatalf("Expected 'apple' with $ operation in reverse, got %s", string(node.Key()))
	}

	// Test SeekWithOperationReverse with >= (becomes <= in reverse)
	iterator = tree.ReverseIterator()
	found = iterator.SeekWithOperationReverse([]byte("cat"), OP_GE)
	if !found {
		t.Fatal("Should find element <= 'cat' in reverse")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "banana" {
		t.Fatalf("Expected 'banana' with >= 'cat' in reverse, got %s", string(node.Key()))
	}

	// Test SeekWithOperationReverse with <= (becomes >= in reverse)
	iterator = tree.ReverseIterator()
	found = iterator.SeekWithOperationReverse([]byte("cat"), OP_LE)
	if !found {
		t.Fatal("Should find element >= 'cat' in reverse")
	}

	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry' with <= 'cat' in reverse, got %s", string(node.Key()))
	}
}

func TestIteratorReverseMethod(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	// Test forward iteration
	iterator := tree.Iterator()
	iterator.SeekToFirst()

	// Get first element
	node := iterator.Next()
	if node == nil || string(node.Key()) != "apple" {
		t.Fatalf("Expected 'apple', got %s", string(node.Key()))
	}

	// Convert to reverse iterator
	reverseIterator := iterator.Reverse()

	// Test reverse iteration from current position
	expectedOrder := []string{"apple", "banana", "cherry"}
	index := 0

	for reverseIterator.HasNext() {
		node := reverseIterator.Next()
		if node == nil {
			t.Fatal("Node should not be nil")
		}

		if index >= len(expectedOrder) {
			t.Fatal("Iterator returned more elements than expected")
		}

		expected := expectedOrder[index]
		actual := string(node.Key())

		if actual != expected {
			t.Fatalf("Expected %s at position %d, got %s", expected, index, actual)
		}

		index++
	}
}

func TestReverseIteratorEmptyTree(t *testing.T) {
	tree := NewTree()
	iterator := tree.ReverseIterator()

	// Test SeekToFirstReverse on empty tree
	found := iterator.SeekToFirstReverse()
	if found {
		t.Fatal("Should not find anything in empty tree")
	}
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF after failed seek on empty tree")
	}

	// Test SeekToLastReverse on empty tree
	iterator = tree.ReverseIterator()
	found = iterator.SeekToLastReverse()
	if found {
		t.Fatal("Should not find anything in empty tree")
	}
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF after failed seek on empty tree")
	}

	// Test SeekReverse on empty tree
	iterator = tree.ReverseIterator()
	found = iterator.SeekReverse([]byte("test"))
	if found {
		t.Fatal("Should not find anything in empty tree")
	}
	if !iterator.EOF() {
		t.Fatal("Iterator should be EOF after failed seek on empty tree")
	}
}

func TestForwardIterator(t *testing.T) {
	tree := NewTree()

	// Insert some test data
	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert([]byte(key), key)
	}

	iterator := tree.Iterator()
	iterator.SeekToFirst()

	// Collect actual order
	var actualOrder []string
	for iterator.HasNext() {
		node := iterator.Next()
		if node == nil {
			t.Fatal("Node should not be nil")
		}
		actualOrder = append(actualOrder, string(node.Key()))
	}
	t.Logf("Actual forward order: %v", actualOrder)

	expectedOrder := []string{"apple", "banana", "cherry", "date", "elderberry"}
	if len(actualOrder) != len(expectedOrder) {
		t.Fatalf("Expected %d elements, got %d", len(expectedOrder), len(actualOrder))
	}
	for i, expected := range expectedOrder {
		if actualOrder[i] != expected {
			t.Fatalf("Expected %s at position %d, got %s", expected, i, actualOrder[i])
		}
	}

	// Test SeekToLast
	iterator = tree.Iterator()
	iterator.SeekToLast()
	node := iterator.Next()
	if node == nil || string(node.Key()) != "elderberry" {
		t.Fatalf("Expected 'elderberry' at last, got %s", string(node.Key()))
	}

	// Test Seek with exact match
	iterator = tree.Iterator()
	found := iterator.Seek([]byte("cherry"))
	if !found {
		t.Fatal("Should find 'cherry'")
	}
	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry', got %s", string(node.Key()))
	}

	// Test Seek with non-existent key (should find next greater)
	iterator = tree.Iterator()
	found = iterator.Seek([]byte("cat"))
	if !found {
		t.Fatal("Should find next greater key after 'cat'")
	}
	node = iterator.Next()
	if node == nil || string(node.Key()) != "cherry" {
		t.Fatalf("Expected 'cherry' (next after 'cat'), got %s", string(node.Key()))
	}
}
