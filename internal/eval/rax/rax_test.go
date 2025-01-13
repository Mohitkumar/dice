package rax

import (
	"strconv"
	"testing"
	"time"
)

func TestInterator(t *testing.T) {
	tree := NewTree()

	tree.Insert([]byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), "xx")
	time.Sleep(1 * time.Second)
	tree.Insert([]byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), "world")
	time.Sleep(1 * time.Second)
	tree.Insert([]byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), "there")
	time.Sleep(1 * time.Second)
	tree.Insert([]byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), "there")
	time_str := strconv.FormatInt(time.Now().UnixMilli(), 10)
	tree.Insert([]byte(time_str+"-1"), "hello")
	tree.Insert([]byte(time_str+"-0"), "hello")
	tree.Insert([]byte(time_str+"-2"), "hello")
	t.Log("testig....")
	it := tree.Iterator()
	for it.HasNext() {
		n := it.Next()
		if n.IsLeaf() {
			t.Log(string(n.Key()))
		}
	}
}
