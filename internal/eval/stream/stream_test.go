package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamEncode(t *testing.T) {
	stream := New()
	id := stream.NextID()
	t.Logf("encoded: %v", id)
	encoded := stream.encodeStramId(id)
	t.Logf("encoded: %v", encoded)
	decoded := id.decodeStramId(encoded)
	t.Logf("decoded: %v", decoded)
	assert.Equal(t, id.ms, decoded.ms)
	assert.Equal(t, id.seq, decoded.seq)
}
