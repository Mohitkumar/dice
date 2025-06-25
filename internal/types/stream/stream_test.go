package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamEncode(t *testing.T) {
	stream := New()
	id, err := stream.NextID()
	assert.NoError(t, err)
	t.Logf("encoded: %v", id)
	encoded := id.Encode()
	t.Logf("encoded: %v", encoded)
	decoded := stream.Decode(encoded)
	t.Logf("decoded: %v", decoded)
	assert.Equal(t, id.ms, decoded.ms)
	assert.Equal(t, id.seq, decoded.seq)
}
