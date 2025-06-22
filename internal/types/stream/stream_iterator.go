package stream

import (
	"math"

	"github.com/dicedb/dice/internal/rax"
)

type streamIterator struct {
	stream      *Stream
	raxIterator rax.Iterator
	rax         *rax.Tree
	rev         bool
}

func NewStreamIterator(s *Stream, rev bool) *streamIterator {
	return &streamIterator{
		stream:      s,
		rax:         s.rax,
		raxIterator: s.rax.Iterator(),
		rev:         rev,
	}
}

func (s *streamIterator) HasNext() bool {
	return s.raxIterator.HasNext()
}

func (s *streamIterator) Next() (*StreamID, []byte) {
	next := s.raxIterator.Next()
	return s.stream.Decode(next.Key()), next.Value().([]byte)
}

func (s *streamIterator) Init(start *StreamID, end *StreamID) {
	if start == nil {
		start = &StreamID{ms: 0, seq: 0}
	}
	if end == nil {
		end = &StreamID{ms: math.MaxUint64, seq: math.MaxUint64}
	}

	if !s.rev {
		if start != nil && (start.ms != 0 || start.seq != 0) {
			s.raxIterator.SeekWithOperation(start.Encode(), rax.OP_LE)
			if s.raxIterator.EOF() {
				s.raxIterator.SeekToFirst()
			}
		} else {
			s.raxIterator.SeekToFirst()
		}
	} else {
		if end != nil && (end.ms != 0 || end.seq != 0) {
			s.raxIterator.SeekWithOperation(end.Encode(), rax.OP_LE)
			if s.raxIterator.EOF() {
				s.raxIterator.SeekToLast()
			}
		} else {
			s.raxIterator.SeekToLast()
		}
	}
}
