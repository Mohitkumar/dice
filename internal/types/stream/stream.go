package stream

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/dicedb/dice/internal/object"

	"github.com/arriqaaq/art"
	diceerrors "github.com/dicedb/dice/internal/errors"
)

type Stream struct {
	rax             *art.Tree
	length          uint64
	firstId         *StreamID
	lastId          *StreamID
	maxDeletedEntry *StreamID
	totalEntries    uint64
	cgroups         *art.Tree
}

type streamIterator struct {
	stream      *Stream
	raxIterator *art.Iterator
}

type streamCG struct {
	lastId    StreamID
	pel       *art.Node
	consumers *art.Tree
}

type streamConsumer struct {
	name       string
	pel        *art.Node
	seenTime   uint64
	activeTime uint64
}

type streamNACK struct {
	deliveryTime  uint64
	deliveryCount uint64
	consumner     *streamConsumer
}

func New() *Stream {
	return &Stream{
		rax:             art.NewTree(),
		length:          0,
		firstId:         &StreamID{ms: 0, seq: 0},
		lastId:          &StreamID{ms: 0, seq: 0},
		maxDeletedEntry: &StreamID{ms: 0, seq: 0},
		totalEntries:    0,
	}
}

func FromObject(obj *object.Obj) (value *Stream, err []byte) {
	if err := object.AssertType(obj.Type, object.ObjTypeStream); err != nil {
		return nil, err
	}
	value, ok := obj.Value.(*Stream)
	if !ok {
		return nil, diceerrors.NewErrWithMessage("Invalid stream object")
	}
	return value, nil
}

func (s *Stream) Length() uint64 {
	return s.length
}

func (s *Stream) encodeStramId(sId *StreamID) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, sId.ms)
	binary.BigEndian.PutUint64(b[8:], sId.seq)
	return b
}

func (s *StreamID) decodeStramId(b []byte) *StreamID {
	return &StreamID{
		ms:  binary.BigEndian.Uint64(b),
		seq: binary.BigEndian.Uint64(b[8:]),
	}
}

func (s *Stream) Append(value []string) (*StreamID, error) {
	id, err := s.NextID()
	s.rax.Insert(s.encodeStramId(id), value)
	return id, err
}

func (s *Stream) AppendWithId(id *StreamID, seqGiven bool, value []string) (*StreamID, error) {
	var insertId *StreamID
	if seqGiven {
		insertId = id
	} else {
		/* The automatically generated sequence can be either zero (new
		 * timestamps) or the incremented sequence of the last ID. In the
		 * latter case, we need to prevent an overflow/advancing forward
		 * in time. */
		if s.lastId.ms == id.ms {
			if s.lastId.seq == math.MaxUint64 {
				return nil, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
			}
			insertId = s.lastId
			insertId.seq++
		} else {
			insertId = id
		}
	}
	s.rax.Insert(s.encodeStramId(insertId), value)
	return insertId, nil
}

func (s *Stream) NextID() (*StreamID, error) {
	ms := uint64(time.Now().UnixMilli())
	if ms > s.lastId.ms {
		return &StreamID{ms: ms, seq: 0}, nil
	} else {
		sid := &StreamID{ms: s.lastId.ms, seq: s.lastId.seq}
		err := sid.incr()
		return sid, err
	}
}

func (s *Stream) IsExhasusted() bool {
	return s.lastId.ms == math.MaxUint64 && s.lastId.seq == math.MaxUint64
}
