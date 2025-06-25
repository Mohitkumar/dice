package stream

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/dicedb/dice/internal/object"

	diceerrors "github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/rax"
)

type Stream struct {
	rax             *rax.Tree
	length          uint64
	firstId         *StreamID
	lastId          *StreamID
	maxDeletedEntry *StreamID
	totalEntries    uint64
	cgroups         *rax.Tree
}

type streamCG struct {
	lastId    StreamID
	pel       *rax.Tree
	consumers *rax.Tree
}

type streamConsumer struct {
	name       string
	pel        *rax.Tree
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
		rax:             rax.NewTree(),
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

func (s *Stream) Append(value []string) (*StreamID, error) {
	if s.IsExhasusted() {
		return nil, errors.New("The stream has exhausted the last possible ID,unable to add more items")
	}
	id, err := s.NextID()
	s.rax.Insert(id.Encode(), value)
	s.lastId = id
	return id, err
}

func (s *Stream) AppendWithId(id *StreamID, seqGiven bool, value []string) (*StreamID, error) {
	if s.IsExhasusted() {
		return nil, errors.New("The stream has exhausted the last possible ID,unable to add more items")
	}
	if id.IsZero() {
		return nil, errors.New("The ID specified in XADD must be greater than 0-0")
	}
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
	if insertId.Compare(s.lastId) <= 0 {
		return nil, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
	}
	s.rax.Insert(insertId.Encode(), value)
	s.lastId = insertId
	return insertId, nil
}

func (s *Stream) Decode(b []byte) *StreamID {
	return &StreamID{
		ms:  binary.BigEndian.Uint64(b),
		seq: binary.BigEndian.Uint64(b[8:]),
	}
}
func (s *Stream) NextID() (*StreamID, error) {
	ms := uint64(time.Now().UnixMilli())
	if ms > s.lastId.ms {
		return &StreamID{ms: ms, seq: 0}, nil
	} else {
		sid := &StreamID{ms: s.lastId.ms, seq: s.lastId.seq}
		err := sid.Incr()
		return sid, err
	}
}

func (s *Stream) IsExhasusted() bool {
	return s.lastId.ms == math.MaxUint64 && s.lastId.seq == math.MaxUint64
}

func (s *Stream) Range(start *StreamID, end *StreamID, cg *streamCG, consumer *streamConsumer, rev bool) ([]string, [][]string) {
	iterator := NewStreamIterator(s, rev)
	iterator.Init(start, end)
	ids := make([]string, 0)
	values := make([][]string, 0)
	for iterator.HasNext() {
		id, value := iterator.Next()
		ids = append(ids, string(id.Encode()))
		values = append(values, value)
	}
	return ids, values
}
