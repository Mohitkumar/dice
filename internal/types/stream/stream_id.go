package stream

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
)

type StreamID struct {
	ms  uint64
	seq uint64
}

func ParseStreamId(id string, strict bool, missingSeq uint64) (*StreamID, bool, error) {
	seqGiven := true
	if len(id) > 127 {
		return nil, seqGiven, errors.New("Invalid stream ID specified as stream command argument")
	}
	if strict && len(id) == 1 && (id[0] == '-' || id[0] == '+') {
		return nil, seqGiven, errors.New("Invalid stream ID specified as stream command argument")
	}
	if len(id) == 1 && id[0] == '-' {
		return &StreamID{ms: 0, seq: 0}, false, nil
	} else if len(id) == 1 && id[0] == '+' {
		return &StreamID{ms: math.MaxUint64, seq: math.MaxUint64}, seqGiven, nil
	}
	var ms uint64
	var seq uint64
	var err error

	if strings.Contains(id, "-") {
		msPart := id[0:strings.Index(id, "-")]
		ms, err = strconv.ParseUint(msPart, 10, 64)
		if err != nil {
			return nil, seqGiven, errors.New("Invalid stream ID specified as stream command argument")
		}
		seqPart := id[strings.Index(id, "-")+1:]
		if len(seqPart) == 1 && seqPart == "*" {
			/* Handle the <ms>-* form. */
			seq = 0
			seqGiven = false
		} else {
			seq, err = strconv.ParseUint(seqPart, 10, 64)
			seqGiven = true
			if err != nil {
				return nil, seqGiven, errors.New("Invalid stream ID specified as stream command argument")
			}
		}
	} else {
		ms, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, seqGiven, errors.New("Invalid stream ID specified as stream command argument")
		}
		seq = missingSeq
	}
	return &StreamID{ms: ms, seq: seq}, seqGiven, nil
}

func ParseStreamIdNoStrict(id string, missingSeq uint64) (*StreamID, bool, error) {
	return ParseStreamId(id, false, missingSeq)
}

func ParseStreamIdStrict(id string, missingSeq uint64) (*StreamID, bool, error) {
	return ParseStreamId(id, true, missingSeq)
}

func ParseStreamIdXRANGE(id string, missingSeq uint64) (*StreamID, bool, error, bool) {
	exclude := len(id) > 1 && id[0] == '('
	if exclude {
		id = id[1:]
		streamId, seqGiven, err := ParseStreamIdStrict(id, missingSeq)
		return streamId, seqGiven, err, exclude
	} else {
		streamId, seqGiven, err := ParseStreamIdNoStrict(id, missingSeq)
		return streamId, seqGiven, err, exclude
	}
}

func (s *StreamID) Incr() error {
	if s.seq == math.MaxUint64 {
		if s.ms == math.MaxUint64 {
			s.ms = 0
			s.seq = 0
			return errors.New("streamID overflow")
		} else {
			s.ms++
			s.seq = 0
		}
	} else {
		s.seq++
	}
	return nil
}

func (s *StreamID) Decr() error {
	if s.seq == 0 {
		if s.ms == 0 {
			return errors.New("streamID underflow")
		} else {
			s.ms--
			s.seq = math.MaxUint64
		}
	} else {
		s.seq--
	}
	return nil
}

func (s *StreamID) IsZero() bool {
	return s.ms == 0 && s.seq == 0
}

func (s *StreamID) String() string {
	return strconv.FormatUint(s.ms, 10) + "-" + strconv.FormatUint(s.seq, 10)
}

func (s *StreamID) Encode() []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, s.ms)
	binary.BigEndian.PutUint64(b[8:], s.seq)
	return b
}
