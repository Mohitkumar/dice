package cmd

import (
	"math"
	"strconv"
	"strings"

	"github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/shardmanager"
	dsstore "github.com/dicedb/dice/internal/store"
	"github.com/dicedb/dice/internal/types/stream"
	"github.com/dicedb/dicedb-go/wire"
)

var cXRANGE = &CommandMeta{
	Name:      "XRANGE",
	Syntax:    "XRANGE key start end [COUNT count]",
	HelpShort: "Returns the stream entries matching a given range of IDs. ",
	HelpLong: `
		The command returns the stream entries matching a given range of IDs. 
		The range is specified by a minimum and maximum ID. 
		All the entries having an ID between the two specified or exactly one of the two IDs specified (closed interval) are returned.
		The - and + special IDs mean respectively the minimum ID possible and the maximum ID possible inside a stream.
	`,
	Examples: `
localhost:7379> XRANGE somestream - +
1) 1) 1526985054069-0
   2) 1) "duration"
      2) "72"
      3) "event-id"
      4) "9"
      5) "user-id"
      6) "839248"
2) 1) 1526985069902-0
   2) 1) "duration"
      2) "415"
      3) "event-id"
      4) "2"
      5) "user-id"
      6) "772213"
... other entries here ...
localhost:7379> XRANGE somestream 1526985054069-0 1526985054170-0 COUNT 1
1) 1) 1526985054069-0
   2) 1) "duration"
      2) "72"
      3) "event-id"
      4) "9"
      5) "user-id"
      6) "839248"
`,
	Eval:    evalXRANGE,
	Execute: executeXRANGE,
}

func init() {
	CommandRegistry.AddCommand(cXRANGE)
}

func newXRANGERes(ids []string, values [][]string) *CmdRes {
	entries := make([]*wire.StreamEntry, len(ids))
	for i, id := range ids {
		vals := values[i]
		fileds := make([]*wire.StreamEntryField, len(values)/2)
		for i := 0; i < len(vals)/2; i += 2 {
			fileds[i/2] = &wire.StreamEntryField{
				Key:   vals[i],
				Value: vals[i+1],
			}

		}
		entry := &wire.StreamEntry{
			Id:     id,
			Fields: fileds,
		}
		entries[i] = entry
	}

	return &CmdRes{
		Rs: &wire.Result{
			Message: "OK",
			Status:  wire.Status_OK,
			Response: &wire.Result_XRANGERes{
				XRANGERes: &wire.XRANGERes{
					Entries: entries,
				},
			},
		},
	}
}

var (
	XRANGEResNilRes = newXRANGERes(make([]string, 0), make([][]string, 0))
)

func evalRANGE(c *Cmd, s *dsstore.Store) (*CmdRes, error) {
	if len(c.C.Args) < 4 {
		return XRANGEResNilRes, errors.ErrWrongArgumentCount("XRANGE")
	}
	key := c.C.Args[0]
	startId, _, err, exclude := stream.ParseStreamIdXRANGE(c.C.Args[1], 0)
	if err != nil {
		return XRANGEResNilRes, err
	}
	if exclude {
		err := startId.Incr()
		if err != nil {
			return XRANGEResNilRes, errors.NewErr("invalid start ID for the interval")
		}
	}
	endId, _, err, exclude := stream.ParseStreamIdXRANGE(c.C.Args[2], math.MaxUint64)
	if exclude {
		err := endId.Decr()
		if err != nil {
			return XRANGEResNilRes, errors.NewErr("invalid end ID for the interval")
		}
	}
	if err != nil {
		return XRANGEResNilRes, err
	}
	count := -1
	if len(c.C.Args) >= 4 {
		for j := 4; j < len(c.C.Args); j++ {
			additional := len(c.C.Args) - j - 1
			if strings.EqualFold(c.C.Args[j], "COUNT") && additional >= 1 {
				count, err := strconv.Atoi(c.C.Args[j+1])
				if err != nil {
					return XRANGEResNilRes, errors.ErrInvalidSyntax("XRANGE")
				}
				if count < 0 {
					count = 0
				}
				j++
			} else {
				return XRANGEResNilRes, errors.ErrInvalidSyntax("XRANGE")
			}
		}
	}
	if count == 0 {
		return XRANGEResNilRes, nil
	} else {
		if count == -1 {
			count = 0
		}
		stream, err := getStream(s, key)
		if err != nil {
			return XRANGEResNilRes, errors.ErrInvalidSyntax("XRANGE")
		}
	}

}

func executeXRANGE(c *Cmd, sm *shardmanager.ShardManager) (*CmdRes, error) {
	if len(c.C.Args) < 3 {
		return XADDResNilRes, errors.ErrWrongArgumentCount("XRANGE")
	}

	shard := sm.GetShardForKey(c.C.Args[0])
	return evalRANGE(c, shard.Thread.Store())
}
