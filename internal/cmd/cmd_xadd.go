// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package cmd

import (
	"strconv"
	"strings"

	"github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/shardmanager"
	dsstore "github.com/dicedb/dice/internal/store"
	"github.com/dicedb/dice/internal/types/stream"
	"github.com/dicedb/dicedb-go/wire"
)

var cXADD = &CommandMeta{
	Name:      "XADD",
	Syntax:    "XADD key [NOMKSTREAM] [<MAXLEN | MINID> [= | ~] threshold [LIMIT count]] <* | id> field value [field value ...]",
	HelpShort: "Appends the specified stream entry to the stream at the specified key.",
	HelpLong: `
Appends the specified stream entry to the stream at the specified key. 
If the key does not exist, as a side effect of running this command the key is created with a stream value. 
The creation of stream's key can be disabled with the NOMKSTREAM option.

An entry is composed of a list of field-value pairs. 
The field-value pairs are stored in the same order they are given by the user.

A stream entry ID identifies a given entry inside a stream.
The XADD command will auto-generate a unique ID for you if the ID argument specified is the '*' character.
It is also possible to specify a well-formed ID, so that the new entry will be added exactly with the specified ID.
When a user specifies an explicit ID to XADD, the minimum valid ID is 0-1, and the user must specify an ID which is greater than any other ID currently inside the stream.

XADD also supports capping the stream to a certain size using the MAXLEN or MINID options.
	`,
	Examples: `
localhost:7379> XADD mystream * name Sara surname OConnor
"1526985054082-0"
localhost:7379> XADD mystream * field1 value1 field2 value2
"1526985054082-1"
localhost:7379> XLEN mystream
(integer) 2
localhost:7379> XRANGE mystream - +
1) 1) "1526985054082-0"
   2) 1) "name"
      2) "Sara"
      3) "surname"
      4) "OConnor"
2) 1) "1526985054082-1"
   2) 1) "field1"
      2) "value1"
      3) "field2"
      4) "value2"
`,
	Eval:    evalXADD,
	Execute: executeXADD,
}

func init() {
	CommandRegistry.AddCommand(cXADD)
}

func newXADDRes(id string) *CmdRes {
	return &CmdRes{
		Rs: &wire.Result{
			Message: "OK",
			Status:  wire.Status_OK,
			Response: &wire.Result_XADDRes{
				XADDRes: &wire.XADDRes{
					Id: id,
				},
			},
		},
	}
}

var (
	XADDResNilRes = newXADDRes("")
)

type TrimStrategy int

const (
	TRIM_STRATEGY_MAXLEN TrimStrategy = iota
	TRIM_STRATEGY_MINID
	TRIM_STRATEGY_NONE
)

type streamXaddCommandOpt struct {
	id           *stream.StreamID /* User-provided ID, for XADD only. */
	idGiven      bool             /* Was an ID different than "*" specified? for XADD only. */
	seqGiven     bool             /* Was an ID different than "ms-*" specified? for XADD only. */
	noMkstream   bool             /* if set to 1 do not create new stream */
	trimStrategy TrimStrategy     /* TRIM_STRATEGY_* */
	maxlen       uint64           /* After trimming, leave stream at this length . */
	minid        *stream.StreamID /* Trim by ID (No stream entries with ID < 'minid' will remain) */
}

func evalXADD(c *Cmd, s *dsstore.Store) (*CmdRes, error) {
	if len(c.C.Args) < 4 {
		return XADDResNilRes, errors.ErrWrongArgumentCount("XADD")
	}

	key := c.C.Args[0]
	commandOpt, fieldPos, err := validateAndParseXAddOrXTrimCommand(c.C.Args[1:], true)
	if err != nil {
		return XADDResNilRes, err
	}
	/* Check arity. */
	if (len(c.C.Args)-fieldPos) < 2 || ((len(c.C.Args)-fieldPos)%2) == 1 {
		return XADDResNilRes, errors.ErrWrongArgumentCount("XADD")
	}

	/* Return ASAP if minimal ID (0-0) was given so we avoid possibly creating
	 * a new stream and have streamAppendItem fail, leaving an empty key in the
	 * database. */
	if commandOpt.idGiven && commandOpt.seqGiven && commandOpt.id.IsZero() {
		return XADDResNilRes, errors.ErrWrongArgumentCount("The ID specified in XADD must be greater than 0-0")
	}
	if commandOpt.noMkstream {
		_, err := getStream(s, key)
		return XADDResNilRes, err
	}
	st, err := getOrCreateStream(s, key)
	if err != nil {
		return XADDResNilRes, err
	}
	if st.IsExhasusted() {
		return XADDResNilRes, errors.NewErr("The stream has exhausted the last possible ID,unable to add more items")

	}
	var id *stream.StreamID
	if commandOpt.idGiven {
		id, err = st.AppendWithId(commandOpt.id, commandOpt.seqGiven, c.C.Args[fieldPos:])
		if err != nil {
			return XADDResNilRes, err
		}
	} else {
		id, err = st.Append(c.C.Args[fieldPos:])
		if err != nil {
			return XADDResNilRes, err
		}
	}
	return newXADDRes(id.String()), nil
}

func executeXADD(c *Cmd, sm *shardmanager.ShardManager) (*CmdRes, error) {
	if len(c.C.Args) < 4 {
		return XADDResNilRes, errors.ErrWrongArgumentCount("XADD")
	}

	shard := sm.GetShardForKey(c.C.Args[0])
	return evalXADD(c, shard.Thread.Store())
}

func validateAndParseXAddOrXTrimCommand(args []string, xadd bool) (commnadOpt *streamXaddCommandOpt, fieldPos int, err error) {
	i := 0
	commandOpt := &streamXaddCommandOpt{}
	commandOpt.seqGiven = true
	commandOpt.idGiven = false
	commandOpt.noMkstream = false
	commandOpt.trimStrategy = TRIM_STRATEGY_NONE
	for ; i < len(args); i++ {
		opt := args[i]
		haveMoreArgs := ((len(args) - 1) - i) > 0
		if xadd && opt == "*" {
			return commandOpt, i + 1, nil
		} else if strings.EqualFold(opt, "maxlen") && haveMoreArgs {
			if commandOpt.trimStrategy != TRIM_STRATEGY_NONE {
				return nil, 0, errors.NewErr("syntax error, MAXLEN and MINID options at the same time are not compatible")
			}
			maxlen, err := strconv.ParseUint(args[i+1], 10, 64)
			if err != nil {
				return nil, 0, errors.NewErr("The MAXLEN argument must be >= 0.")
			}
			commandOpt.maxlen = maxlen
			if commandOpt.maxlen < 0 {
				return nil, 0, errors.NewErr("The MAXLEN argument must be >= 0.")
			}
			i++
			commandOpt.trimStrategy = TRIM_STRATEGY_MAXLEN
		} else if strings.EqualFold(opt, "minid") && haveMoreArgs {
			if commandOpt.trimStrategy != TRIM_STRATEGY_NONE {
				return nil, 0, errors.NewErr("syntax error, MAXLEN and MINID options at the same time are not compatible")
			}

			streamId, seqGiven, err := stream.ParseStreamId(args[i+1], true, 0)
			if err != nil {
				return nil, 0, err
			}
			i++
			commandOpt.minid = streamId
			commandOpt.seqGiven = seqGiven
			commandOpt.trimStrategy = TRIM_STRATEGY_MINID
		} else if xadd && strings.EqualFold(opt, "nomkstream") {
			commandOpt.noMkstream = true
		} else if xadd {
			streamId, seqGiven, err := stream.ParseStreamId(args[i+1], true, 0)
			if err != nil {
				return nil, 0, err
			}
			commandOpt.seqGiven = seqGiven
			commandOpt.id = streamId
			commandOpt.idGiven = true
			break
		} else {
			return nil, 0, errors.NewErr("syntax error")
		}
	}
	return commandOpt, i + 1, nil
}

func getOrCreateStream(store *dsstore.Store, key string) (*stream.Stream, error) {
	obj := store.Get(key)
	if obj != nil {
		stream, err := stream.FromObject(obj)
		if err != nil {
			return nil, errors.ErrWrongTypeOperation
		}
		return stream, nil
	}
	return stream.New(), nil
}

func getStream(store *dsstore.Store, key string) (*stream.Stream, error) {
	obj := store.Get(key)
	if obj != nil {
		stream, err := stream.FromObject(obj)
		if err != nil {
			return nil, errors.ErrWrongTypeOperation
		}
		return stream, nil
	}
	return nil, errors.NewErr("stream does not exist")
}
