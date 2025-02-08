package eval

import (
	"errors"
	"strconv"
	"strings"

	"github.com/dicedb/dice/internal/clientio"
	diceerrors "github.com/dicedb/dice/internal/errors"
	"github.com/dicedb/dice/internal/eval/stream"
	dstore "github.com/dicedb/dice/internal/store"
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

func evalXADD(args []string, store *dstore.Store) *EvalResponse {
	// if length of command is 3, throw error as it is not possible
	if len(args) < 4 {
		return &EvalResponse{
			Result: nil,
			Error:  diceerrors.ErrWrongArgumentCount("XADD"),
		}
	}
	key := args[0]
	commandOpt, fieldPos, err := validateAndParseXAddOrXTrimCommand(args[1:], true)
	if err != nil {
		return &EvalResponse{
			Result: nil,
			Error:  err,
		}
	}
	/* Check arity. */
	if (len(args)-fieldPos) < 2 || ((len(args)-fieldPos)%2) == 1 {
		return &EvalResponse{
			Result: nil,
			Error:  diceerrors.ErrWrongArgumentCount("XADD"),
		}
	}
	/* Return ASAP if minimal ID (0-0) was given so we avoid possibly creating
	 * a new stream and have streamAppendItem fail, leaving an empty key in the
	 * database. */
	if commandOpt.idGiven && commandOpt.seqGiven &&
		commandOpt.id.IsZero() {
		return &EvalResponse{
			Result: nil,
			Error:  diceerrors.ErrWrongArgumentCount("The ID specified in XADD must be greater than 0-0"),
		}
	}
	if commandOpt.noMkstream {
		_, err := getStream(store, key)
		if err != nil {
			return &EvalResponse{
				Result: clientio.NIL,
				Error:  nil,
			}
		}
	}
	st, err := getOrCreateStream(store, key)
	if err != nil {
		return &EvalResponse{
			Result: nil,
			Error:  err,
		}
	}
	if st.IsExhasusted() {
		return &EvalResponse{
			Result: nil,
			Error:  errors.New("The stream has exhausted the last possible ID,unable to add more items"),
		}
	}
	var id *stream.StreamID
	if commandOpt.idGiven {
		id, err = st.AppendWithId(commandOpt.id, commandOpt.seqGiven, args[fieldPos:])
		if err != nil {
			return &EvalResponse{
				Result: nil,
				Error:  err,
			}
		}
	} else {
		id, err = st.Append(args[fieldPos:])
		if err != nil {
			return &EvalResponse{
				Result: nil,
				Error:  err,
			}
		}
	}
	return &EvalResponse{
		Result: id.String(),
		Error:  nil,
	}
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
				return nil, 0, errors.New("syntax error, MAXLEN and MINID options at the same time are not compatible")
			}
			maxlen, err := strconv.ParseUint(args[i+1], 10, 64)
			if err != nil {
				return nil, 0, errors.New("The MAXLEN argument must be >= 0.")
			}
			commandOpt.maxlen = maxlen
			if commandOpt.maxlen < 0 {
				return nil, 0, errors.New("The MAXLEN argument must be >= 0.")
			}
			i++
			commandOpt.trimStrategy = TRIM_STRATEGY_MAXLEN
		} else if strings.EqualFold(opt, "minid") && haveMoreArgs {
			if commandOpt.trimStrategy != TRIM_STRATEGY_NONE {
				return nil, 0, errors.New("syntax error, MAXLEN and MINID options at the same time are not compatible")
			}

			streamId, seqGiven, err := stream.ParseStreamIdXADD(args[i+1], true, 0)
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
			streamId, seqGiven, err := stream.ParseStreamIdXADD(args[i+1], true, 0)
			if err != nil {
				return nil, 0, err
			}
			commandOpt.seqGiven = seqGiven
			commandOpt.id = streamId
			commandOpt.idGiven = true
			break
		} else {
			return nil, 0, errors.New("syntax error")
		}
	}
	return commandOpt, i + 1, nil
}

func getOrCreateStream(store *dstore.Store, key string) (*stream.Stream, error) {
	obj := store.Get(key)
	if obj != nil {
		stream, err := stream.FromObject(obj)
		if err != nil {
			return nil, diceerrors.ErrWrongTypeOperation
		}
		return stream, nil
	}
	return stream.New(), nil
}

func getStream(store *dstore.Store, key string) (*stream.Stream, error) {
	obj := store.Get(key)
	if obj != nil {
		stream, err := stream.FromObject(obj)
		if err != nil {
			return nil, diceerrors.ErrWrongTypeOperation
		}
		return stream, nil
	}
	return nil, errors.New("stream does not exist")
}
