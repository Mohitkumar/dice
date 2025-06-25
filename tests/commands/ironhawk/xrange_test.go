package ironhawk

import (
	"errors"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
)

func extractValueXRANGE(res *wire.Result) interface{} {
	if res.GetXRANGERes() != nil {
		return len(res.GetXRANGERes().Entries)
	}
	return 0
}

func TestXRange(t *testing.T) {
	client := getLocalConnection()
	defer client.Close()

	testCases := []TestCase{
		{
			name:     "Call XRANGE with no arguments",
			commands: []string{"XRANGE"},
			expected: []interface{}{
				errors.New("wrong number of arguments for 'XRANGE' command"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name: "XRANGE with - and +",
			commands: []string{
				`XADD mystream 1-0 k v`,
				`XADD mystream 2-0 k v`,
				`XRANGE mystream - +`,
			},
			expected: []interface{}{
				"not empty", "not empty", 2,
			},
			valueExtractor: []ValueExtractorFn{extractValueXADD, extractValueXADD, extractValueXRANGE},
		},
		{
			name: "XRANGE with specific range",
			commands: []string{
				`XRANGE mystream 2-0 4-0`,
			},
			expected: []interface{}{
				3, // Expect 3 entries (2-0, 3-0, 4-0)
			},
			valueExtractor: []ValueExtractorFn{extractValueXRANGE},
		},
		{
			name: "XRANGE with COUNT",
			commands: []string{
				`XRANGE mystream - + COUNT 2`,
			},
			expected: []interface{}{
				2, // Expect 2 entries
			},
			valueExtractor: []ValueExtractorFn{extractValueXRANGE},
		},
		{
			name: "XRANGE on non-existent key",
			commands: []string{
				`XRANGE non-existent-stream - +`,
			},
			expected: []interface{}{
				0, // Expect 0 entries
			},
			valueExtractor: []ValueExtractorFn{extractValueXRANGE},
		},
		{
			name: "XRANGE with invalid range",
			commands: []string{
				`XRANGE mystream + -`,
			},
			expected: []interface{}{
				0, // Expect 0 entries
			},
			valueExtractor: []ValueExtractorFn{extractValueXRANGE},
		},
	}

	// Since some tests depend on the state from previous tests,
	// they are run sequentially here.
	for _, tc := range testCases {
		// Create a new slice with the single test case
		singleTestCase := []TestCase{tc}
		// Run the test case
		runTestcases(t, client, singleTestCase)
	}
}
