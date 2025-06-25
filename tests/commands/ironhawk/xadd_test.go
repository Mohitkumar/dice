package ironhawk

import (
	"errors"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
)

func extractValueXADD(res *wire.Result) interface{} {
	if res.GetXADDRes() != nil {
		id := res.GetXADDRes().Id
		if len(id) > 0 {
			if id == "12345-0" {
				return id
			}
			return "not empty"
		}
	}
	return ""
}

func extractValueXADDExact(res *wire.Result) interface{} {
	if res.GetXADDRes() != nil {
		id := res.GetXADDRes().Id
		if len(id) > 0 {
			return id
		}
	}
	return ""
}

func TestXAdd(t *testing.T) {
	client := getLocalConnection()
	defer client.Close()

	testCases := []TestCase{
		{
			name:     "Call XADD with no arguments",
			commands: []string{"XADD"},
			expected: []interface{}{
				errors.New("wrong number of arguments for 'XADD' command"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name:     "Call XADD with just the key",
			commands: []string{"XADD mystream"},
			expected: []interface{}{
				errors.New("wrong number of arguments for 'XADD' command"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name:     "Call XADD with key and id but no fields",
			commands: []string{"XADD mystream *"},
			expected: []interface{}{
				errors.New("wrong number of arguments for 'XADD' command"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name: "XADD with min id",
			commands: []string{
				`XADD mystream 0-0 name John age 30`,
			},
			expected: []interface{}{
				errors.New("The ID specified in XADD must be greater than 0-0"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name: "Simple XADD",
			commands: []string{
				`XADD mystream * name John age 30`,
			},
			expected: []interface{}{
				"not empty",
			},
			valueExtractor: []ValueExtractorFn{extractValueXADD},
		},
		{
			name: "XADD NOMKSTREAM",
			commands: []string{
				`XADD ss NOMKSTREAM * name John age 30`,
			},
			expected: []interface{}{
				errors.New("stream does not exist"),
			},
			valueExtractor: []ValueExtractorFn{nil},
		},
		{
			name: "XADD with specific ID",
			commands: []string{
				`XADD mystream 12345-0 name Jane age 25`,
			},
			expected: []interface{}{
				"12345-0",
			},
			valueExtractor: []ValueExtractorFn{extractValueXADD},
		},
		{
			name: "XADD with specific ID 2",
			commands: []string{
				`XADD mystream 1-0 k v`,
			},
			expected: []interface{}{
				"1-0",
			},
			valueExtractor: []ValueExtractorFn{extractValueXADDExact},
		},
		{
			name: "XADD with duplicate ID",
			commands: []string{
				`XADD mystream 12345-0 name Jane age 25`,
				`XADD mystream 12345-0 name Jane age 25`,
			},
			expected: []interface{}{
				"12345-0",
				errors.New("The ID specified in XADD is equal or smaller than the target stream top item"),
			},
			valueExtractor: []ValueExtractorFn{extractValueXADD, nil},
		},
		{
			name: "XADD with max id",
			commands: []string{
				`XADD mystream 18446744073709551615-18446744073709551615 name Jane age 25`,
				`XADD mystream * name Jane age 25`,
			},
			expected: []interface{}{
				"18446744073709551615-18446744073709551615",
				errors.New("The stream has exhausted the last possible ID,unable to add more items"),
			},
			valueExtractor: []ValueExtractorFn{extractValueXADDExact, nil},
		},
		{
			name: "XADD with max seq less 1",
			commands: []string{
				`XADD mystream 18446744073709551615-18446744073709551614 name Jane age 25`,
				`XADD mystream * name Jane age 25`,
			},
			expected: []interface{}{
				"18446744073709551615-18446744073709551614",
				"18446744073709551615-18446744073709551615",
			},
			valueExtractor: []ValueExtractorFn{extractValueXADDExact, extractValueXADDExact},
		},
		{
			name: "XADD with xxx-*",
			commands: []string{
				`XADD mystream 12345-* name Jane age 25`,
			},
			expected: []interface{}{
				"12345-0",
			},
			valueExtractor: []ValueExtractorFn{extractValueXADDExact},
		}, {
			name: "XADD with xxx-*",
			commands: []string{
				`XADD mystream *-* name Jane age 25`,
			},
			expected: []interface{}{
				errors.New("Invalid stream ID specified as stream command argument"),
			},
			valueExtractor: []ValueExtractorFn{nil},
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
