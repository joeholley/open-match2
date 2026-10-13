// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package testcases contains lists of ticket filtering test cases used to
// verify Pool filter inclusion and exclusion behavior.
package testcases

import (
	"fmt"
	"math"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCase defines a single filtering test case to run against filter.In(Pool, Ticket).
type TestCase struct {
	Name   string
	Ticket *pb.Ticket
	Pool   *pb.Pool
}

var (
	refStart = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	refMid   = time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	refEnd   = time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
)

// IncludedTestCases returns a list of test cases where using the given Pool filter,
// the Ticket must be included in the result (filter.In returns true).
func IncludedTestCases() []TestCase {
	return []TestCase{
		{
			Name:   "No filters in pool includes any ticket",
			Ticket: &pb.Ticket{},
			Pool:   &pb.Pool{},
		},

		simpleDoubleRange("simpleInRange", 5, 0, 10, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("simpleInRange", 5, 0, 10, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("simpleInRange", 5, 0, 10, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("simpleInRange", 5, 0, 10, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("exactMatch", 5, 5, 5, pb.Pool_EXCLUDE_NONE),

		simpleDoubleRange("infinityMax", math.Inf(1), 0, math.Inf(1), pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("infinityMax", math.Inf(1), 0, math.Inf(1), pb.Pool_EXCLUDE_MIN),

		simpleDoubleRange("infinityMin", math.Inf(-1), math.Inf(-1), 0, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("infinityMin", math.Inf(-1), math.Inf(-1), 0, pb.Pool_EXCLUDE_MAX),

		simpleDoubleRange("excludeNoneAtMinBoundary", 0, 0, 1, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("excludeNoneAtMaxBoundary", 1, 0, 1, pb.Pool_EXCLUDE_NONE),

		simpleDoubleRange("excludeMinAtMaxBoundary", 1, 0, 1, pb.Pool_EXCLUDE_MIN),

		simpleDoubleRange("excludeMaxAtMinBoundary", 0, 0, 1, pb.Pool_EXCLUDE_MAX),

		simpleDoubleRange("excludeBothInsideUpper", 2, 0, 3, pb.Pool_EXCLUDE_BOTH),
		simpleDoubleRange("excludeBothInsideLower", 1, 0, 3, pb.Pool_EXCLUDE_BOTH),

		// CreationTimeRangeFilter positive cases across all FilterBounds
		simpleCreationTimeRange("inRange", refMid, refStart, refEnd, pb.Pool_EXCLUDE_NONE),
		simpleCreationTimeRange("inRange", refMid, refStart, refEnd, pb.Pool_EXCLUDE_START),
		simpleCreationTimeRange("inRange", refMid, refStart, refEnd, pb.Pool_EXCLUDE_END),
		simpleCreationTimeRange("inRange", refMid, refStart, refEnd, pb.Pool_EXCLUDE_BOTH),
		simpleCreationTimeRange("atStartBoundary", refStart, refStart, refEnd, pb.Pool_EXCLUDE_NONE),
		simpleCreationTimeRange("atStartBoundary", refStart, refStart, refEnd, pb.Pool_EXCLUDE_END),
		simpleCreationTimeRange("atEndBoundary", refEnd, refStart, refEnd, pb.Pool_EXCLUDE_NONE),
		simpleCreationTimeRange("atEndBoundary", refEnd, refStart, refEnd, pb.Pool_EXCLUDE_START),

		{
			Name: "StringEquals exact match",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					StringArgs: map[string]string{
						"field": "value",
					},
				},
			},
			Pool: &pb.Pool{
				StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					{
						StringArg: "field",
						Value:     "value",
					},
				},
			},
		},

		{
			Name: "TagPresent single tag match",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{
						"mytag",
					},
				},
			},
			Pool: &pb.Pool{
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{
						Tag: "mytag",
					},
				},
			},
		},

		{
			Name: "TagPresent multiple tags all present regardless of order",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{
						"A", "B", "C",
					},
				},
			},
			Pool: &pb.Pool{
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{
						Tag: "A",
					},
					{
						Tag: "C",
					},
					{
						Tag: "B",
					},
				},
			},
		},

		multipleFilters(true, true, true, true),
	}
}

// ExcludedTestCases returns a list of test cases where using the given Pool filter,
// the Ticket must NOT be included in the result (filter.In returns false).
func ExcludedTestCases() []TestCase {
	return []TestCase{
		{
			Name:   "DoubleRange with nil Ticket",
			Ticket: nil,
			Pool: &pb.Pool{
				DoubleRangeFilters: []*pb.Pool_DoubleRangeFilter{
					{
						DoubleArg: "field",
						Minimum:   math.Inf(-1),
						Maximum:   math.Inf(1),
					},
				},
			},
		},
		{
			Name:   "StringEquals with nil Ticket",
			Ticket: nil,
			Pool: &pb.Pool{
				StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					{
						StringArg: "field",
						Value:     "value",
					},
				},
			},
		},
		{
			Name:   "TagPresent with nil Ticket",
			Ticket: nil,
			Pool: &pb.Pool{
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{
						Tag: "value",
					},
				},
			},
		},
		{
			Name:   "CreationTimeRange with nil Ticket",
			Ticket: nil,
			Pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: timestamppb.New(refStart),
					End:   timestamppb.New(refEnd),
				},
			},
		},
		{
			Name: "DoubleRange missing requested field in DoubleArgs",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					DoubleArgs: map[string]float64{
						"otherfield": 0,
					},
				},
			},
			Pool: &pb.Pool{
				DoubleRangeFilters: []*pb.Pool_DoubleRangeFilter{
					{
						DoubleArg: "field",
						Minimum:   math.Inf(-1),
						Maximum:   math.Inf(1),
					},
				},
			},
		},

		simpleDoubleRange("exactMatch", 5, 5, 5, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("exactMatch", 5, 5, 5, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("exactMatch", 5, 5, 5, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("valueTooLow", -1, 0, 10, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("valueTooLow", -1, 0, 10, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("valueTooLow", -1, 0, 10, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("valueTooLow", -1, 0, 10, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("valueTooHigh", 11, 0, 10, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("valueTooHigh", 11, 0, 10, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("valueTooHigh", 11, 0, 10, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("valueTooHigh", 11, 0, 10, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("minIsNan", 5, math.NaN(), 10, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("minIsNan", 5, math.NaN(), 10, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("minIsNan", 5, math.NaN(), 10, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("minIsNan", 5, math.NaN(), 10, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("maxIsNan", 5, 0, math.NaN(), pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("maxIsNan", 5, 0, math.NaN(), pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("maxIsNan", 5, 0, math.NaN(), pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("maxIsNan", 5, 0, math.NaN(), pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("minMaxAreNan", 5, math.NaN(), math.NaN(), pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("minMaxAreNan", 5, math.NaN(), math.NaN(), pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("minMaxAreNan", 5, math.NaN(), math.NaN(), pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("minMaxAreNan", 5, math.NaN(), math.NaN(), pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("valueIsNan", math.NaN(), 0, 10, pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("valueIsNan", math.NaN(), 0, 10, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("valueIsNan", math.NaN(), 0, 10, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("valueIsNan", math.NaN(), 0, 10, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("valueIsNanInfRange", math.NaN(), math.Inf(-1), math.Inf(1), pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("valueIsNanInfRange", math.NaN(), math.Inf(-1), math.Inf(1), pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("valueIsNanInfRange", math.NaN(), math.Inf(-1), math.Inf(1), pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("valueIsNanInfRange", math.NaN(), math.Inf(-1), math.Inf(1), pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("infinityMax", math.Inf(1), 0, math.Inf(1), pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("infinityMax", math.Inf(1), 0, math.Inf(1), pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("infinityMin", math.Inf(-1), math.Inf(-1), 0, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("infinityMin", math.Inf(-1), math.Inf(-1), 0, pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("allAreNan", math.NaN(), math.NaN(), math.NaN(), pb.Pool_EXCLUDE_NONE),
		simpleDoubleRange("allAreNan", math.NaN(), math.NaN(), math.NaN(), pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("allAreNan", math.NaN(), math.NaN(), math.NaN(), pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("allAreNan", math.NaN(), math.NaN(), math.NaN(), pb.Pool_EXCLUDE_BOTH),

		simpleDoubleRange("valueIsMax", 1, 0, 1, pb.Pool_EXCLUDE_MAX),
		simpleDoubleRange("valueIsMin", 0, 0, 1, pb.Pool_EXCLUDE_MIN),
		simpleDoubleRange("excludeBothAtMinBoundary", 0, 0, 1, pb.Pool_EXCLUDE_BOTH),
		simpleDoubleRange("excludeBothAtMaxBoundary", 1, 0, 1, pb.Pool_EXCLUDE_BOTH),

		// CreationTimeRangeFilter negative cases (before Start, after End, and excluded boundaries)
		simpleCreationTimeRange("beforeStart", refStart.Add(-time.Second), refStart, refEnd, pb.Pool_EXCLUDE_NONE),
		simpleCreationTimeRange("afterEnd", refEnd.Add(time.Second), refStart, refEnd, pb.Pool_EXCLUDE_NONE),
		simpleCreationTimeRange("atStartExcludedByStart", refStart, refStart, refEnd, pb.Pool_EXCLUDE_START),
		simpleCreationTimeRange("atStartExcludedByBoth", refStart, refStart, refEnd, pb.Pool_EXCLUDE_BOTH),
		simpleCreationTimeRange("atEndExcludedByEnd", refEnd, refStart, refEnd, pb.Pool_EXCLUDE_END),
		simpleCreationTimeRange("atEndExcludedByBoth", refEnd, refStart, refEnd, pb.Pool_EXCLUDE_BOTH),

		{
			Name: "StringEquals case-sensitive mismatch (value vs VALUE)",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					StringArgs: map[string]string{
						"field": "value",
					},
				},
			},
			Pool: &pb.Pool{
				StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					{
						StringArg: "field",
						Value:     "VALUE",
					},
				},
			},
		},

		{
			Name: "StringEquals missing requested field in StringArgs",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					StringArgs: map[string]string{
						"otherfield": "othervalue",
					},
				},
			},
			Pool: &pb.Pool{
				StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					{
						StringArg: "field",
						Value:     "value",
					},
				},
			},
		},

		{
			Name: "TagPresent case-sensitive mismatch (MYTAG vs mytag)",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{
						"MYTAG",
					},
				},
			},
			Pool: &pb.Pool{
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{
						Tag: "mytag",
					},
				},
			},
		},

		{
			Name: "TagPresent multiple required tags with one missing",
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{
						"A", "B", "C",
					},
				},
			},
			Pool: &pb.Pool{
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{
						Tag: "A",
					},
					{
						Tag: "D",
					},
					{
						Tag: "C",
					},
				},
			},
		},

		multipleFilters(false, true, true, true),
		multipleFilters(true, false, true, true),
		multipleFilters(true, true, false, true),
		multipleFilters(true, true, true, false),
	}
}

// simpleDoubleRange builds a single-field DoubleRangeFilter test case whose Name
// includes the scenario label, tested value, [min, max] range, and FilterBounds.
func simpleDoubleRange(name string, value, min, max float64, exclude pb.Pool_FilterBounds) TestCase {
	return TestCase{
		Name: fmt.Sprintf("DoubleRange %s (val=%v, range=[%v,%v], bounds=%s)", name, value, min, max, exclude.String()),
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				DoubleArgs: map[string]float64{
					"field": value,
				},
			},
		},
		Pool: &pb.Pool{
			DoubleRangeFilters: []*pb.Pool_DoubleRangeFilter{
				{
					DoubleArg: "field",
					Minimum:   min,
					Maximum:   max,
					Bounds:    exclude,
				},
			},
		},
	}
}

// simpleCreationTimeRange builds a CreationTimeRangeFilter test case with the
// given ticket creation time, [start, end] range, and FilterBounds.
func simpleCreationTimeRange(name string, crTime, start, end time.Time, bounds pb.Pool_FilterBounds) TestCase {
	return TestCase{
		Name: fmt.Sprintf("CreationTimeRange %s (bounds=%s)", name, bounds.String()),
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				CreationTime: timestamppb.New(crTime),
			},
		},
		Pool: &pb.Pool{
			CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
				Start:  timestamppb.New(start),
				End:    timestamppb.New(end),
				Bounds: bounds,
			},
		},
	}
}

// multipleFilters builds a test case combining all four filter types
// (DoubleRangeFilter, StringEqualsFilter, TagPresentFilter, and CreationTimeRangeFilter).
// A ticket is included only when all four filter types match (all booleans true)
// and excluded when any one filter type fails to match.
func multipleFilters(doubleRange, stringEquals, tagPresent, creationTimeRange bool) TestCase {
	a := float64(0) // value that produces a filter pass
	if !doubleRange {
		a = 10 // value that produces a filter rejection
	}

	b := "hi" // value that produces a filter pass
	if !stringEquals {
		b = "bye" // value that produces a filter rejection
	}

	c := "yo" // value that produces a filter pass
	if !tagPresent {
		c = "cya" // value that produces a filter rejection
	}

	// value that produces a filter pass
	d := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)
	if !creationTimeRange {
		// value that produces a filter rejection
		d = time.Date(1909, 11, 17, 20, 34, 58, 651387237, time.UTC)
	}

	return TestCase{
		Name: fmt.Sprintf("MultipleFilters (doubleRange=%t, stringEquals=%t, tagPresent=%t, creationTimeRange=%t)",
			doubleRange, stringEquals, tagPresent, creationTimeRange),
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				DoubleArgs: map[string]float64{
					"a": a,
				},
				StringArgs: map[string]string{
					"b": b,
				},
				Tags:         []string{c},
				CreationTime: timestamppb.New(d),
			},
		},
		Pool: &pb.Pool{
			DoubleRangeFilters: []*pb.Pool_DoubleRangeFilter{
				{
					DoubleArg: "a",
					Minimum:   -1,
					Maximum:   1,
				},
			},
			StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
				{
					StringArg: "b",
					Value:     "hi",
				},
			},
			TagPresentFilters: []*pb.Pool_TagPresentFilter{
				{
					Tag: "yo",
				},
			},
			CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
				Start: timestamppb.New(time.Date(2000, 11, 17, 20, 34, 58, 651387237, time.UTC)),
				End:   timestamppb.New(time.Date(2024, 11, 17, 20, 34, 58, 651387237, time.UTC)),
			},
		},
	}
}
