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

package filter

import (
	"testing"
	"time"

	"github.com/googleforgames/open-match2/v2/internal/filter/testcases"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestMeetsCriteria validates that tickets are correctly included in or excluded
// from a pool based on that pool's DoubleRangeFilters, StringEqualsFilters,
// TagPresentFilters, and CreationTimeRangeFilter criteria.
func TestMeetsCriteria(t *testing.T) {
	t.Run("Included", func(t *testing.T) {
		for _, tc := range testcases.IncludedTestCases() {
			tc := tc
			t.Run(tc.Name, func(t *testing.T) {
				if !In(tc.Pool, tc.Ticket) {
					t.Error("ticket should be included in the pool")
				}
			})
		}
	})

	t.Run("Excluded", func(t *testing.T) {
		for _, tc := range testcases.ExcludedTestCases() {
			tc := tc
			t.Run(tc.Name, func(t *testing.T) {
				if In(tc.Pool, tc.Ticket) {
					t.Error("ticket should be excluded from the pool")
				}
			})
		}
	})
}

// TestValidPoolFilter verifies that ValidatePoolFilters accepts pools with nil
// or valid CreationTimeRangeFilter bounds and rejects pools whose Start or End
// timestamp is invalid according to protobuf timestamp rules.
func TestValidPoolFilter(t *testing.T) {
	now := timestamppb.New(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	later := timestamppb.New(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC))

	type testcase struct {
		name   string
		pool   *pb.Pool
		wantOk bool
		err    error
	}
	tcs := []testcase{
		{
			name:   "valid pool without creation time filter",
			pool:   &pb.Pool{Name: "no-time-filter"},
			wantOk: true,
		},
		{
			name: "valid creation time filter with start and end",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: now,
					End:   later,
				},
			},
			wantOk: true,
		},
		{
			name: "valid creation time filter with start only",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: now,
				},
			},
			wantOk: true,
		},
		{
			name: "valid creation time filter with end only",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					End: later,
				},
			},
			wantOk: true,
		},
		{
			name: "valid creation time filter with both start and end nil",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{},
			},
			wantOk: true,
		},
		{
			name: "invalid creation time filter start",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: &timestamppb.Timestamp{Nanos: -1},
				},
			},
			wantOk: false,
			err:    InvalidCreationTimeFilterStartErrorStatus,
		},
		{
			name: "invalid creation time filter end",
			pool: &pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					End: &timestamppb.Timestamp{Nanos: -1},
				},
			},
			wantOk: false,
			err:    InvalidCreationTimeFilterEndErrorStatus,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ok, err := ValidatePoolFilters(tc.pool)
			require.Equal(t, tc.wantOk, ok)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
