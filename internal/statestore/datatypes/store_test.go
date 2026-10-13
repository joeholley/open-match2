// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseReplIdTimestampMs verifies that ParseReplIdTimestampMs extracts the
// millisecond Unix timestamp prefix from a "<ms>-<seq>" or bare "<ms>"
// replication ID, and returns an error when the prefix is empty or non-numeric.
func TestParseReplIdTimestampMs(t *testing.T) {
	t.Parallel()

	ts, err := ParseReplIdTimestampMs("1790307871351-12")
	require.NoError(t, err)
	assert.Equal(t, int64(1790307871351), ts)

	tsBare, err := ParseReplIdTimestampMs("1790307871351")
	require.NoError(t, err)
	assert.Equal(t, int64(1790307871351), tsBare)

	_, err = ParseReplIdTimestampMs("")
	assert.Error(t, err)

	_, err = ParseReplIdTimestampMs("-12")
	assert.Error(t, err)

	_, err = ParseReplIdTimestampMs("not-a-timestamp")
	assert.Error(t, err)
}

// TestCompareReplIds verifies numeric ordering of "<timestampMs>-<seq>"
// replication IDs, handling of empty IDs, and lexicographical fallback when
// an ID is not in "<uint>-<uint>" format.
func TestCompareReplIds(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, CompareReplIds("", ""))
	assert.Equal(t, -1, CompareReplIds("", "1790307871351-0"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-0", ""))
	assert.Equal(t, 0, CompareReplIds("1790307871351-12", "1790307871351-12"))

	// Numerically equal IDs with different zero-padding compare equal.
	assert.Equal(t, 0, CompareReplIds("01790307871351-0", "1790307871351-0"))

	// Numeric sequence comparison ("-2" < "-12", whereas lexicographically "-12" < "-2")
	assert.Equal(t, -1, CompareReplIds("1790307871351-2", "1790307871351-12"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-12", "1790307871351-2"))

	// Timestamp comparison
	assert.Equal(t, -1, CompareReplIds("1790307871350-99", "1790307871351-0"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-0", "1790307871350-99"))

	// Fallback to lexicographical strings.Compare when an ID lacks "-<seq>" (e.g., initial bare timestamp).
	assert.Equal(t, -1, CompareReplIds("1790307871350", "1790307871351-0"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-0", "1790307871350"))
}

// TestStateUpdatePrimaryKey verifies that PrimaryKey returns Key when set,
// falls back to the first non-empty element in Keys, and returns "" for nil
// or empty updates.
func TestStateUpdatePrimaryKey(t *testing.T) {
	t.Parallel()

	var nilUpdate *StateUpdate
	assert.Equal(t, "", nilUpdate.PrimaryKey())
	assert.Equal(t, "", (&StateUpdate{}).PrimaryKey())
	assert.Equal(t, "k1", (&StateUpdate{Key: "k1"}).PrimaryKey())
	assert.Equal(t, "k1", (&StateUpdate{Key: "k1", Keys: []string{"k2", "k3"}}).PrimaryKey())
	assert.Equal(t, "k2", (&StateUpdate{Keys: []string{"", "k2", "k3"}}).PrimaryKey())
	assert.Equal(t, "", (&StateUpdate{Keys: []string{"", ""}}).PrimaryKey())
}

// TestStateUpdateString verifies that StateUpdate.String truncates packed Keys
// beyond 3 entries and summarizes binary Ticket payloads by byte length while
// preserving human-readable fields on non-Ticket commands.
func TestStateUpdateString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   StateUpdate
		want string
	}{
		{
			name: "short packed keys",
			in:   StateUpdate{Cmd: Activate, Key: "k1", Keys: []string{"k1", "k2", "k3"}},
			want: "{Cmd:1 Key:k1 Keys:[k1 k2 k3] Value: ReplId:}",
		},
		{
			name: "truncated packed keys",
			in:   StateUpdate{Cmd: Deactivate, Key: "k1", Keys: []string{"k1", "k2", "k3", "k4", "k5"}},
			want: "{Cmd:2 Key:k1 Keys:[k1 k2 k3 ...] Value: ReplId:}",
		},
		{
			name: "binary ticket value payload summarized",
			in:   StateUpdate{Cmd: Ticket, Key: "1790307871351-0", Value: "raw-protobuf-binary-payload", ReplId: "1790307871351-0"},
			want: "{Cmd:0 Key:1790307871351-0 Keys: Value:<27 bytes> ReplId:1790307871351-0}",
		},
		{
			name: "empty ticket value is not summarized as byte length",
			in:   StateUpdate{Cmd: Ticket, Key: "1790307871351-0", Value: ""},
			want: "{Cmd:0 Key:1790307871351-0 Keys: Value: ReplId:}",
		},
		{
			name: "assign command prints value and replId verbatim",
			in:   StateUpdate{Cmd: Assign, Key: "1790307871351-0", Value: "10.0.0.1:7777", ReplId: "1790307871352-0"},
			want: "{Cmd:3 Key:1790307871351-0 Keys: Value:10.0.0.1:7777 ReplId:1790307871352-0}",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.in.String(), tc.name)
	}
}
