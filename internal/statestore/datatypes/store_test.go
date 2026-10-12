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

func TestParseReplIdTimestampMs(t *testing.T) {
	t.Parallel()

	ts, err := ParseReplIdTimestampMs("1790307871351-12")
	require.NoError(t, err)
	assert.Equal(t, int64(1790307871351), ts)

	_, err = ParseReplIdTimestampMs("")
	assert.Error(t, err)

	_, err = ParseReplIdTimestampMs("not-a-timestamp")
	assert.Error(t, err)
}

func TestCompareReplIds(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, CompareReplIds("", ""))
	assert.Equal(t, -1, CompareReplIds("", "1790307871351-0"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-0", ""))
	assert.Equal(t, 0, CompareReplIds("1790307871351-12", "1790307871351-12"))

	// Numeric sequence comparison ("-2" < "-12", whereas lexicographically "-12" < "-2")
	assert.Equal(t, -1, CompareReplIds("1790307871351-2", "1790307871351-12"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-12", "1790307871351-2"))

	// Timestamp comparison
	assert.Equal(t, -1, CompareReplIds("1790307871350-99", "1790307871351-0"))
	assert.Equal(t, 1, CompareReplIds("1790307871351-0", "1790307871350-99"))
}

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
			in:   StateUpdate{Cmd: Ticket, Key: "1790307871351-0", Value: "raw-protobuf-binary-payload"},
			want: "{Cmd:0 Key:1790307871351-0 Keys: Value:<27 bytes> ReplId:}",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.in.String(), tc.name)
	}
}
