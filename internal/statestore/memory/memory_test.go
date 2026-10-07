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
package memoryReplicator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConfig(maxUpdatesPerPoll, queueBufferSize, waitTimeoutMs int) *viper.Viper {
	cfg := viper.New()
	cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", maxUpdatesPerPoll)
	cfg.Set("OM_CACHE_IN_QUEUE_BUFFER_SIZE", queueBufferSize)
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", waitTimeoutMs)
	return cfg
}

func TestSendAndGetUpdatesRoundTrip(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(100, 100, 20))
	validator := rc.GetReplIdValidator()
	require.NotNil(t, validator)

	updates := []*store.StateUpdate{
		{Cmd: store.Ticket, Value: "ticket-payload-1"},
		{Cmd: store.Activate, Key: "t-1"},
		{Cmd: store.Activate, Keys: []string{"t-2", "", "t-3"}},
		{Cmd: store.Deactivate, Key: "t-1"},
		{Cmd: store.Deactivate, Keys: []string{"t-2", "t-3"}},
		{Cmd: store.Assign, Key: "t-1", Value: "10.0.0.1:7777"},
	}

	res := rc.SendUpdates(updates)
	require.Len(t, res, len(updates))
	for i, r := range res {
		require.NoError(t, r.Err, "update %d failed", i)
		assert.Regexp(t, validator, r.Result)
		if i > 0 {
			assert.Equal(t, -1, store.CompareReplIds(res[i-1].Result, r.Result))
		}
	}

	got := rc.GetUpdates()
	require.Len(t, got, len(updates))

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Ticket,
		Key:    res[0].Result,
		Keys:   nil,
		Value:  "ticket-payload-1",
		ReplId: res[0].Result,
	}, got[0])

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Activate,
		Key:    "t-1",
		Keys:   nil,
		Value:  "",
		ReplId: res[1].Result,
	}, got[1])

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Activate,
		Key:    "t-2",
		Keys:   []string{"t-2", "t-3"},
		Value:  "",
		ReplId: res[2].Result,
	}, got[2])

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Deactivate,
		Key:    "t-1",
		Keys:   nil,
		Value:  "",
		ReplId: res[3].Result,
	}, got[3])

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Deactivate,
		Key:    "t-2",
		Keys:   []string{"t-2", "t-3"},
		Value:  "",
		ReplId: res[4].Result,
	}, got[4])

	assert.Equal(t, &store.StateUpdate{
		Cmd:    store.Assign,
		Key:    "t-1",
		Keys:   nil,
		Value:  "10.0.0.1:7777",
		ReplId: res[5].Result,
	}, got[5])
}

func TestSendUpdatesValidationErrorsAndResultContract(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(100, 100, 20))

	cases := []struct {
		name    string
		update  *store.StateUpdate
		wantErr error
		wantKey string
	}{
		{
			name:    "Ticket missing value with empty key",
			update:  &store.StateUpdate{Cmd: store.Ticket, Value: ""},
			wantErr: NoTicketDataErr,
			wantKey: "",
		},
		{
			name:    "Ticket missing value with caller key",
			update:  &store.StateUpdate{Cmd: store.Ticket, Key: "caller-ticket-key", Value: ""},
			wantErr: NoTicketDataErr,
			wantKey: "caller-ticket-key",
		},
		{
			name:    "Activate missing key and keys",
			update:  &store.StateUpdate{Cmd: store.Activate},
			wantErr: NoTicketKeyErr,
			wantKey: "",
		},
		{
			name:    "Activate packed all empty keys",
			update:  &store.StateUpdate{Cmd: store.Activate, Keys: []string{"", ""}},
			wantErr: NoTicketKeyErr,
			wantKey: "",
		},
		{
			name:    "Activate packed all empty keys with fallback Key set",
			update:  &store.StateUpdate{Cmd: store.Activate, Key: "fallback-act-key", Keys: []string{"", ""}},
			wantErr: NoTicketKeyErr,
			wantKey: "fallback-act-key",
		},
		{
			name:    "Deactivate missing key and keys",
			update:  &store.StateUpdate{Cmd: store.Deactivate},
			wantErr: NoTicketKeyErr,
			wantKey: "",
		},
		{
			name:    "Deactivate packed all empty keys",
			update:  &store.StateUpdate{Cmd: store.Deactivate, Keys: []string{""}},
			wantErr: NoTicketKeyErr,
			wantKey: "",
		},
		{
			name:    "Assign missing key",
			update:  &store.StateUpdate{Cmd: store.Assign, Key: "", Value: "10.0.0.1:7777"},
			wantErr: NoTicketKeyErr,
			wantKey: "",
		},
		{
			name:    "Assign missing value",
			update:  &store.StateUpdate{Cmd: store.Assign, Key: "ticket-42", Value: ""},
			wantErr: NoAssignmentErr,
			wantKey: "ticket-42",
		},
		{
			name:    "Invalid command with single key",
			update:  &store.StateUpdate{Cmd: 999, Key: "bad-cmd-key"},
			wantErr: InvalidInputErr,
			wantKey: "bad-cmd-key",
		},
		{
			name:    "Invalid command with packed keys",
			update:  &store.StateUpdate{Cmd: -1, Keys: []string{"", "bad-packed-key", "other"}},
			wantErr: InvalidInputErr,
			wantKey: "bad-packed-key",
		},
	}

	inputs := make([]*store.StateUpdate, len(cases))
	for i, tc := range cases {
		inputs[i] = tc.update
	}

	res := rc.SendUpdates(inputs)
	require.Len(t, res, len(cases))
	for i, tc := range cases {
		assert.ErrorIs(t, res[i].Err, tc.wantErr, tc.name)
		assert.Equal(t, tc.wantKey, res[i].Result, tc.name)
		assert.Equal(t, tc.update.PrimaryKey(), res[i].Result, tc.name)
	}

	// Ensure no invalid updates were enqueued.
	got := rc.GetUpdates()
	require.NotNil(t, got)
	assert.Empty(t, got)
}

func TestSendUpdatesDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(100, 100, 20))

	inputs := []*store.StateUpdate{
		{
			Cmd:    store.Ticket,
			Key:    "orig-ticket-key",
			Keys:   []string{"k1", "k2"},
			Value:  "ticket-data",
			ReplId: "orig-repl-1",
		},
		{
			Cmd:    store.Activate,
			Key:    "orig-act-key",
			Keys:   []string{"a1", "", "a2"},
			Value:  "orig-act-val",
			ReplId: "orig-repl-2",
		},
		{
			Cmd:    store.Deactivate,
			Key:    "",
			Keys:   []string{"d1"},
			Value:  "orig-deact-val",
			ReplId: "",
		},
		{
			Cmd:    store.Assign,
			Key:    "t-assign",
			Keys:   []string{"extra-key"},
			Value:  "conn-str",
			ReplId: "orig-repl-4",
		},
	}

	expected := []*store.StateUpdate{
		{
			Cmd:    store.Ticket,
			Key:    "orig-ticket-key",
			Keys:   []string{"k1", "k2"},
			Value:  "ticket-data",
			ReplId: "orig-repl-1",
		},
		{
			Cmd:    store.Activate,
			Key:    "orig-act-key",
			Keys:   []string{"a1", "", "a2"},
			Value:  "orig-act-val",
			ReplId: "orig-repl-2",
		},
		{
			Cmd:    store.Deactivate,
			Key:    "",
			Keys:   []string{"d1"},
			Value:  "orig-deact-val",
			ReplId: "",
		},
		{
			Cmd:    store.Assign,
			Key:    "t-assign",
			Keys:   []string{"extra-key"},
			Value:  "conn-str",
			ReplId: "orig-repl-4",
		},
	}

	res := rc.SendUpdates(inputs)
	require.Len(t, res, len(inputs))
	for _, r := range res {
		require.NoError(t, r.Err)
	}

	assert.Equal(t, expected, inputs)
}

func TestFieldNormalizationMatchesRedisRoundTrip(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(100, 100, 20))

	inputs := []*store.StateUpdate{
		// Ticket drops caller Key and Keys
		{
			Cmd:   store.Ticket,
			Key:   "caller-key",
			Keys:  []string{"extra-1", "extra-2"},
			Value: "pb-data",
		},
		// Activate with 1 valid key in Keys normalizes Keys to nil, forces Key = validKeys[0], drops Value
		{
			Cmd:   store.Activate,
			Key:   "wrong-key",
			Keys:  []string{"", "only-valid-key", ""},
			Value: "dropped-value",
		},
		// Deactivate with >1 valid keys in Keys forces Key = validKeys[0], keeps filtered Keys, drops Value
		{
			Cmd:   store.Deactivate,
			Key:   "wrong-key",
			Keys:  []string{"", "first-valid", "", "second-valid"},
			Value: "dropped-value",
		},
		// Activate with empty non-nil Keys slice and valid Key normalizes Keys to nil and drops Value
		{
			Cmd:   store.Activate,
			Key:   "single-key",
			Keys:  []string{},
			Value: "dropped-value",
		},
		// Assign drops extraneous Keys
		{
			Cmd:   store.Assign,
			Key:   "assign-key",
			Keys:  []string{"dropped-key"},
			Value: "server:1234",
		},
	}

	res := rc.SendUpdates(inputs)
	require.Len(t, res, len(inputs))
	for _, r := range res {
		require.NoError(t, r.Err)
	}

	got := rc.GetUpdates()
	require.Len(t, got, len(inputs))

	// 0: Ticket
	assert.Equal(t, res[0].Result, got[0].Key)
	assert.Nil(t, got[0].Keys)
	assert.Equal(t, "pb-data", got[0].Value)
	assert.Equal(t, res[0].Result, got[0].ReplId)

	// 1: 1-key packed Activate -> unpacked
	assert.Equal(t, "only-valid-key", got[1].Key)
	assert.Nil(t, got[1].Keys)
	assert.Empty(t, got[1].Value)
	assert.Equal(t, res[1].Result, got[1].ReplId)

	// 2: multi-key packed Deactivate -> Key forced to validKeys[0]
	assert.Equal(t, "first-valid", got[2].Key)
	assert.Equal(t, []string{"first-valid", "second-valid"}, got[2].Keys)
	assert.Empty(t, got[2].Value)
	assert.Equal(t, res[2].Result, got[2].ReplId)

	// 3: single-key Activate with empty slice -> Keys is nil
	assert.Equal(t, "single-key", got[3].Key)
	assert.Nil(t, got[3].Keys)
	assert.Empty(t, got[3].Value)
	assert.Equal(t, res[3].Result, got[3].ReplId)

	// 4: Assign -> Keys is nil
	assert.Equal(t, "assign-key", got[4].Key)
	assert.Nil(t, got[4].Keys)
	assert.Equal(t, "server:1234", got[4].Value)
	assert.Equal(t, res[4].Result, got[4].ReplId)
}

func TestConcurrentSendUpdatesStrictOrderingAndMonotonicClockRegression(t *testing.T) {
	t.Parallel()

	const workers = 16
	const batchesPerWorker = 25
	const updatesPerBatch = 3
	totalUpdates := workers * batchesPerWorker * updatesPerBatch

	rc := New(newTestConfig(totalUpdates, totalUpdates, 50))

	// Simulate wall-clock regression by setting replTS slightly in the future
	// before concurrent callers run; generated IDs must still be strictly monotonic.
	rc.mu.Lock()
	rc.replTS = time.Now().Add(5 * time.Second)
	rc.replCount = 0
	rc.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				batch := []*store.StateUpdate{
					{Cmd: store.Ticket, Value: fmt.Sprintf("w%d-b%d", w, b)},
					{Cmd: store.Activate, Key: fmt.Sprintf("k-%d-%d", w, b)},
					{Cmd: store.Deactivate, Keys: []string{fmt.Sprintf("k1-%d-%d", w, b), fmt.Sprintf("k2-%d-%d", w, b)}},
				}
				res := rc.SendUpdates(batch)
				for _, r := range res {
					assert.NoError(t, r.Err)
				}
			}
		}()
	}
	wg.Wait()

	got := rc.GetUpdates()
	require.Len(t, got, totalUpdates)
	for i := 1; i < len(got); i++ {
		assert.Equal(t, -1, store.CompareReplIds(got[i-1].ReplId, got[i].ReplId),
			"out-of-order ReplIds at index %d: %s vs %s", i, got[i-1].ReplId, got[i].ReplId)
	}
}

func TestSendUpdatesFullChannelNonBlocking(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(2, 2, 20))

	res := rc.SendUpdates([]*store.StateUpdate{
		{Cmd: store.Activate, Key: "k1"},
		{Cmd: store.Activate, Key: "k2"},
	})
	require.Len(t, res, 2)
	require.NoError(t, res[0].Err)
	require.NoError(t, res[1].Err)

	// Queue is now at capacity (2). Sending more updates must fail immediately without blocking.
	overflowRes := rc.SendUpdates([]*store.StateUpdate{
		{Cmd: store.Activate, Key: "overflow-1"},
		{Cmd: store.Deactivate, Keys: []string{"", "overflow-2"}},
	})
	require.Len(t, overflowRes, 2)
	assert.ErrorIs(t, overflowRes[0].Err, FullReplicationQueueErr)
	assert.Equal(t, "overflow-1", overflowRes[0].Result)
	assert.ErrorIs(t, overflowRes[1].Err, FullReplicationQueueErr)
	assert.Equal(t, "overflow-2", overflowRes[1].Result)

	// Drain existing items and verify subsequent send succeeds with strictly greater ReplId.
	drained := rc.GetUpdates()
	require.Len(t, drained, 2)
	assert.Equal(t, "k1", drained[0].Key)
	assert.Equal(t, "k2", drained[1].Key)

	afterDrainRes := rc.SendUpdates([]*store.StateUpdate{
		{Cmd: store.Activate, Key: "k3"},
	})
	require.Len(t, afterDrainRes, 1)
	require.NoError(t, afterDrainRes[0].Err)
	assert.Equal(t, -1, store.CompareReplIds(res[1].Result, afterDrainRes[0].Result))
}

func TestGetUpdatesTimeoutAndBatching(t *testing.T) {
	t.Parallel()

	rc := New(newTestConfig(3, 10, 15))

	// Timeout on empty queue returns non-nil empty slice.
	empty := rc.GetUpdates()
	require.NotNil(t, empty)
	assert.Empty(t, empty)

	// Enqueue 7 updates; with maxUpdatesPerPoll=3, GetUpdates should return 3, 3, 1, then 0.
	updates := make([]*store.StateUpdate, 7)
	for i := range updates {
		updates[i] = &store.StateUpdate{
			Cmd: store.Activate,
			Key: fmt.Sprintf("ticket-%d", i),
		}
	}
	res := rc.SendUpdates(updates)
	for _, r := range res {
		require.NoError(t, r.Err)
	}

	batch1 := rc.GetUpdates()
	require.Len(t, batch1, 3)
	assert.Equal(t, "ticket-0", batch1[0].Key)
	assert.Equal(t, "ticket-1", batch1[1].Key)
	assert.Equal(t, "ticket-2", batch1[2].Key)

	batch2 := rc.GetUpdates()
	require.Len(t, batch2, 3)
	assert.Equal(t, "ticket-3", batch2[0].Key)
	assert.Equal(t, "ticket-4", batch2[1].Key)
	assert.Equal(t, "ticket-5", batch2[2].Key)

	batch3 := rc.GetUpdates()
	require.Len(t, batch3, 1)
	assert.Equal(t, "ticket-6", batch3[0].Key)

	batch4 := rc.GetUpdates()
	require.NotNil(t, batch4)
	assert.Empty(t, batch4)
}
