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
package cache

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleforgames/open-match2/v2/internal/config"
	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	memoryReplicator "github.com/googleforgames/open-match2/v2/internal/statestore/memory"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMain(m *testing.M) {
	meter := noop.NewMeterProvider().Meter("cache-test")
	RegisterMetrics(&meter)
	os.Exit(m.Run())
}

func TestWaitForDeactivationViaInactiveSetAndWatermark(t *testing.T) {
	t.Parallel()

	tc := New(config.Read(), nil)

	// Case 1: Ticket already in InactiveSet returns immediately.
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-1"}, nil, nil)
	assert.True(t, tc.WaitForDeactivation("1790307871351-1", "", 50*time.Millisecond))

	// Case 2: Ticket expired/removed from InactiveSet, but watermark has passed targetReplId.
	tc.recordApplied("1790307871400-5")
	assert.True(t, tc.WaitForDeactivation("1790307871351-2", "1790307871400-3", 50*time.Millisecond))

	// Case 3: Wakes immediately when recordApplied advances past targetReplId.
	done := make(chan bool, 1)
	go func() {
		done <- tc.WaitForDeactivation("1790307871351-3", "1790307871500-0", 2*time.Second)
	}()
	time.Sleep(20 * time.Millisecond)
	tc.recordApplied("1790307871500-1")
	require.True(t, <-done)

	// Case 4: Times out when neither InactiveSet nor watermark matches.
	assert.False(t, tc.WaitForDeactivation("1790307871351-4", "1790307871999-0", 30*time.Millisecond))
}

func TestEndToEndMemoryReplicationWatermark(t *testing.T) {
	t.Parallel()

	cfg := config.Read()
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 20)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 10)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
	cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := New(cfg, memoryReplicator.New(cfg))
	go tc.OutgoingReplicationQueue(ctx)
	go tc.IncomingReplicationQueue(ctx)

	rChan := make(chan *store.StateResponse, 1)
	tc.UpRequests <- &UpdateRequest{
		Ctx:         ctx,
		ResultsChan: rChan,
		EnqueuedAt:  time.Now(),
		Update: store.StateUpdate{
			Cmd: store.Deactivate,
			Key: "1790307871351-12",
		},
	}
	res := <-rChan
	require.NoError(t, res.Err)
	require.NotEmpty(t, res.Result)

	assert.True(t, tc.WaitForDeactivation("1790307871351-12", res.Result, 2*time.Second))
	assert.True(t, tc.HasAppliedReplId(res.Result))
}

func TestFullPollUsesTunableFullPollWait(t *testing.T) {
	t.Parallel()

	cfg := config.Read()
	// Set idle wait timeout and partial-poll wait very high (2000ms), but full-poll
	// wait short (15ms) with a small max updates per poll (2). Sending 5 updates
	// requires 3 polls: the first 2 polls are full (2 updates each) and should use
	// OM_CACHE_IN_FULL_POLL_WAIT_MS rather than waiting 2000ms between polls.
	cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", 2)
	cfg.Set("OM_CACHE_IN_QUEUE_BUFFER_SIZE", 20)
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 2000)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 2000)
	cfg.Set("OM_CACHE_IN_FULL_POLL_WAIT_MS", 15)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
	cfg.Set("OM_CACHE_IN_FULL_APPLY_SLEEP_MS", 5)
	cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
	cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := New(cfg, memoryReplicator.New(cfg))

	// Send 5 updates into the replicator before starting IncomingReplicationQueue
	// so all 5 are waiting in the replication stream when polling begins.
	go tc.OutgoingReplicationQueue(ctx)
	rChan := make(chan *store.StateResponse, 5)
	ticketIds := []string{
		"1790307871351-1",
		"1790307871351-2",
		"1790307871351-3",
		"1790307871351-4",
		"1790307871351-5",
	}
	for _, id := range ticketIds {
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: rChan,
			EnqueuedAt:  time.Now(),
			Update: store.StateUpdate{
				Cmd: store.Deactivate,
				Key: id,
			},
		}
	}
	var lastReplId string
	for range ticketIds {
		res := <-rChan
		require.NoError(t, res.Err)
		lastReplId = res.Result
	}

	start := time.Now()
	go tc.IncomingReplicationQueue(ctx)

	// If full polls waited OM_CACHE_IN_WAIT_TIMEOUT_MS or OM_CACHE_IN_POLL_WAIT_MS (2000ms),
	// 3 polls would take >4s. With OM_CACHE_IN_FULL_POLL_WAIT_MS=15ms, all 5 updates should
	// be applied in well under 500ms.
	require.True(t, tc.WaitForDeactivation("1790307871351-5", lastReplId, 600*time.Millisecond))
	assert.Less(t, time.Since(start), 600*time.Millisecond)
}

func TestPartialPollUsesPollWaitInsteadOfIdleWaitTimeout(t *testing.T) {
	t.Parallel()

	cfg := config.Read()
	// Set idle OM_CACHE_IN_WAIT_TIMEOUT_MS high (2000ms), but OM_CACHE_IN_POLL_WAIT_MS
	// short (15ms) with OM_CACHE_IN_MAX_UPDATES_PER_POLL=100 so single-update polls are
	// non-empty partial polls. Wave 2 arriving right after Wave 1 should only wait
	// OM_CACHE_IN_POLL_WAIT_MS (15ms) rather than the 2000ms OM_CACHE_IN_WAIT_TIMEOUT_MS.
	cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", 100)
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 2000)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 15)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
	cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
	cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := New(cfg, memoryReplicator.New(cfg))
	go tc.OutgoingReplicationQueue(ctx)

	// Enqueue Wave 1 before starting IncomingReplicationQueue so the first poll
	// immediately sees a partial batch (1 update < 100 maxUpdatesPerPoll).
	rChan1 := make(chan *store.StateResponse, 1)
	tc.UpRequests <- &UpdateRequest{
		Ctx:         ctx,
		ResultsChan: rChan1,
		EnqueuedAt:  time.Now(),
		Update: store.StateUpdate{
			Cmd: store.Deactivate,
			Key: "1790307871351-101",
		},
	}
	res1 := <-rChan1
	require.NoError(t, res1.Err)

	start := time.Now()
	go tc.IncomingReplicationQueue(ctx)
	require.True(t, tc.WaitForDeactivation("1790307871351-101", res1.Result, 400*time.Millisecond))

	// Immediately send Wave 2. Because Poll 1 was a partial poll, the poll loop
	// pauses only for OM_CACHE_IN_POLL_WAIT_MS (15ms) before Poll 2, rather than
	// waiting out the 2000ms OM_CACHE_IN_WAIT_TIMEOUT_MS.
	rChan2 := make(chan *store.StateResponse, 1)
	tc.UpRequests <- &UpdateRequest{
		Ctx:         ctx,
		ResultsChan: rChan2,
		EnqueuedAt:  time.Now(),
		Update: store.StateUpdate{
			Cmd: store.Deactivate,
			Key: "1790307871351-102",
		},
	}
	res2 := <-rChan2
	require.NoError(t, res2.Err)

	require.True(t, tc.WaitForDeactivation("1790307871351-102", res2.Result, 400*time.Millisecond))
	assert.Less(t, time.Since(start), 400*time.Millisecond)
}

type immediateEmptyReplicator struct {
	polls     atomic.Int64
	validator *regexp.Regexp
}

func (r *immediateEmptyReplicator) GetUpdates() []*store.StateUpdate {
	r.polls.Add(1)
	return nil
}

func (r *immediateEmptyReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	return make([]*store.StateResponse, len(updates))
}

func (r *immediateEmptyReplicator) GetReplIdValidator() *regexp.Regexp {
	return r.validator
}

func TestEmptyPollReturningEarlyThrottlesToWaitTimeout(t *testing.T) {
	t.Parallel()

	cfg := config.Read()
	// When GetUpdates() returns an empty slice immediately without blocking (for
	// example, on a transient Redis XREAD error), the default post-poll branch
	// should sleep for the remaining OM_CACHE_IN_WAIT_TIMEOUT_MS (40ms) rather
	// than spinning in an unthrottled tight loop.
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 40)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
	cfg.Set("OM_CACHE_IN_FULL_POLL_WAIT_MS", 5)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rep := &immediateEmptyReplicator{
		validator: regexp.MustCompile(`^\d{13}-\d+$`),
	}
	tc := New(cfg, rep)
	go tc.IncomingReplicationQueue(ctx)

	time.Sleep(100 * time.Millisecond)
	polls := rep.polls.Load()
	assert.GreaterOrEqual(t, polls, int64(2), "expected at least 2 polls in 100ms with 40ms wait timeout")
	assert.LessOrEqual(t, polls, int64(5), "expected early-returning empty polls to be throttled by OM_CACHE_IN_WAIT_TIMEOUT_MS (got %d polls in 100ms)", polls)
}

func TestPackedTicketStateUpdates(t *testing.T) {
	t.Parallel()

	cfg := config.Read()
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 20)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 10)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
	cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
	cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := New(cfg, memoryReplicator.New(cfg))
	go tc.OutgoingReplicationQueue(ctx)
	go tc.IncomingReplicationQueue(ctx)

	matchTickets := []string{
		"1790307871351-10",
		"1790307871351-11",
		"1790307871351-12",
		"1790307871351-13",
	}

	// 1. Send a single packed Deactivate update for all 4 tickets.
	rChan := make(chan *store.StateResponse, 1)
	tc.UpRequests <- &UpdateRequest{
		Ctx:         ctx,
		ResultsChan: rChan,
		EnqueuedAt:  time.Now(),
		Update: store.StateUpdate{
			Cmd:  store.Deactivate,
			Key:  matchTickets[0],
			Keys: matchTickets,
		},
	}
	deactRes := <-rChan
	require.NoError(t, deactRes.Err)
	require.NotEmpty(t, deactRes.Result)

	require.True(t, tc.WaitForDeactivation(matchTickets[len(matchTickets)-1], deactRes.Result, 2*time.Second))
	for _, id := range matchTickets {
		_, isInactive := tc.InactiveSet.Load(id)
		assert.True(t, isInactive, "expected ticket %s to be in InactiveSet after packed deactivation", id)
	}

	// 2. Send a single packed Activate update for all 4 tickets.
	actChan := make(chan *store.StateResponse, 1)
	tc.UpRequests <- &UpdateRequest{
		Ctx:         ctx,
		ResultsChan: actChan,
		EnqueuedAt:  time.Now(),
		Update: store.StateUpdate{
			Cmd:  store.Activate,
			Key:  matchTickets[0],
			Keys: matchTickets,
		},
	}
	actRes := <-actChan
	require.NoError(t, actRes.Err)
	require.NotEmpty(t, actRes.Result)

	// Wait until the packed activation replication ID has been applied.
	require.Eventually(t, func() bool {
		return tc.HasAppliedReplId(actRes.Result)
	}, 2*time.Second, 10*time.Millisecond)
	for _, id := range matchTickets {
		_, isInactive := tc.InactiveSet.Load(id)
		assert.False(t, isInactive, "expected ticket %s to be removed from InactiveSet after packed activation", id)
	}
}

func TestMergeRequestsToUpdates(t *testing.T) {
	t.Parallel()

	t.Run("coalesces 10 contiguous deactivate requests and deduplicates keys", func(t *testing.T) {
		reqs := make([]*UpdateRequest, 10)
		expectedKeys := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("1790307871351-%d", i+1)
			expectedKeys = append(expectedKeys, key)
			if i == 5 {
				// Include a duplicate key from an earlier request to verify deduplication.
				reqs[i] = &UpdateRequest{
					Update: store.StateUpdate{
						Cmd:  store.Deactivate,
						Key:  "1790307871351-1",
						Keys: []string{"1790307871351-1", key},
					},
				}
			} else {
				reqs[i] = &UpdateRequest{
					Update: store.StateUpdate{
						Cmd: store.Deactivate,
						Key: key,
					},
				}
			}
		}

		packed, groups := mergeRequestsToUpdates(reqs, 1000)
		require.Len(t, packed, 1)
		require.Len(t, groups, 1)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, groups[0])
		assert.Equal(t, store.Deactivate, packed[0].Cmd)
		assert.Equal(t, expectedKeys[0], packed[0].Key)
		assert.Equal(t, expectedKeys, packed[0].Keys)
	})

	t.Run("breaks runs on interleaved commands preserving causal order", func(t *testing.T) {
		reqs := []*UpdateRequest{
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "A"}},
			{Update: store.StateUpdate{Cmd: store.Activate, Key: "A"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "B"}},
			{Update: store.StateUpdate{Cmd: store.Ticket, Value: "ticket-blob"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "C"}},
			{Update: store.StateUpdate{Cmd: store.Assign, Key: "C", Value: "conn"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "D"}},
		}

		packed, groups := mergeRequestsToUpdates(reqs, 1000)
		require.Len(t, packed, 7)
		require.Equal(t, [][]int{{0}, {1}, {2}, {3}, {4}, {5}, {6}}, groups)

		assert.Equal(t, store.Deactivate, packed[0].Cmd)
		assert.Equal(t, []string{"A"}, packed[0].Keys)

		assert.Equal(t, store.Activate, packed[1].Cmd)
		assert.Equal(t, []string{"A"}, packed[1].Keys)

		assert.Equal(t, store.Deactivate, packed[2].Cmd)
		assert.Equal(t, []string{"B"}, packed[2].Keys)

		assert.Equal(t, store.Ticket, packed[3].Cmd)
		assert.Equal(t, "ticket-blob", packed[3].Value)

		assert.Equal(t, store.Deactivate, packed[4].Cmd)
		assert.Equal(t, []string{"C"}, packed[4].Keys)

		assert.Equal(t, store.Assign, packed[5].Cmd)
		assert.Equal(t, "C", packed[5].Key)
		assert.Equal(t, "conn", packed[5].Value)

		assert.Equal(t, store.Deactivate, packed[6].Cmd)
		assert.Equal(t, []string{"D"}, packed[6].Keys)
	})

	t.Run("isolates requests with empty keys without poisoning adjacent requests", func(t *testing.T) {
		reqs := []*UpdateRequest{
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "id-1"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "id-2"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "", Keys: nil}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "id-3"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "", Keys: []string{""}}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "id-4"}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Key: "id-5"}},
		}

		packed, groups := mergeRequestsToUpdates(reqs, 1000)
		require.Len(t, packed, 5)
		require.Equal(t, [][]int{{0, 1}, {2}, {3}, {4}, {5, 6}}, groups)

		assert.Equal(t, []string{"id-1", "id-2"}, packed[0].Keys)
		assert.Empty(t, packed[1].Key)
		assert.Empty(t, packed[1].Keys)
		assert.Equal(t, []string{"id-3"}, packed[2].Keys)
		assert.Empty(t, packed[3].Key)
		assert.Equal(t, []string{""}, packed[3].Keys)
		assert.Equal(t, []string{"id-4", "id-5"}, packed[4].Keys)
	})

	t.Run("enforces maxKeysPerUpdate cap by splitting contiguous runs", func(t *testing.T) {
		reqs := []*UpdateRequest{
			{Update: store.StateUpdate{Cmd: store.Deactivate, Keys: []string{"k1", "k2"}}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Keys: []string{"k3", "k4"}}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Keys: []string{"k5", "k6"}}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Keys: []string{"k7", "k8"}}},
			{Update: store.StateUpdate{Cmd: store.Deactivate, Keys: []string{"k9", "k10"}}},
		}

		packed, groups := mergeRequestsToUpdates(reqs, 4)
		require.Len(t, packed, 3)
		require.Equal(t, [][]int{{0, 1}, {2, 3}, {4}}, groups)
		assert.Equal(t, []string{"k1", "k2", "k3", "k4"}, packed[0].Keys)
		assert.Equal(t, []string{"k5", "k6", "k7", "k8"}, packed[1].Keys)
		assert.Equal(t, []string{"k9", "k10"}, packed[2].Keys)
	})
}

type spyReplicator struct {
	inner       store.StateReplicator
	mu          sync.Mutex
	sentBatches [][]*store.StateUpdate
}

func (s *spyReplicator) GetUpdates() []*store.StateUpdate {
	return s.inner.GetUpdates()
}

func (s *spyReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	s.mu.Lock()
	batchCopy := make([]*store.StateUpdate, len(updates))
	for i, u := range updates {
		cp := *u
		if len(u.Keys) > 0 {
			cp.Keys = append([]string(nil), u.Keys...)
		}
		batchCopy[i] = &cp
	}
	s.sentBatches = append(s.sentBatches, batchCopy)
	s.mu.Unlock()
	return s.inner.SendUpdates(updates)
}

func (s *spyReplicator) GetReplIdValidator() *regexp.Regexp {
	return s.inner.GetReplIdValidator()
}

func TestOutgoingReplicationQueueCrossCallerPacking(t *testing.T) {
	t.Parallel()

	const numCallers = 10
	cfg := config.Read()
	cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)
	cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 1000)
	cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", numCallers)
	cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 15)
	cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
	cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spy := &spyReplicator{inner: memoryReplicator.New(cfg)}
	tc := New(cfg, spy)
	go tc.OutgoingReplicationQueue(ctx)
	go tc.IncomingReplicationQueue(ctx)

	resChans := make([]chan *store.StateResponse, numCallers)
	ticketIds := make([]string, numCallers)
	var wg sync.WaitGroup
	wg.Add(numCallers)
	for i := 0; i < numCallers; i++ {
		resChans[i] = make(chan *store.StateResponse, 1)
		ticketIds[i] = fmt.Sprintf("1790307871351-%d", i+1)
		go func(idx int) {
			defer wg.Done()
			tc.UpRequests <- &UpdateRequest{
				Ctx:         ctx,
				ResultsChan: resChans[idx],
				EnqueuedAt:  time.Now(),
				Update: store.StateUpdate{
					Cmd: store.Deactivate,
					Key: ticketIds[idx],
				},
			}
		}(i)
	}
	wg.Wait()

	var sharedReplId string
	for i := 0; i < numCallers; i++ {
		res := <-resChans[i]
		require.NotNil(t, res)
		require.NoError(t, res.Err)
		require.NotEmpty(t, res.Result)
		if i == 0 {
			sharedReplId = res.Result
		} else {
			assert.Equal(t, sharedReplId, res.Result, "expected all 10 packed requests to receive the same ReplId")
		}
		assert.True(t, tc.WaitForDeactivation(ticketIds[i], res.Result, 2*time.Second))
	}

	spy.mu.Lock()
	require.Len(t, spy.sentBatches, 1)
	require.Len(t, spy.sentBatches[0], 1)
	assert.ElementsMatch(t, ticketIds, spy.sentBatches[0][0].Keys)
	spy.mu.Unlock()
}

func baselineSetDifference(tickets, inactiveSet *sync.Map) []*pb.Ticket {
	out := make([]*pb.Ticket, 0)
	tickets.Range(func(id, val any) bool {
		if _, inactive := inactiveSet.Load(id); !inactive {
			if t, ok := val.(*pb.Ticket); ok {
				out = append(out, t)
			}
		}
		return true
	})
	return out
}

func snapshotToTicketSlice(snap []any) []*pb.Ticket {
	out := make([]*pb.Ticket, 0, len(snap))
	for _, v := range snap {
		if t, ok := v.(*pb.Ticket); ok {
			out = append(out, t)
		}
	}
	return out
}

// TestActiveTicketsEquivalenceSynchronous drives updates synchronously (R7) via
// applyUpdate and expireCacheEntries across Ticket -> Activate -> Deactivate ->
// Activate -> Expire transitions, verifying that SnapshotActiveTickets() always
// matches the exact set of *pb.Ticket pointers from setDifference(&Tickets, &InactiveSet).
func TestActiveTicketsEquivalenceSynchronous(t *testing.T) {
	ctx := context.Background()
	cfg := config.Read()
	cfg.Set("OM_CACHE_TICKET_TTL_MS", 600000)

	tc := New(cfg, nil)
	assertEquivalent := func(step string) {
		t.Helper()
		expected := baselineSetDifference(&tc.Tickets, &tc.InactiveSet)
		actual := snapshotToTicketSlice(tc.SnapshotActiveTickets())
		assert.ElementsMatch(t, expected, actual, "mismatch at step: %s", step)
	}

	now := time.Now()
	nowMs := now.UnixMilli()
	id1 := fmt.Sprintf("%013d-1", nowMs)
	id2 := fmt.Sprintf("%013d-2", nowMs)
	id3 := fmt.Sprintf("%013d-3", nowMs-700000) // already past default TTL (600,000ms)
	nonExistentId := fmt.Sprintf("%013d-999", nowMs)

	t1 := &pb.Ticket{Id: id1, ExpirationTime: timestamppb.New(now.Add(10 * time.Minute))}
	t2 := &pb.Ticket{Id: id2, ExpirationTime: timestamppb.New(now.Add(-5 * time.Second))} // custom early expiration in the past
	t3 := &pb.Ticket{Id: id3, ExpirationTime: timestamppb.New(now.Add(10 * time.Minute))}

	// Step 1: Create 3 tickets (all begin inactive).
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id1}, t1, nil)
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id2}, t2, nil)
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id3}, t3, nil)
	assertEquivalent("after Ticket creations (all inactive)")
	assert.Empty(t, tc.SnapshotActiveTickets())

	// Step 2: Activate non-existent ticket ID -> must NOT insert nil/phantom into ActiveTickets.
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: nonExistentId}, nil, nil)
	assertEquivalent("after Activate on non-existent ticket ID")
	assert.Empty(t, tc.SnapshotActiveTickets())

	// Step 3: Activate id1 and packed-activate [id2, id3, nonExistentId].
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: id1}, nil, nil)
	assertEquivalent("after Activate id1")
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: id2, Keys: []string{id2, id3, nonExistentId}}, nil, nil)
	assertEquivalent("after packed Activate [id2, id3, nonExistentId]")
	assert.Len(t, tc.SnapshotActiveTickets(), 3)

	// Step 4: Deactivate id1 and id3 (packed), then re-activate id1, then deactivate id3 again.
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: id1, Keys: []string{id1, id3}}, nil, nil)
	assertEquivalent("after packed Deactivate [id1, id3]")
	tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: id1}, nil, nil)
	assertEquivalent("after re-Activate id1")

	// Step 5: Run expireCacheEntries -> id3 expires via InactiveSet TTL (and is deleted from Tickets & ActiveTickets),
	// and id2 expires via custom early ExpirationTime (and is deleted from Tickets & ActiveTickets). Only id1 remains active.
	tc.expireCacheEntries(ctx)
	assertEquivalent("after expireCacheEntries")
	active := snapshotToTicketSlice(tc.SnapshotActiveTickets())
	require.Len(t, active, 1)
	assert.Same(t, t1, active[0])
}

func TestOrderedExpirationAndGauges(t *testing.T) {
	ctx := context.Background()

	t.Run("custom early expiration vs default TTL and out-of-order deactivate", func(t *testing.T) {
		cfg := config.Read()
		cfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)

		tc := New(cfg, nil)
		now := time.Now()
		nowMs := now.UnixMilli()

		// olderTicketId has creation timestamp 15s ago (> 10s TTL), newerTicketId has creation timestamp now.
		olderTicketId := fmt.Sprintf("%013d-1", nowMs-15000)
		newerTicketId := fmt.Sprintf("%013d-2", nowMs)
		earlyCustomId := fmt.Sprintf("%013d-3", nowMs)

		tOlder := &pb.Ticket{Id: olderTicketId, ExpirationTime: timestamppb.New(now.Add(5 * time.Minute))}
		tNewer := &pb.Ticket{Id: newerTicketId, ExpirationTime: timestamppb.New(now.Add(5 * time.Minute))}
		tEarly := &pb.Ticket{Id: earlyCustomId, ExpirationTime: timestamppb.New(now.Add(-1 * time.Second))}

		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: newerTicketId}, tNewer, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: olderTicketId}, tOlder, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: earlyCustomId}, tEarly, nil)

		// Activate all three, then deactivate newerTicketId first and olderTicketId second (out of creation order).
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Keys: []string{newerTicketId, olderTicketId, earlyCustomId}}, nil, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: newerTicketId}, nil, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: olderTicketId}, nil, nil)

		tc.expireCacheEntries(ctx)

		// olderTicketId must be expired from InactiveSet, Tickets, AND ActiveTickets (never resurrected as active!).
		_, olderInTickets := tc.Tickets.Load(olderTicketId)
		_, olderInInactive := tc.InactiveSet.Load(olderTicketId)
		_, olderInActive := tc.ActiveTickets.Load(olderTicketId)
		assert.False(t, olderInTickets, "older ticket should be deleted from Tickets")
		assert.False(t, olderInInactive, "older ticket should be deleted from InactiveSet")
		assert.False(t, olderInActive, "older ticket should not be in ActiveTickets")

		// earlyCustomId must be expired from Tickets and ActiveTickets due to custom ExpirationTime.
		_, earlyInTickets := tc.Tickets.Load(earlyCustomId)
		_, earlyInActive := tc.ActiveTickets.Load(earlyCustomId)
		assert.False(t, earlyInTickets, "early custom-expiration ticket should be deleted from Tickets")
		assert.False(t, earlyInActive, "early custom-expiration ticket should be deleted from ActiveTickets")

		// newerTicketId must still exist in Tickets and InactiveSet.
		_, newerInTickets := tc.Tickets.Load(newerTicketId)
		_, newerInInactive := tc.InactiveSet.Load(newerTicketId)
		assert.True(t, newerInTickets, "newer ticket should still exist in Tickets")
		assert.True(t, newerInInactive, "newer ticket should still exist in InactiveSet")
		assert.Empty(t, tc.SnapshotActiveTickets())
	})

	t.Run("accurate gauges after duplicate updates and expiration", func(t *testing.T) {
		cfg := config.Read()
		cfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)
		cfg.Set("OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS", 10000)

		tc := New(cfg, nil)
		now := time.Now()
		nowMs := now.UnixMilli()

		expiredId := fmt.Sprintf("%013d-1", nowMs-25000) // expired for both ticket TTL (10s) and assignment TTL (20s)
		liveId1 := fmt.Sprintf("%013d-2", nowMs)
		liveId2 := fmt.Sprintf("%013d-3", nowMs)

		for _, id := range []string{expiredId, liveId1, liveId2} {
			tk := &pb.Ticket{Id: id, ExpirationTime: timestamppb.New(now.Add(5 * time.Minute))}
			// Duplicate Ticket updates for same ID should not double-increment gauges.
			tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id}, tk, nil)
			tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id}, tk, nil)
		}

		// Duplicate Activate and Deactivate updates.
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: liveId1}, nil, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: liveId1}, nil, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: liveId2}, nil, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Deactivate, Key: liveId2}, nil, nil)

		// Duplicate Assign updates.
		assignPb := &pb.Assignment{Connection: "10.0.0.1:7777"}
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: expiredId}, nil, assignPb)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: expiredId}, nil, assignPb)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: liveId1}, nil, assignPb)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: liveId1}, nil, assignPb)

		tc.expireCacheEntries(ctx)

		// expiredId (ticket + inactive + assignment) is expired; liveId1 (active) and liveId2 (inactive) remain; liveId1 assignment remains.
		assert.Equal(t, int64(2), atomic.LoadInt64(&TicketCount))
		assert.Equal(t, int64(1), atomic.LoadInt64(&InactiveCount))
		assert.Equal(t, int64(1), atomic.LoadInt64(&AssignmentCount))
	})

	t.Run("OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE bounds deletions per cycle", func(t *testing.T) {
		cfg := config.Read()
		cfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)
		cfg.Set("OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE", 4)

		tc := New(cfg, nil)
		now := time.Now()
		expiredMs := now.UnixMilli() - 20000

		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("%013d-%d", expiredMs, i+1)
			tk := &pb.Ticket{Id: id, ExpirationTime: timestamppb.New(now.Add(5 * time.Minute))}
			tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id}, tk, nil)
		}

		// Cycle 1: deletes 4 expired tickets, 6 remain.
		tc.expireCacheEntries(ctx)
		assert.Equal(t, int64(6), atomic.LoadInt64(&TicketCount))
		assert.Equal(t, int64(6), atomic.LoadInt64(&InactiveCount))

		// Cycle 2: deletes 4 more expired tickets, 2 remain.
		tc.expireCacheEntries(ctx)
		assert.Equal(t, int64(2), atomic.LoadInt64(&TicketCount))
		assert.Equal(t, int64(2), atomic.LoadInt64(&InactiveCount))

		// Cycle 3: deletes the remaining 2 expired tickets, 0 remain.
		tc.expireCacheEntries(ctx)
		assert.Equal(t, int64(0), atomic.LoadInt64(&TicketCount))
		assert.Equal(t, int64(0), atomic.LoadInt64(&InactiveCount))
	})
}

type faultInjectingReplicator struct {
	inner  store.StateReplicator
	mutate func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse
}

func (f *faultInjectingReplicator) GetUpdates() []*store.StateUpdate {
	return f.inner.GetUpdates()
}

func (f *faultInjectingReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	res := f.inner.SendUpdates(updates)
	if f.mutate != nil {
		return f.mutate(updates, res)
	}
	return res
}

func (f *faultInjectingReplicator) GetReplIdValidator() *regexp.Regexp {
	return f.inner.GetReplIdValidator()
}

func TestOutgoingReplicationQueueDefensiveFallbacks(t *testing.T) {
	t.Parallel()

	t.Run("NilResultElement_Unpacked", func(t *testing.T) {
		t.Parallel()
		cfg := config.Read()
		cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)
		cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 1000)
		cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		faultRep := &faultInjectingReplicator{
			inner: memoryReplicator.New(cfg),
			mutate: func(_ []*store.StateUpdate, _ []*store.StateResponse) []*store.StateResponse {
				return []*store.StateResponse{nil, nil}
			},
		}
		tc := New(cfg, faultRep)
		go tc.OutgoingReplicationQueue(ctx)

		ch1 := make(chan *store.StateResponse, 1)
		ch2 := make(chan *store.StateResponse, 1)
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch1,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-1"},
		}
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch2,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "", Keys: []string{"1790307871351-2"}},
		}

		res1 := <-ch1
		require.NotNil(t, res1)
		require.EqualError(t, res1.Err, "replicator returned nil result for update")
		assert.Equal(t, "1790307871351-1", res1.Result)

		res2 := <-ch2
		require.NotNil(t, res2)
		require.EqualError(t, res2.Err, "replicator returned nil result for update")
		assert.Equal(t, "1790307871351-2", res2.Result)
	})

	t.Run("NilResultElement_Packed", func(t *testing.T) {
		t.Parallel()
		cfg := config.Read()
		cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)
		cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 1000)
		cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		faultRep := &faultInjectingReplicator{
			inner: memoryReplicator.New(cfg),
			mutate: func(_ []*store.StateUpdate, _ []*store.StateResponse) []*store.StateResponse {
				return []*store.StateResponse{nil}
			},
		}
		tc := New(cfg, faultRep)
		go tc.OutgoingReplicationQueue(ctx)

		ch1 := make(chan *store.StateResponse, 1)
		ch2 := make(chan *store.StateResponse, 1)
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch1,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-11"},
		}
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch2,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "", Keys: []string{"1790307871351-12"}},
		}

		res1 := <-ch1
		require.NotNil(t, res1)
		require.EqualError(t, res1.Err, "replicator returned nil result for update")
		assert.Equal(t, "1790307871351-11", res1.Result)

		res2 := <-ch2
		require.NotNil(t, res2)
		require.EqualError(t, res2.Err, "replicator returned nil result for update")
		assert.Equal(t, "1790307871351-12", res2.Result)
	})

	t.Run("FewerResultsThanRequested_Unpacked", func(t *testing.T) {
		t.Parallel()
		cfg := config.Read()
		cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)
		cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 1000)
		cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 3)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		faultRep := &faultInjectingReplicator{
			inner: memoryReplicator.New(cfg),
			mutate: func(_ []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
				// Return only the first result, truncating the remaining two.
				return results[:1]
			},
		}
		tc := New(cfg, faultRep)
		go tc.OutgoingReplicationQueue(ctx)

		ch1 := make(chan *store.StateResponse, 1)
		ch2 := make(chan *store.StateResponse, 1)
		ch3 := make(chan *store.StateResponse, 1)
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch1,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-21"},
		}
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch2,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-22"},
		}
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch3,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "", Keys: []string{"1790307871351-23"}},
		}

		res1 := <-ch1
		require.NotNil(t, res1)
		require.NoError(t, res1.Err)
		assert.NotEmpty(t, res1.Result)

		res2 := <-ch2
		require.NotNil(t, res2)
		require.EqualError(t, res2.Err, "replicator returned fewer results than requested")
		assert.Equal(t, "1790307871351-22", res2.Result)

		res3 := <-ch3
		require.NotNil(t, res3)
		require.EqualError(t, res3.Err, "replicator returned fewer results than requested")
		assert.Equal(t, "1790307871351-23", res3.Result)
	})

	t.Run("FewerResultsThanRequested_Packed", func(t *testing.T) {
		t.Parallel()
		cfg := config.Read()
		cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)
		cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 1000)
		cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 3)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		faultRep := &faultInjectingReplicator{
			inner: memoryReplicator.New(cfg),
			mutate: func(_ []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
				// Return only the first group's result, truncating the second group.
				return results[:1]
			},
		}
		tc := New(cfg, faultRep)
		go tc.OutgoingReplicationQueue(ctx)

		ch1 := make(chan *store.StateResponse, 1)
		ch2 := make(chan *store.StateResponse, 1)
		ch3 := make(chan *store.StateResponse, 1)
		// Group 0: Deactivate
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch1,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Deactivate, Key: "1790307871351-31"},
		}
		// Group 1: two contiguous Activate requests coalesced together
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch2,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Activate, Key: "1790307871351-32"},
		}
		tc.UpRequests <- &UpdateRequest{
			Ctx:         ctx,
			ResultsChan: ch3,
			EnqueuedAt:  time.Now(),
			Update:      store.StateUpdate{Cmd: store.Activate, Key: "", Keys: []string{"1790307871351-33"}},
		}

		res1 := <-ch1
		require.NotNil(t, res1)
		require.NoError(t, res1.Err)
		assert.NotEmpty(t, res1.Result)

		res2 := <-ch2
		require.NotNil(t, res2)
		require.EqualError(t, res2.Err, "replicator returned fewer results than requested")
		assert.Equal(t, "1790307871351-32", res2.Result)

		res3 := <-ch3
		require.NotNil(t, res3)
		require.EqualError(t, res3.Err, "replicator returned fewer results than requested")
		assert.Equal(t, "1790307871351-33", res3.Result)
	})
}
