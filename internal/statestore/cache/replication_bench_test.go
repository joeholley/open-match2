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

// Package cache synthetic replication load-test and benchmark harness.
//
// # Fidelity and Modeling Boundaries
//
// The benchmarks and burst stress tests in this file exercise ReplicatedTicketCache
// at three distinct fidelity tiers:
//
//  1. What memoryReplicator models directly:
//     - OutgoingReplicationQueue channel queueing (UpRequests), batch collection up to
//     OM_CACHE_OUT_MAX_QUEUE_THRESHOLD or OM_CACHE_OUT_WAIT_TIMEOUT_MS, cross-caller
//     packing (when OM_CACHE_PACK_TICKET_STATE_UPDATES=true), and per-caller ResultsChan
//     fan-out.
//     - Monotonic "<13-digit-ms>-<seq>" replication ID generation and regex validation.
//     - IncomingReplicationQueue polling limits (OM_CACHE_IN_MAX_UPDATES_PER_POLL),
//     idle, partial-poll, and full-poll waits (OM_CACHE_IN_WAIT_TIMEOUT_MS,
//     OM_CACHE_IN_POLL_WAIT_MS, OM_CACHE_IN_FULL_POLL_WAIT_MS),
//     replStream buffering (OM_CACHE_IN_QUEUE_BUFFER_SIZE), apply-loop sleeps
//     (OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS, OM_CACHE_IN_FULL_APPLY_SLEEP_MS),
//     max apply cycle bounding (OM_CACHE_IN_MAX_APPLY_DURATION_MS), protobuf unmarshaling,
//     and recordApplied watermark broadcast notifications (WaitForDeactivation).
//
//  2. What simulatedLatencyReplicator models on top of memoryReplicator:
//     - Configurable network round-trip latency on SendUpdates (sendLatency) and
//     GetUpdates (pollLatency). Injecting non-zero send/poll RTT forces requests to
//     accumulate in UpRequests while a batch write is in flight and introduces realistic
//     replication lag between write completion and local cache application.
//
//  3. What requires a real Redis instance (not modeled in-memory):
//     - Redigo connection pool contention and queueing (OM_REDIS_POOL_MAX_ACTIVE,
//     OM_REDIS_POOL_MAX_IDLE, Wait: true).
//     - RESP wire serialization/deserialization, multi-field XADD / XREAD BLOCK command
//     parsing, TCP socket buffer pressure, and Redis single-threaded server execution.
package cache

import (
	"context"
	"fmt"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// simulatedLatencyReplicator wraps a store.StateReplicator (such as memoryReplicator)
// with configurable send and poll latencies to model network RTT.
type simulatedLatencyReplicator struct {
	inner       store.StateReplicator
	sendLatency time.Duration
	pollLatency time.Duration
}

func newSimulatedLatencyReplicator(inner store.StateReplicator, sendLatency, pollLatency time.Duration) *simulatedLatencyReplicator {
	return &simulatedLatencyReplicator{
		inner:       inner,
		sendLatency: sendLatency,
		pollLatency: pollLatency,
	}
}

func (s *simulatedLatencyReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	if s.sendLatency > 0 {
		time.Sleep(s.sendLatency)
	}
	return s.inner.SendUpdates(updates)
}

func (s *simulatedLatencyReplicator) GetUpdates() []*store.StateUpdate {
	if s.pollLatency > 0 {
		time.Sleep(s.pollLatency)
	}
	return s.inner.GetUpdates()
}

func (s *simulatedLatencyReplicator) GetReplIdValidator() *regexp.Regexp {
	return s.inner.GetReplIdValidator()
}

// snapshotActiveTicketsBench constructs a point-in-time slice of active tickets
// via ReplicatedTicketCache.SnapshotActiveTickets().
func snapshotActiveTicketsBench(tc *ReplicatedTicketCache) []any {
	return tc.SnapshotActiveTickets()
}

// populateFreshCacheForExpireBench seeds the 99% non-expired entries into
// Tickets, InactiveSet, and Assignments and initializes the expiration heaps
// prior to timing.
func populateFreshCacheForExpireBench(tc *ReplicatedTicketCache, total int, now time.Time, ticketTtlMs int64) {
	expiredCount := total / 100
	freshCreationMs := now.UnixMilli()
	freshProtoTs := timestamppb.New(now.Add(time.Duration(ticketTtlMs+600000) * time.Millisecond))
	assignPb := &pb.Assignment{Connection: "10.0.0.1:7777"}

	for i := expiredCount; i < total; i++ {
		id := fmt.Sprintf("%013d-%d", freshCreationMs, i)
		t := &pb.Ticket{
			Id:             id,
			ExpirationTime: freshProtoTs,
		}
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id}, t, nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: id}, nil, assignPb)
	}
}

// seedExpiredSliceForExpireBench inserts the 1% (expiredCount) expired entries
// into Tickets, InactiveSet, Assignments, and their expiration heaps so each
// benchmarked expireCacheEntries call pops and deletes the 1% expired fraction.
func seedExpiredSliceForExpireBench(tc *ReplicatedTicketCache, expiredCount int, expiredTicketIds, expiredAssignIds []string, expiredTickets []*pb.Ticket, assignPb *pb.Assignment) {
	for i := 0; i < expiredCount; i++ {
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: expiredTicketIds[i]}, expiredTickets[i], nil)
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Assign, Key: expiredAssignIds[i]}, nil, assignPb)
	}
}

func BenchmarkExpireCacheEntries(b *testing.B) {
	for _, total := range []int{10000, 50000} {
		b.Run(fmt.Sprintf("N=%d", total), func(b *testing.B) {
			cfg := config.Read()
			ticketTtlMs := int64(600000)
			assignAdditionalTtlMs := int64(120000)
			cfg.Set("OM_CACHE_TICKET_TTL_MS", ticketTtlMs)
			cfg.Set("OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS", assignAdditionalTtlMs)

			tc := New(cfg, nil)
			now := time.Now()
			populateFreshCacheForExpireBench(tc, total, now, ticketTtlMs)

			expiredCount := total / 100
			expiredCreationMs := now.UnixMilli() - ticketTtlMs - 10000
			expiredAssignCreationMs := now.UnixMilli() - (ticketTtlMs + assignAdditionalTtlMs) - 10000
			expiredProtoTs := timestamppb.New(now.Add(-time.Duration(ticketTtlMs+10000) * time.Millisecond))
			assignPb := &pb.Assignment{Connection: "10.0.0.1:7777"}

			expiredTicketIds := make([]string, expiredCount)
			expiredAssignIds := make([]string, expiredCount)
			expiredTickets := make([]*pb.Ticket, expiredCount)
			for i := 0; i < expiredCount; i++ {
				id := fmt.Sprintf("%013d-%d", expiredCreationMs, i)
				expiredTicketIds[i] = id
				expiredAssignIds[i] = fmt.Sprintf("%013d-%d", expiredAssignCreationMs, i)
				expiredTickets[i] = &pb.Ticket{
					Id:             id,
					ExpirationTime: expiredProtoTs,
				}
			}

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				seedExpiredSliceForExpireBench(tc, expiredCount, expiredTicketIds, expiredAssignIds, expiredTickets, assignPb)
				b.StartTimer()
				tc.expireCacheEntries(ctx)
			}
		})
	}
}

func BenchmarkActiveTicketsSnapshot(b *testing.B) {
	const (
		totalTickets    = 50000
		inactiveTickets = 25000
		activeTickets   = totalTickets - inactiveTickets
	)

	cfg := config.Read()
	tc := New(cfg, nil)

	now := time.Now()
	freshCreationMs := now.UnixMilli()
	freshProtoTs := timestamppb.New(now.Add(10 * time.Minute))

	for i := 0; i < totalTickets; i++ {
		id := fmt.Sprintf("%013d-%d", freshCreationMs, i)
		t := &pb.Ticket{
			Id:             id,
			ExpirationTime: freshProtoTs,
		}
		tc.applyUpdate(&store.StateUpdate{Cmd: store.Ticket, Key: id}, t, nil)
		if i >= inactiveTickets {
			tc.applyUpdate(&store.StateUpdate{Cmd: store.Activate, Key: id}, nil, nil)
		}
	}

	initialSnap := snapshotActiveTicketsBench(tc)
	if len(initialSnap) != activeTickets {
		b.Fatalf("expected %d active tickets in initial snapshot, got %d", activeTickets, len(initialSnap))
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			snap := snapshotActiveTicketsBench(tc)
			if len(snap) != activeTickets {
				b.Fatalf("expected %d active tickets, got %d", activeTickets, len(snap))
			}
		}
	})
}

func BenchmarkReplicationThroughput(b *testing.B) {
	for _, packed := range []bool{false, true} {
		b.Run(fmt.Sprintf("packed=%v", packed), func(b *testing.B) {
			cfg := config.Read()
			cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", packed)
			cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 2)
			cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 500)
			cfg.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 2000)
			cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 2)
			cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 1)
			cfg.Set("OM_CACHE_IN_FULL_POLL_WAIT_MS", 1)
			cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 1)
			cfg.Set("OM_CACHE_IN_FULL_APPLY_SLEEP_MS", 1)
			cfg.Set("OM_CACHE_IN_MAX_APPLY_DURATION_MS", 500)
			cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", 10000)
			cfg.Set("OM_CACHE_IN_QUEUE_BUFFER_SIZE", 20000)
			cfg.Set("OM_CACHE_EXPIRATION_INTERVAL_MS", 3600000)
			cfg.Set("OM_MAX_STATE_UPDATES_PER_CALL", 1000)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tc := New(
				cfg,
				newSimulatedLatencyReplicator(
					memoryReplicator.New(cfg),
					500*time.Microsecond,
					500*time.Microsecond,
				),
			)
			go tc.OutgoingReplicationQueue(ctx)
			go tc.IncomingReplicationQueue(ctx)

			const (
				matchesPerOp    = 5
				ticketsPerMatch = 10
			)
			baseTsMs := time.Now().Add(10 * time.Minute).UnixMilli()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if packed {
					rChan := make(chan *store.StateResponse, matchesPerOp)
					var lastTicketId string
					for m := 0; m < matchesPerOp; m++ {
						keys := make([]string, ticketsPerMatch)
						for tIdx := 0; tIdx < ticketsPerMatch; tIdx++ {
							seq := (i*matchesPerOp+m)*ticketsPerMatch + tIdx
							keys[tIdx] = fmt.Sprintf("%013d-%d", baseTsMs, seq)
						}
						lastTicketId = keys[len(keys)-1]
						tc.UpRequests <- &UpdateRequest{
							Ctx:         ctx,
							ResultsChan: rChan,
							EnqueuedAt:  time.Now(),
							Update: store.StateUpdate{
								Cmd:  store.Deactivate,
								Key:  keys[0],
								Keys: keys,
							},
						}
					}
					var maxReplId string
					for m := 0; m < matchesPerOp; m++ {
						res := <-rChan
						if res.Err != nil {
							b.Fatalf("unexpected replicate error: %v", res.Err)
						}
						if store.CompareReplIds(res.Result, maxReplId) > 0 {
							maxReplId = res.Result
						}
					}
					if !tc.WaitForDeactivation(lastTicketId, maxReplId, 5*time.Second) {
						b.Fatalf("WaitForDeactivation timed out for ticket %s (replId %s)", lastTicketId, maxReplId)
					}
				} else {
					totalUpdates := matchesPerOp * ticketsPerMatch
					rChan := make(chan *store.StateResponse, totalUpdates)
					var lastTicketId string
					for u := 0; u < totalUpdates; u++ {
						seq := i*totalUpdates + u
						lastTicketId = fmt.Sprintf("%013d-%d", baseTsMs, seq)
						tc.UpRequests <- &UpdateRequest{
							Ctx:         ctx,
							ResultsChan: rChan,
							EnqueuedAt:  time.Now(),
							Update: store.StateUpdate{
								Cmd: store.Deactivate,
								Key: lastTicketId,
							},
						}
					}
					var maxReplId string
					for u := 0; u < totalUpdates; u++ {
						res := <-rChan
						if res.Err != nil {
							b.Fatalf("unexpected replicate error: %v", res.Err)
						}
						if store.CompareReplIds(res.Result, maxReplId) > 0 {
							maxReplId = res.Result
						}
					}
					if !tc.WaitForDeactivation(lastTicketId, maxReplId, 5*time.Second) {
						b.Fatalf("WaitForDeactivation timed out for ticket %s (replId %s)", lastTicketId, maxReplId)
					}
				}
			}
		})
	}
}

func TestSyntheticBurstReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst load test in short mode")
	}

	t.Run("BurstMatchDeactivations", func(t *testing.T) {
		for _, packed := range []bool{false, true} {
			packed := packed
			t.Run(fmt.Sprintf("packed=%v", packed), func(t *testing.T) {
				cfg := config.Read()
				cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", packed)
				cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)
				cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 250)
				cfg.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 2000)
				cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 5)
				cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 2)
				cfg.Set("OM_CACHE_IN_FULL_POLL_WAIT_MS", 2)
				cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 2)
				cfg.Set("OM_CACHE_IN_FULL_APPLY_SLEEP_MS", 2)
				cfg.Set("OM_CACHE_IN_MAX_APPLY_DURATION_MS", 250)
				cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", 2000)
				cfg.Set("OM_CACHE_IN_QUEUE_BUFFER_SIZE", 10000)
				cfg.Set("OM_CACHE_TICKET_TTL_MS", 600000)
				cfg.Set("OM_CACHE_EXPIRATION_INTERVAL_MS", 60000)
				cfg.Set("OM_MAX_STATE_UPDATES_PER_CALL", 1000)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				tc := New(
					cfg,
					newSimulatedLatencyReplicator(
						memoryReplicator.New(cfg),
						1*time.Millisecond,
						1*time.Millisecond,
					),
				)
				go tc.OutgoingReplicationQueue(ctx)
				go tc.IncomingReplicationQueue(ctx)

				const (
					numMatches      = 25
					ticketsPerMatch = 40
					totalTickets    = numMatches * ticketsPerMatch
				)

				ticketPayload, err := proto.Marshal(&pb.Ticket{
					ExpirationTime: timestamppb.New(time.Now().Add(10 * time.Minute)),
				})
				require.NoError(t, err)

				// 1. Concurrently create and activate 1,000 tickets across 25 match groups.
				ticketIdsByMatch := make([][]string, numMatches)
				var createWg sync.WaitGroup
				for m := 0; m < numMatches; m++ {
					m := m
					ticketIdsByMatch[m] = make([]string, ticketsPerMatch)
					createWg.Add(1)
					go func() {
						defer createWg.Done()
						createChan := make(chan *store.StateResponse, ticketsPerMatch)
						for i := 0; i < ticketsPerMatch; i++ {
							tc.UpRequests <- &UpdateRequest{
								Ctx:         ctx,
								ResultsChan: createChan,
								EnqueuedAt:  time.Now(),
								Update: store.StateUpdate{
									Cmd:   store.Ticket,
									Value: string(ticketPayload),
								},
							}
						}
						for i := 0; i < ticketsPerMatch; i++ {
							res := <-createChan
							require.NoError(t, res.Err)
							require.NotEmpty(t, res.Result)
							ticketIdsByMatch[m][i] = res.Result
						}

						// Activate the created tickets for this group.
						if packed {
							actChan := make(chan *store.StateResponse, 1)
							tc.UpRequests <- &UpdateRequest{
								Ctx:         ctx,
								ResultsChan: actChan,
								EnqueuedAt:  time.Now(),
								Update: store.StateUpdate{
									Cmd:  store.Activate,
									Key:  ticketIdsByMatch[m][0],
									Keys: ticketIdsByMatch[m],
								},
							}
							actRes := <-actChan
							require.NoError(t, actRes.Err)
						} else {
							actChan := make(chan *store.StateResponse, ticketsPerMatch)
							for _, id := range ticketIdsByMatch[m] {
								tc.UpRequests <- &UpdateRequest{
									Ctx:         ctx,
									ResultsChan: actChan,
									EnqueuedAt:  time.Now(),
									Update: store.StateUpdate{
										Cmd: store.Activate,
										Key: id,
									},
								}
							}
							for i := 0; i < ticketsPerMatch; i++ {
								actRes := <-actChan
								require.NoError(t, actRes.Err)
							}
						}
					}()
				}
				createWg.Wait()

				// 2. Drive concurrent match deactivation bursts (25 matches x 40 tickets = 1,000 deactivations)
				// while also streaming concurrent background ticket creations.
				var (
					deactWg      sync.WaitGroup
					timeoutCount int64
					replMu       sync.Mutex
					highestRepl  string
				)
				recordRepl := func(replId string) {
					replMu.Lock()
					if store.CompareReplIds(replId, highestRepl) > 0 {
						highestRepl = replId
					}
					replMu.Unlock()
				}

				// Background concurrent ticket creation during deactivation burst.
				deactWg.Add(1)
				go func() {
					defer deactWg.Done()
					extraChan := make(chan *store.StateResponse, 100)
					for i := 0; i < 100; i++ {
						tc.UpRequests <- &UpdateRequest{
							Ctx:         ctx,
							ResultsChan: extraChan,
							EnqueuedAt:  time.Now(),
							Update: store.StateUpdate{
								Cmd:   store.Ticket,
								Value: string(ticketPayload),
							},
						}
					}
					for i := 0; i < 100; i++ {
						res := <-extraChan
						require.NoError(t, res.Err)
						recordRepl(res.Result)
					}
				}()

				for m := 0; m < numMatches; m++ {
					m := m
					deactWg.Add(1)
					go func() {
						defer deactWg.Done()
						ids := ticketIdsByMatch[m]
						if packed {
							rChan := make(chan *store.StateResponse, 1)
							tc.UpRequests <- &UpdateRequest{
								Ctx:         ctx,
								ResultsChan: rChan,
								EnqueuedAt:  time.Now(),
								Update: store.StateUpdate{
									Cmd:  store.Deactivate,
									Key:  ids[0],
									Keys: ids,
								},
							}
							res := <-rChan
							require.NoError(t, res.Err)
							recordRepl(res.Result)
							for _, id := range ids {
								if !tc.WaitForDeactivation(id, res.Result, 5*time.Second) {
									atomic.AddInt64(&timeoutCount, 1)
								}
							}
						} else {
							rChan := make(chan *store.StateResponse, len(ids))
							for _, id := range ids {
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
							var matchMaxRepl string
							for range ids {
								res := <-rChan
								require.NoError(t, res.Err)
								if store.CompareReplIds(res.Result, matchMaxRepl) > 0 {
									matchMaxRepl = res.Result
								}
							}
							recordRepl(matchMaxRepl)
							for _, id := range ids {
								if !tc.WaitForDeactivation(id, matchMaxRepl, 5*time.Second) {
									atomic.AddInt64(&timeoutCount, 1)
								}
							}
						}
					}()
				}
				deactWg.Wait()

				require.Equal(t, int64(0), atomic.LoadInt64(&timeoutCount), "expected zero WaitForDeactivation timeouts")
				require.NotEmpty(t, highestRepl)
				require.Eventually(t, func() bool {
					return tc.HasAppliedReplId(highestRepl)
				}, 5*time.Second, 10*time.Millisecond, "expected AppliedReplId (%s) to reach highestRepl (%s)", tc.AppliedReplId(), highestRepl)

				for m := 0; m < numMatches; m++ {
					for _, id := range ticketIdsByMatch[m] {
						_, isInactive := tc.InactiveSet.Load(id)
						assert.True(t, isInactive, "expected ticket %s to be in InactiveSet", id)
					}
				}
			})
		}
	})

	t.Run("MassExpirationUnderLoad", func(t *testing.T) {
		cfg := config.Read()
		cfg.Set("OM_CACHE_TICKET_TTL_MS", 150)
		cfg.Set("OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS", 50)
		cfg.Set("OM_CACHE_EXPIRATION_INTERVAL_MS", 50)
		cfg.Set("OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE", 500)
		cfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)
		cfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)
		cfg.Set("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD", 250)
		cfg.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 3000)
		cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 5)
		cfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 2)
		cfg.Set("OM_CACHE_IN_FULL_POLL_WAIT_MS", 2)
		cfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 2)
		cfg.Set("OM_CACHE_IN_FULL_APPLY_SLEEP_MS", 2)
		cfg.Set("OM_CACHE_IN_MAX_APPLY_DURATION_MS", 100)
		cfg.Set("OM_CACHE_IN_MAX_UPDATES_PER_POLL", 1000)
		cfg.Set("OM_CACHE_IN_QUEUE_BUFFER_SIZE", 10000)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tc := New(
			cfg,
			newSimulatedLatencyReplicator(
				memoryReplicator.New(cfg),
				500*time.Microsecond,
				500*time.Microsecond,
			),
		)
		go tc.OutgoingReplicationQueue(ctx)
		go tc.IncomingReplicationQueue(ctx)

		// 1. Create 2,000 tickets sharing a short 150ms TTL window so they all
		// become eligible for expiration simultaneously.
		const expiringTicketCount = 2000
		shortTtlPayload, err := proto.Marshal(&pb.Ticket{
			ExpirationTime: timestamppb.New(time.Now().Add(150 * time.Millisecond)),
		})
		require.NoError(t, err)

		createChan := make(chan *store.StateResponse, expiringTicketCount)
		for i := 0; i < expiringTicketCount; i++ {
			tc.UpRequests <- &UpdateRequest{
				Ctx:         ctx,
				ResultsChan: createChan,
				EnqueuedAt:  time.Now(),
				Update: store.StateUpdate{
					Cmd:   store.Ticket,
					Value: string(shortTtlPayload),
				},
			}
		}

		expiringIds := make([]string, expiringTicketCount)
		var lastCreateReplId string
		for i := 0; i < expiringTicketCount; i++ {
			res := <-createChan
			require.NoError(t, res.Err)
			expiringIds[i] = res.Result
			if store.CompareReplIds(res.Result, lastCreateReplId) > 0 {
				lastCreateReplId = res.Result
			}
		}

		// Wait until all 2,000 tickets have been ingested by the apply loop.
		require.Eventually(t, func() bool {
			return tc.HasAppliedReplId(lastCreateReplId)
		}, 5*time.Second, 5*time.Millisecond)

		// 2. While the 2,000 tickets cross their 150ms TTL window and are swept across
		// 50ms expiration cycles, concurrently stream new Deactivate updates and assert
		// that WaitForDeactivation succeeds without stalling.
		const (
			streamWorkers          = 10
			batchesPerWorker       = 15
			ticketsPerStreamBatch  = 10
			workerPauseBetweenSend = 15 * time.Millisecond
		)
		futureTsMs := time.Now().Add(10 * time.Minute).UnixMilli()
		var (
			streamWg       sync.WaitGroup
			streamTimeouts int64
		)

		for w := 0; w < streamWorkers; w++ {
			w := w
			streamWg.Add(1)
			go func() {
				defer streamWg.Done()
				for b := 0; b < batchesPerWorker; b++ {
					keys := make([]string, ticketsPerStreamBatch)
					for k := 0; k < ticketsPerStreamBatch; k++ {
						seq := (w*batchesPerWorker+b)*ticketsPerStreamBatch + k
						keys[k] = fmt.Sprintf("%013d-%d", futureTsMs, seq)
					}
					rChan := make(chan *store.StateResponse, 1)
					tc.UpRequests <- &UpdateRequest{
						Ctx:         ctx,
						ResultsChan: rChan,
						EnqueuedAt:  time.Now(),
						Update: store.StateUpdate{
							Cmd:  store.Deactivate,
							Key:  keys[0],
							Keys: keys,
						},
					}
					res := <-rChan
					require.NoError(t, res.Err)
					if !tc.WaitForDeactivation(keys[len(keys)-1], res.Result, 3*time.Second) {
						atomic.AddInt64(&streamTimeouts, 1)
					}
					time.Sleep(workerPauseBetweenSend)
				}
			}()
		}

		streamWg.Wait()
		require.Equal(t, int64(0), atomic.LoadInt64(&streamTimeouts), "expected zero WaitForDeactivation timeouts during mass expiration")

		// 3. Assert that all 2,000 short-TTL tickets have expired and been removed
		// from both Tickets and InactiveSet, and the incoming backlog is drained.
		require.Eventually(t, func() bool {
			for _, id := range expiringIds {
				if _, ok := tc.Tickets.Load(id); ok {
					return false
				}
				if _, ok := tc.InactiveSet.Load(id); ok {
					return false
				}
			}
			return atomic.LoadInt64(&IncomingBacklogCount) == 0
		}, 5*time.Second, 20*time.Millisecond, "expected all 2,000 short-TTL tickets to be expired from Tickets and InactiveSet")
	})
}
