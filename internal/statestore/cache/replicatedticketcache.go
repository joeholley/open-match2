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
	"container/heap"
	"context"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"

	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "cache",
	})
)

// expItem and expMinHeap implement a min-heap (container/heap) ordered by
// timestamp/deadline in milliseconds (deadlineMs). ReplicatedTicketCache uses
// these heaps to track the oldest inactive entries, ticket expiration times,
// and assignments so expireCacheEntries can pop only expired items in
// O(expired * log N) time instead of scanning all sync.Map entries every cycle.
type expItem struct {
	id         string
	deadlineMs int64
}

type expMinHeap []expItem

func (h expMinHeap) Len() int           { return len(h) }
func (h expMinHeap) Less(i, j int) bool { return h[i].deadlineMs < h[j].deadlineMs }
func (h expMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *expMinHeap) Push(x any) {
	item, ok := x.(expItem)
	if !ok {
		return
	}
	*h = append(*h, item)
}

func (h *expMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// cache.UpdateRequest is basically a wrapper around a StateUpdate (in
// statestorage/datatypes/store.go) that adds context and a channel where
// results should go. The state storage layer shouldn't need to understand the
// underlying context, or where the update request originated (which is where
// it will return). These are necessary for the gRPC server in om-core,
// however!
type UpdateRequest struct {
	Ctx context.Context
	// The update itself.
	Update store.StateUpdate
	// Return channel to confirm the write by sending back the assigned ticket ID
	ResultsChan chan *store.StateResponse
	// Required: timestamp set by the producer when enqueueing the update request
	EnqueuedAt time.Time
}

// The server instantiates a replicatedTicketCache on startup, and all
// ticket-reading functionality reads from this local cache. The cache also has
// the necessary data structures for replicating ticket cache changes that come
// in to this instance by it handling gRPC calls. This data structure contains
// sync.Map members, so it should not be copied after instantiation (see
// https://pkg.go.dev/sync#Map)
type ReplicatedTicketCache struct {
	// Local copies of all the state data.
	Tickets       sync.Map
	InactiveSet   sync.Map
	ActiveTickets sync.Map
	Assignments   sync.Map

	// How this replicatedTicketCache is replicated.
	Replicator store.StateReplicator
	// The queue of cache updates
	UpRequests chan *UpdateRequest

	// Compiled regex to quickly validate redis stream replication ids
	IdValidator *regexp.Regexp

	// Application config
	Cfg *viper.Viper

	// v2.1 - waiting for deactivation before returning matches slightly
	// optimizated, using a watermark and broadcast notification for replication
	// progress.
	appliedMu     sync.RWMutex
	appliedReplId string
	appliedNotify chan struct{}

	// v2.1 - applying replcation updates and expiring items from the ticket
	// cache are now concurrent processes, so require a mutex when writing.
	// Readers remain lock-free on sync.Map.
	cacheMu          sync.Mutex
	inactiveHeap     expMinHeap
	inactiveInHeap   map[string]struct{}
	ticketHeap       expMinHeap
	assignmentHeap   expMinHeap
	assignmentInHeap map[string]struct{}

	// v2.1 - tracking active tickets in memory now, so  tracking ticket state
	// counts for otel metrics
	liveTickets     int64
	liveInactive    int64
	liveAssignments int64
}

// Init initializes the cache's configuration, state replicator, channels, and expiration heap tracking maps.
func (tc *ReplicatedTicketCache) Init(cfg *viper.Viper, r store.StateReplicator) {
	tc.Cfg = cfg
	tc.Replicator = r
	tc.UpRequests = make(chan *UpdateRequest, cfg.GetInt("OM_CACHE_OUT_QUEUE_BUFFER_SIZE"))
	tc.inactiveInHeap = make(map[string]struct{})
	tc.assignmentInHeap = make(map[string]struct{})
	tc.appliedNotify = make(chan struct{})
}

// New creates and initializes a ReplicatedTicketCache.
func New(cfg *viper.Viper, r store.StateReplicator) *ReplicatedTicketCache {
	tc := &ReplicatedTicketCache{}
	tc.Init(cfg, r)
	return tc
}

// SnapshotActiveTickets returns a point-in-time slice copy of all currently
// active tickets without acquiring cacheMu.
func (tc *ReplicatedTicketCache) SnapshotActiveTickets() []any {
	activeTickets := make([]any, 0)
	tc.ActiveTickets.Range(func(_, ticket any) bool {
		activeTickets = append(activeTickets, ticket)
		return true
	})
	return activeTickets
}

// recordApplied updates the highest applied replication ID (when non-empty) and
// wakes any goroutines waiting for replication updates to be applied to the
// local cache.
func (tc *ReplicatedTicketCache) recordApplied(replId string) {
	tc.appliedMu.Lock()
	if replId != "" && store.CompareReplIds(replId, tc.appliedReplId) > 0 {
		tc.appliedReplId = replId
	}
	close(tc.appliedNotify)
	tc.appliedNotify = make(chan struct{})
	tc.appliedMu.Unlock()
}

// AppliedReplId returns the replication ID of the most recent update applied to
// the local cache.
func (tc *ReplicatedTicketCache) AppliedReplId() string {
	tc.appliedMu.RLock()
	defer tc.appliedMu.RUnlock()
	return tc.appliedReplId
}

// HasAppliedReplId returns true if the local cache has applied all replication
// stream updates up to and including targetReplId.
func (tc *ReplicatedTicketCache) HasAppliedReplId(targetReplId string) bool {
	if targetReplId == "" {
		return false
	}
	tc.appliedMu.RLock()
	applied := tc.appliedReplId
	tc.appliedMu.RUnlock()
	return applied != "" && store.CompareReplIds(applied, targetReplId) >= 0
}

// WaitForDeactivation waits until
// - ticketId is present in InactiveSet or
// - the local cache watermark has reached targetReplId or
// - until timeout elapses.
func (tc *ReplicatedTicketCache) WaitForDeactivation(ticketId string, targetReplId string, timeout time.Duration) bool {
	if ticketId == "" && targetReplId == "" {
		return false
	}

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	for {
		// Capture the current broadcast channel before checking completion conditions
		// so a concurrent recordApplied call cannot slip in between the check and
		// the select below without closing notify.
		tc.appliedMu.Lock()
		notify := tc.appliedNotify
		tc.appliedMu.Unlock()

		if ticketId != "" {
			if _, ok := tc.InactiveSet.Load(ticketId); ok {
				return true
			}
		}
		if tc.HasAppliedReplId(targetReplId) {
			return true
		}

		select {
		case <-timeoutTimer.C:
			return false
		case <-notify:
		}
	}
}

// outgoingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server.  It processes incoming repliation events that are
// produced by gRPC handlers, and sends those events to the configured state
// storage. The updates aren't applied to the local copy of the ticket cache
// yet at this point; once the event has been successfully replicated and
// received in the incomingReplicationQueue goroutine, the update is applied to
// the local cache.
func (tc *ReplicatedTicketCache) OutgoingReplicationQueue(ctx context.Context) {
	logger := logger.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "outgoing",
	})

	logger.Debug("Listening for replication requests")
	exec := false
	pipelineRequests := make([]*UpdateRequest, 0)
	pipeline := make([]*store.StateUpdate, 0)

	for {
		// initialize variables for this loop
		exec = false
		pipelineRequests = pipelineRequests[:0]
		pipeline = pipeline[:0]
		maxThreshold := tc.Cfg.GetInt("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD")
		timeoutTimer := time.NewTimer(time.Millisecond * time.Duration(tc.Cfg.GetInt("OM_CACHE_OUT_WAIT_TIMEOUT_MS")))

		// collect currently pending requests to write to state storage using a single command (e.g. Redis Pipelining)
		for !exec {
			select {
			case req := <-tc.UpRequests:
				pipelineRequests = append(pipelineRequests, req)
				pipeline = append(pipeline, &req.Update)
				atomic.StoreInt64(&OutgoingBacklogCount, int64(len(tc.UpRequests)))

				logger.Tracef(" %v requests queued for current batch", len(pipelineRequests))
				if len(pipelineRequests) >= maxThreshold {
					// Maximum batch size reached
					otelCacheOutgoingThresholdReachedCount.Add(ctx, 1)
					logger.Trace("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD reached")
					exec = true
				}

			case <-timeoutTimer.C:
				// Timeout reached, don't wait for the batch to be full.
				otelCacheOutgoingQueueTimeouts.Add(ctx, 1)
				logger.Trace("OM_CACHE_OUT_WAIT_TIMEOUT_MS reached")
				exec = true
			}
		}
		timeoutTimer.Stop()

		// Opportunistically drain any additional already-queued requests up to maxThreshold.
		drainMore := true
		for drainMore && len(pipelineRequests) < maxThreshold {
			select {
			case req := <-tc.UpRequests:
				pipelineRequests = append(pipelineRequests, req)
				pipeline = append(pipeline, &req.Update)
			default:
				drainMore = false
			}
		}

		atomic.StoreInt64(&OutgoingBacklogCount, int64(len(tc.UpRequests)))

		// If the redis update pipeline batch job has commands to run, execute
		if len(pipelineRequests) > 0 {
			// Record the number of requests in this cycle
			logger.WithFields(logrus.Fields{
				"batch_update_count": len(pipelineRequests),
			}).Trace("sending state update batch to replicator")

			if tc.Cfg.GetBool("OM_CACHE_PACK_TICKET_STATE_UPDATES") && len(pipelineRequests) > 1 {
				maxKeysPerUpdate := tc.Cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL")
				packedPipeline, requestGroups := mergeRequestsToUpdates(pipelineRequests, maxKeysPerUpdate)
				otelCacheOutgoingUpdatesPerPoll.Record(ctx, int64(len(packedPipeline)))

				sendStart := time.Now()
				results := tc.Replicator.SendUpdates(packedPipeline)
				otelCacheOutgoingSendDuration.Record(ctx, float64(time.Since(sendStart).Microseconds())/1000.0)

				// Record the number of results received from the replicator.
				logger.WithFields(logrus.Fields{
					"result_count": len(results),
				}).Trace("state update batch results received from replicator")

				completedAt := time.Now()
				for groupIdx, result := range results {
					if groupIdx < len(requestGroups) {
						for _, reqIdx := range requestGroups[groupIdx] {
							waitMs := float64(completedAt.Sub(pipelineRequests[reqIdx].EnqueuedAt).Microseconds()) / 1000.0
							if waitMs < 0 {
								waitMs = 0
							}
							otelCacheOutgoingQueueWait.Record(ctx, waitMs)
							var resp *store.StateResponse
							if result != nil {
								resp = &store.StateResponse{
									Result: result.Result,
									Err:    result.Err,
								}
							} else {
								// Populate Result with the request's primary ticket key so the
								// caller can identify which ticket update failed.
								resp = &store.StateResponse{
									Result: pipelineRequests[reqIdx].Update.PrimaryKey(),
									Err:    errors.New("replicator returned nil result for update"),
								}
							}
							pipelineRequests[reqIdx].ResultsChan <- resp
						}
					}
				}
				for groupIdx := len(results); groupIdx < len(requestGroups); groupIdx++ {
					for _, reqIdx := range requestGroups[groupIdx] {
						pipelineRequests[reqIdx].ResultsChan <- &store.StateResponse{
							Result: pipelineRequests[reqIdx].Update.PrimaryKey(),
							Err:    errors.New("replicator returned fewer results than requested"),
						}
					}
				}
			} else {
				otelCacheOutgoingUpdatesPerPoll.Record(ctx, int64(len(pipeline)))
				sendStart := time.Now()
				results := tc.Replicator.SendUpdates(pipeline)
				otelCacheOutgoingSendDuration.Record(ctx, float64(time.Since(sendStart).Microseconds())/1000.0)

				// Record the number of results received from the replicator.
				logger.WithFields(logrus.Fields{
					"result_count": len(results),
				}).Trace("state update batch results received from replicator")

				completedAt := time.Now()
				for index, result := range results {
					if index < len(pipelineRequests) {
						waitMs := float64(completedAt.Sub(pipelineRequests[index].EnqueuedAt).Microseconds()) / 1000.0
						if waitMs < 0 {
							waitMs = 0
						}
						otelCacheOutgoingQueueWait.Record(ctx, waitMs)
						if result == nil {
							// Populate Result with the request's primary ticket key so the
							// caller can identify which ticket update failed.
							result = &store.StateResponse{
								Result: pipelineRequests[index].Update.PrimaryKey(),
								Err:    errors.New("replicator returned nil result for update"),
							}
						}
						// send back this result to it's unique return channel
						pipelineRequests[index].ResultsChan <- result
					}
				}
				for index := len(results); index < len(pipelineRequests); index++ {
					pipelineRequests[index].ResultsChan <- &store.StateResponse{
						Result: pipelineRequests[index].Update.PrimaryKey(),
						Err:    errors.New("replicator returned fewer results than requested"),
					}
				}
			}
		}
	}
}

// mergeRequestsToUpdates coalesces contiguous runs of Activate or Deactivate
// UpdateRequests into packed StateUpdate entries while preserving causal order
// across interleaved commands and isolating invalid (empty-key) requests.
func mergeRequestsToUpdates(pipelineRequests []*UpdateRequest, maxKeysPerUpdate int) ([]*store.StateUpdate, [][]int) {
	packedPipeline := make([]*store.StateUpdate, 0, len(pipelineRequests))
	requestGroups := make([][]int, 0, len(pipelineRequests))

	var activeUpdate *store.StateUpdate
	var activeKeySet map[string]struct{}

	for i, req := range pipelineRequests {
		switch req.Update.Cmd {
		case store.Activate, store.Deactivate:
			var uniqueKeys []string
			if len(req.Update.Keys) > 0 {
				seen := make(map[string]struct{}, len(req.Update.Keys))
				for _, k := range req.Update.Keys {
					if k == "" {
						logger.Warn("Skipping empty ticket key in packed activate/deactivate update")
						continue
					}
					if _, exists := seen[k]; !exists {
						seen[k] = struct{}{}
						uniqueKeys = append(uniqueKeys, k)
					}
				}
			} else if req.Update.Key != "" {
				uniqueKeys = []string{req.Update.Key}
			}

			// If the request has zero non-empty keys, do not merge it into a shared
			// run so the replicator fails only this invalid request.
			if len(uniqueKeys) == 0 {
				activeUpdate = nil
				activeKeySet = nil
				packedPipeline = append(packedPipeline, &req.Update)
				requestGroups = append(requestGroups, []int{i})
				continue
			}

			if activeUpdate != nil && activeUpdate.Cmd == req.Update.Cmd {
				var newKeys []string
				for _, k := range uniqueKeys {
					if _, exists := activeKeySet[k]; !exists {
						newKeys = append(newKeys, k)
					}
				}
				if len(activeUpdate.Keys)+len(newKeys) <= maxKeysPerUpdate {
					for _, k := range newKeys {
						activeKeySet[k] = struct{}{}
						activeUpdate.Keys = append(activeUpdate.Keys, k)
					}
					activeUpdate.Key = activeUpdate.Keys[0]
					lastGroup := len(requestGroups) - 1
					requestGroups[lastGroup] = append(requestGroups[lastGroup], i)
					continue
				}
			}

			activeKeySet = make(map[string]struct{}, len(uniqueKeys))
			keysCopy := make([]string, len(uniqueKeys))
			for idx, k := range uniqueKeys {
				activeKeySet[k] = struct{}{}
				keysCopy[idx] = k
			}
			activeUpdate = &store.StateUpdate{
				Cmd:  req.Update.Cmd,
				Key:  keysCopy[0],
				Keys: keysCopy,
			}
			packedPipeline = append(packedPipeline, activeUpdate)
			requestGroups = append(requestGroups, []int{i})

		default:
			activeUpdate = nil
			activeKeySet = nil
			packedPipeline = append(packedPipeline, &req.Update)
			requestGroups = append(requestGroups, []int{i})
		}
	}

	return packedPipeline, requestGroups
}

// incomingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server. It reads all incoming replication events from the
// configured state storage and applies them to the local ticket cache.  In
// practice, this does almost all the work for every om-core gRPC handler
// /except/ InvokeMatchMakingFunction.
//
//nolint:gocognit,cyclop,maintidx
func (tc *ReplicatedTicketCache) IncomingReplicationQueue(ctx context.Context) {

	logger := logger.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "incoming",
	})

	// Listen to the replication streams in Redis asynchronously,
	// and add updates to the channel to be processed as they come in
	inBufSize := tc.Cfg.GetInt("OM_CACHE_IN_QUEUE_BUFFER_SIZE")
	replStream := make(chan store.StateUpdate, inBufSize)
	go func() {
		for {
			maxUpdatesPerPoll := tc.Cfg.GetInt("OM_CACHE_IN_MAX_UPDATES_PER_POLL")
			waitTimeoutMs := tc.Cfg.GetInt("OM_CACHE_IN_WAIT_TIMEOUT_MS")
			pollWaitMs := tc.Cfg.GetInt("OM_CACHE_IN_POLL_WAIT_MS")
			fullPollWaitMs := tc.Cfg.GetInt("OM_CACHE_IN_FULL_POLL_WAIT_MS")

			// The GetUpdates() function blocks if there are no updates, but
			// its internal implementation respects the timeout defined in the
			// config variable OM_CACHE_IN_WAIT_TIMEOUT_MS, so this is
			// guaranteed to return in a timely fashion. It fetches up to
			// OM_CACHE_IN_MAX_UPDATES_PER_POLL updates if there are that many
			// pending.
			pollStart := time.Now()
			results := tc.Replicator.GetUpdates()
			pollDuration := time.Since(pollStart)
			otelCacheIncomingPollDuration.Record(ctx, float64(pollDuration.Microseconds())/1000.0)

			otelCacheIncomingPerPoll.Record(ctx, int64(len(results)))

			if len(results) == 0 {
				// Record that there were no updates
				otelCacheIncomingEmptyTimeouts.Add(ctx, 1)
			}

			isFullPoll := len(results) >= maxUpdatesPerPoll
			if isFullPoll {
				otelCacheIncomingFullPolls.Add(ctx, 1)
			}

			// Put the updates into the replication channel to process.
			queueStart := time.Now()
			for _, curUpdate := range results {
				logger.WithFields(logrus.Fields{
					"update.key":     curUpdate.Key,
					"update.command": curUpdate.Cmd,
				}).Trace("queueing incoming update from state storage")
				replStream <- *curUpdate
			}
			if len(results) > 0 {
				otelCacheIncomingQueueWait.Record(ctx, float64(time.Since(queueStart).Microseconds())/1000.0)
			}
			atomic.StoreInt64(&IncomingBacklogCount, int64(len(replStream)))

			// Determine the post-poll wait before issuing the next GetUpdates() call:
			// - Full poll (len(results) >= maxUpdatesPerPoll): more updates are likely
			//   still waiting in state storage; pause for OM_CACHE_IN_FULL_POLL_WAIT_MS
			//   to yield CPU before reading the next batch.
			// - Partial poll (0 < len(results) < maxUpdatesPerPoll): GetUpdates()
			//   returned immediately upon seeing available update(s); pause for
			//   OM_CACHE_IN_POLL_WAIT_MS so trickling updates coalesce into a batch
			//   in state storage instead of triggering rapid single-update polls,
			//   without waiting out the full idle OM_CACHE_IN_WAIT_TIMEOUT_MS.
			// - Empty poll (len(results) == 0): GetUpdates() normally already blocked
			//   for OM_CACHE_IN_WAIT_TIMEOUT_MS. If it returned early (for example,
			//   due to a transient state storage error), sleep for the remainder of
			//   OM_CACHE_IN_WAIT_TIMEOUT_MS to avoid tight-looping on errors.
			var postPollWait time.Duration
			switch {
			case isFullPoll:
				postPollWait = time.Duration(fullPollWaitMs) * time.Millisecond
			case len(results) > 0:
				postPollWait = time.Duration(pollWaitMs) * time.Millisecond
			default:
				if remaining := time.Duration(waitTimeoutMs)*time.Millisecond - pollDuration; remaining > 0 {
					postPollWait = remaining
				}
			}
			waitStart := time.Now()
			if postPollWait > 0 {
				time.Sleep(postPollWait)
			}
			otelCacheIncomingPollWait.Record(ctx, float64(time.Since(waitStart).Microseconds())/1000.0)
		}
	}()

	// Run local cache expiration asynchronously on its own ticker so local
	// expiration sweeps do not block applying incoming replication updates.
	expIntervalMs := tc.Cfg.GetInt("OM_CACHE_EXPIRATION_INTERVAL_MS")
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(expIntervalMs))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tc.expireCacheEntries(ctx)
			}
		}
	}()

	// Check the channel for updates, and apply them
	hitApplyTimeout := false
	for {
		// Force sleep time between applying replication updates into the local cache
		// to avoid tight looping and high cpu usage. When the previous apply cycle
		// was force-stopped by OM_CACHE_IN_MAX_APPLY_DURATION_MS with updates still
		// pending, use OM_CACHE_IN_FULL_APPLY_SLEEP_MS; otherwise use
		// OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS.
		sleepMs := tc.Cfg.GetInt("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS")
		if hitApplyTimeout {
			sleepMs = tc.Cfg.GetInt("OM_CACHE_IN_FULL_APPLY_SLEEP_MS")
		}
		sleepStart := time.Now()
		if sleepMs > 0 {
			time.Sleep(time.Millisecond * time.Duration(sleepMs))
		}
		otelCacheIncomingApplySleep.Record(ctx, float64(time.Since(sleepStart).Microseconds())/1000.0)
		done := false
		hitApplyTimeout = false

		// Maximum length of time we can process updates in a single cycle so
		// the replication goroutine yields regularly to other tasks.
		maxApplyMs := tc.Cfg.GetInt("OM_CACHE_IN_MAX_APPLY_DURATION_MS")
		applyStart := time.Now()
		updateTimer := time.NewTimer(time.Millisecond * time.Duration(maxApplyMs))
		var lastReplId string
		appliedAny := false

		var err error
		for !done {
			// Check the cycle deadline first so a full replStream cannot starve
			// the timeout branch when Go selects between ready channels.
			select {
			case <-updateTimer.C:
				otelCacheIncomingProcessingTimeouts.Add(ctx, 1)
				logger.Trace("lock hold timeout")
				hitApplyTimeout = true
				done = true
				continue
			default:
			}

			// Process all incoming updates until there are none left or the
			// cycle time limit is reached.
			select {
			case curUpdate := <-replStream:
				// Still updates to process. Unmarshal any protobuf payload
				// outside tc.cacheMu, then apply the single update under
				// tc.cacheMu via tc.applyUpdate.
				var (
					ticketPb     *pb.Ticket
					assignmentPb *pb.Assignment
				)
				switch curUpdate.Cmd {
				case store.Ticket:
					// Convert the update value back to a protobuf message for
					// storage.
					//
					// https://protobuf.dev/programming-guides/api/#use-different-messages
					// states that this is not a preferred pattern, but om-core
					// meets the criteria to be an exception:
					// "If all of the following are true:
					//
					// - your service is the storage system
					// - your system doesn't make decisions based on your
					//   clients' structured data
					// - your system simply stores, loads, and perhaps provides
					//   queries at your client's request"
					ticketPb = &pb.Ticket{}
					err = proto.Unmarshal([]byte(curUpdate.Value), ticketPb)
					if err != nil {
						logger.Error("received ticket replication could not be unmarshalled")
					}

					// Set TicketID. Must do it post-replication since
					// TicketID /is/ the Replication ID
					// (e.g. redis stream entry id)
					// This guarantees that the client can never get an
					// invalid ticketID that was not successfully stored/replicated
					ticketPb.Id = curUpdate.Key
					logger.Tracef("ticket replication received: %v", curUpdate.Key)

				case store.Activate:
					if len(curUpdate.Keys) > 0 {
						logger.Tracef("packed activation replication received for %v tickets", len(curUpdate.Keys))
					} else {
						logger.Tracef("activation replication received: %v", curUpdate.Key)
					}

				case store.Deactivate:
					if len(curUpdate.Keys) > 0 {
						logger.Tracef("packed deactivate replication received for %v tickets", len(curUpdate.Keys))
					} else {
						logger.Tracef("deactivate replication received: %v", curUpdate.Key)
					}

				case store.Assign:
					// Convert the assignment back into a protobuf message.
					assignmentPb = &pb.Assignment{}
					err = proto.Unmarshal([]byte(curUpdate.Value), assignmentPb)
					if err != nil {
						logger.Error("received assignment replication could not be unmarshalled")
					}
					logger.Tracef("**DEPRECATED** assign replication received %v:%v", curUpdate.Key, assignmentPb.GetConnection())
				}

				tc.applyUpdate(&curUpdate, ticketPb, assignmentPb)
				appliedAny = true

				if curUpdate.ReplId != "" {
					lastReplId = curUpdate.ReplId
					if ts, tsErr := store.ParseReplIdTimestampMs(curUpdate.ReplId); tsErr == nil {
						lagMs := float64(time.Now().UnixMilli() - ts)
						if lagMs < 0 {
							lagMs = 0
						}
						otelCacheReplicationLag.Record(ctx, lagMs)
					}
				}
			case <-updateTimer.C:
				// Cycle time limit exceeded
				otelCacheIncomingProcessingTimeouts.Add(ctx, 1)
				logger.Trace("lock hold timeout")
				hitApplyTimeout = true
				done = true
			default:
				// Nothing left to process; exit immediately
				logger.Trace("Incoming update queue empty")
				done = true
			}
		}
		updateTimer.Stop()
		otelCacheIncomingApplyDuration.Record(ctx, float64(time.Since(applyStart).Microseconds())/1000.0)
		atomic.StoreInt64(&IncomingBacklogCount, int64(len(replStream)))
		if appliedAny {
			tc.recordApplied(lastReplId)
		}
	}
}

const expirationLockChunkSize = 256

// applyUpdate applies a single incoming replication update to the local cache
// maps, expiration heaps, and live counters while holding tc.cacheMu.
func (tc *ReplicatedTicketCache) applyUpdate(curUpdate *store.StateUpdate, ticketPb *pb.Ticket, assignmentPb *pb.Assignment) {
	if curUpdate == nil {
		return
	}
	tc.cacheMu.Lock()
	defer tc.cacheMu.Unlock()

	switch curUpdate.Cmd {
	case store.Ticket:
		if ticketPb == nil {
			ticketPb = &pb.Ticket{}
		}
		ticketPb.Id = curUpdate.Key
		tc.storeTicketLocked(curUpdate.Key, ticketPb)
	case store.Activate:
		if len(curUpdate.Keys) > 0 {
			for _, k := range curUpdate.Keys {
				tc.activateTicketLocked(k)
			}
		} else {
			tc.activateTicketLocked(curUpdate.Key)
		}
	case store.Deactivate:
		if len(curUpdate.Keys) > 0 {
			for _, k := range curUpdate.Keys {
				tc.deactivateTicketLocked(k)
			}
		} else {
			tc.deactivateTicketLocked(curUpdate.Key)
		}
	case store.Assign:
		if assignmentPb == nil {
			assignmentPb = &pb.Assignment{}
		}
		tc.storeAssignmentLocked(curUpdate.Key, assignmentPb)
	}
}

func (tc *ReplicatedTicketCache) storeTicketLocked(id string, ticketPb *pb.Ticket) {
	if id == "" || ticketPb == nil {
		return
	}
	if _, loaded := tc.InactiveSet.LoadOrStore(id, true); !loaded {
		tc.liveInactive++
	}
	tc.ActiveTickets.Delete(id)
	if _, existed := tc.Tickets.Swap(id, ticketPb); !existed {
		tc.liveTickets++
	}
	if _, inHeap := tc.inactiveInHeap[id]; !inHeap {
		if creationMs, err := store.ParseReplIdTimestampMs(id); err == nil {
			tc.inactiveInHeap[id] = struct{}{}
			heap.Push(&tc.inactiveHeap, expItem{id: id, deadlineMs: creationMs})
		}
	}
	expireAtMs := ticketPb.GetExpirationTime().AsTime().UnixMilli()
	heap.Push(&tc.ticketHeap, expItem{id: id, deadlineMs: expireAtMs})
}

func (tc *ReplicatedTicketCache) activateTicketLocked(id string) {
	if id == "" {
		return
	}
	ticketVal, hasTicket := tc.Tickets.Load(id)
	if _, existed := tc.InactiveSet.LoadAndDelete(id); existed {
		tc.liveInactive--
	}
	if hasTicket && ticketVal != nil {
		tc.ActiveTickets.Store(id, ticketVal)
	}
}

func (tc *ReplicatedTicketCache) deactivateTicketLocked(id string) {
	if id == "" {
		return
	}
	if _, loaded := tc.InactiveSet.LoadOrStore(id, true); !loaded {
		tc.liveInactive++
	}
	tc.ActiveTickets.Delete(id)
	if _, inHeap := tc.inactiveInHeap[id]; !inHeap {
		if creationMs, err := store.ParseReplIdTimestampMs(id); err == nil {
			tc.inactiveInHeap[id] = struct{}{}
			heap.Push(&tc.inactiveHeap, expItem{id: id, deadlineMs: creationMs})
		}
	}
}

func (tc *ReplicatedTicketCache) storeAssignmentLocked(id string, assignmentPb *pb.Assignment) {
	if id == "" || assignmentPb == nil {
		return
	}
	if _, existed := tc.Assignments.Swap(id, assignmentPb); !existed {
		tc.liveAssignments++
	}
	if _, inHeap := tc.assignmentInHeap[id]; !inHeap {
		if creationMs, err := store.ParseReplIdTimestampMs(id); err == nil {
			tc.assignmentInHeap[id] = struct{}{}
			heap.Push(&tc.assignmentHeap, expItem{id: id, deadlineMs: creationMs})
		}
	}
}

func (tc *ReplicatedTicketCache) runExpireChunk(
	startTime time.Time,
	nowMs int64,
	ticketTtlMs int64,
	assignmentTtlMs int64,
	maxDeletes int,
	deletedSoFar *int,
	numInactiveDeletions *int64,
	numTicketDeletions *int64,
	numAssignmentDeletions *int64,
) (moreWork bool, numAssignments, numTickets, numInactive int64) {
	tc.cacheMu.Lock()
	defer tc.cacheMu.Unlock()

	moreWork = tc.expireChunkLocked(
		startTime,
		nowMs,
		ticketTtlMs,
		assignmentTtlMs,
		maxDeletes,
		deletedSoFar,
		numInactiveDeletions,
		numTicketDeletions,
		numAssignmentDeletions,
	)
	return moreWork, tc.liveAssignments, tc.liveTickets, tc.liveInactive
}

func (tc *ReplicatedTicketCache) expireChunkLocked(
	startTime time.Time,
	nowMs int64,
	ticketTtlMs int64,
	assignmentTtlMs int64,
	maxDeletes int,
	deletedSoFar *int,
	numInactiveDeletions *int64,
	numTicketDeletions *int64,
	numAssignmentDeletions *int64,
) bool {
	chunkOps := 0

	// 1. Cull expired tickets from the local cache inactive ticket set.
	// Ensure that when expiring a ticket from the inactive set, the ticket is
	// always deleted from ActiveTickets and Tickets BEFORE InactiveSet.LoadAndDelete
	// is visible to lock-free readers.
	for tc.inactiveHeap.Len() > 0 && *deletedSoFar < maxDeletes && chunkOps < expirationLockChunkSize {
		top := tc.inactiveHeap[0]
		if (nowMs - top.deadlineMs) <= ticketTtlMs {
			break
		}
		popped, ok := heap.Pop(&tc.inactiveHeap).(expItem)
		if !ok {
			break
		}
		chunkOps++
		delete(tc.inactiveInHeap, popped.id)
		if _, isInactive := tc.InactiveSet.Load(popped.id); isInactive {
			tc.ActiveTickets.Delete(popped.id)
			if _, existed := tc.Tickets.LoadAndDelete(popped.id); existed {
				tc.liveTickets--
				*numTicketDeletions++
			}
			if _, removed := tc.InactiveSet.LoadAndDelete(popped.id); removed {
				tc.liveInactive--
				*numInactiveDeletions++
				*deletedSoFar++
			}
		}
	}
	if *deletedSoFar >= maxDeletes {
		return false
	}
	if chunkOps >= expirationLockChunkSize {
		return true
	}

	// 2. Cull expired tickets from local cache based on the ticket's expiration time.
	for tc.ticketHeap.Len() > 0 && *deletedSoFar < maxDeletes && chunkOps < expirationLockChunkSize {
		top := tc.ticketHeap[0]
		if nowMs <= top.deadlineMs {
			break
		}
		popped, ok := heap.Pop(&tc.ticketHeap).(expItem)
		if !ok {
			break
		}
		chunkOps++
		if val, exists := tc.Tickets.Load(popped.id); exists {
			if ticketPb, tOk := val.(*pb.Ticket); tOk && ticketPb != nil {
				if startTime.After(ticketPb.GetExpirationTime().AsTime()) {
					tc.ActiveTickets.Delete(popped.id)
					if _, removed := tc.Tickets.LoadAndDelete(popped.id); removed {
						tc.liveTickets--
						*numTicketDeletions++
						*deletedSoFar++
					}
				}
			}
		}
	}
	if *deletedSoFar >= maxDeletes {
		return false
	}
	if chunkOps >= expirationLockChunkSize {
		return true
	}

	// 3. Cull expired assignments from local cache.
	for tc.assignmentHeap.Len() > 0 && *deletedSoFar < maxDeletes && chunkOps < expirationLockChunkSize {
		top := tc.assignmentHeap[0]
		if (nowMs - top.deadlineMs) <= assignmentTtlMs {
			break
		}
		popped, ok := heap.Pop(&tc.assignmentHeap).(expItem)
		if !ok {
			break
		}
		chunkOps++
		delete(tc.assignmentInHeap, popped.id)
		if _, removed := tc.Assignments.LoadAndDelete(popped.id); removed {
			tc.liveAssignments--
			*numAssignmentDeletions++
			*deletedSoFar++
		}
	}
	if *deletedSoFar >= maxDeletes {
		return false
	}
	return chunkOps >= expirationLockChunkSize
}

// expireCacheEntries removes expired tickets, inactive states, and assignments
// from the replicated ticket cache.
//
// Removal logic is as follows:
//   - ticket ids expired from the inactive list MUST also have their
//     tickets removed from the ticket cache! Any ticket that exists and
//     doesn't have it's id on the inactive list is considered active and
//     will appear in ticket pools for invoked MMFs!
//   - tickets with user-specified expiration times sooner than the
//     default MUST be removed from the cache at the user-specified time.
//     Inactive list is not affected by this as inactive list entries for
//     tickets that don't exist have no effect (except briefly taking up a
//     few bytes of memory). Dangling inactive list entries will be cleaned
//     up in expirations cycles after the configured OM ticket TTL anyway.
//   - assignments are expired after the configured OM ticket TTL AND the
//     configured OM assignment TTL have elapsed. This is to handle cases
//     where tickets expire after they were passed to invoked MMFs but
//     before they are in sessions. Such tickets are still allowed to be
//     assigned and their assignments will be retained until the
//     expiration time described above. **DEPRECATED**
func (tc *ReplicatedTicketCache) expireCacheEntries(ctx context.Context) {
	// Separate logrus instance with its own metadata to aid troubleshooting
	exLogger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicatedTicketCache",
		"operation": "expiration",
	})

	// Metrics are tallied in local variables. The values get copied to
	// the module global values that the metrics actually sample after
	// th tally is complete. This way the mid-tally values never get
	// accidentally reported to the metrics sidecar (which could happen
	// if we used the module global values to compute the tally)
	var (
		numInactive            int64
		numInactiveDeletions   int64
		numTickets             int64
		numTicketDeletions     int64
		numAssignments         int64
		numAssignmentDeletions int64
	)
	startTime := time.Now()
	nowMs := startTime.UnixMilli()
	ticketTtlMs := int64(tc.Cfg.GetInt("OM_CACHE_TICKET_TTL_MS"))
	assignmentTtlMs := ticketTtlMs + int64(tc.Cfg.GetInt("OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS"))
	maxDeletesPerCycle := tc.Cfg.GetInt("OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE")

	deletedSoFar := 0
	for {
		var moreWork bool
		moreWork, numAssignments, numTickets, numInactive = tc.runExpireChunk(
			startTime,
			nowMs,
			ticketTtlMs,
			assignmentTtlMs,
			maxDeletesPerCycle,
			&deletedSoFar,
			&numInactiveDeletions,
			&numTicketDeletions,
			&numAssignmentDeletions,
		)
		if !moreWork {
			break
		}
	}

	// Log results and record counter metrics
	atomic.StoreInt64(&AssignmentCount, numAssignments)
	atomic.StoreInt64(&TicketCount, numTickets)
	atomic.StoreInt64(&InactiveCount, numInactive)

	// Record time elapsed for histogram
	elapsed := float64(time.Since(startTime).Microseconds())
	otelCacheExpirationCycleDuration.Record(ctx, elapsed/1000.0) // OTEL measurements are in milliseconds

	// Record expiration count data for histograms
	otelCacheTicketsExpiredPerCycle.Record(ctx, int64(numTicketDeletions))
	otelCacheInactivesExpiredPerCycle.Record(ctx, int64(numInactiveDeletions))
	otelCacheAssignmentsExpiredPerCycle.Record(ctx, int64(numAssignmentDeletions))

	// Trace logging for advanced debugging
	if numAssignmentDeletions > 0 {
		exLogger.Tracef("Removed %v expired assignments from local cache", numAssignmentDeletions)
	}
	if numInactiveDeletions > 0 {
		exLogger.Tracef("%v ticket ids expired from the inactive list in local cache", numInactiveDeletions)
	}
	if numTicketDeletions > 0 {
		exLogger.Tracef("Removed %v expired tickets from local cache", numTicketDeletions)
	}
	if elapsed >= 0.01 {
		exLogger.Tracef("Local cache expiration code took %.2f us", elapsed)
	}
}
