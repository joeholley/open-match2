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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":        "open_match",
		"component":  "statestore",
		"replicator": "memory",
	})
	NoTicketKeyErr          = errors.New("Missing ticket key")
	NoTicketDataErr         = errors.New("No ticket data")
	NoAssignmentErr         = errors.New("Missing assignment")
	InvalidInputErr         = errors.New("Invalid input")
	FullReplicationQueueErr = errors.New("Replication queue full")
)

// Local, in-memory state storage.  This mocks a subset of the
// Redis Streams functionality to provide the same surface area used by om-core.
// Used for tests and local development. NOT RECOMMENDED FOR PRODUCTION.
type memoryReplicator struct {
	mu              sync.Mutex
	cfg             *viper.Viper
	replChan        chan *store.StateUpdate
	replTS          time.Time
	replCount       int
	replIdValidator *regexp.Regexp
}

func New(cfg *viper.Viper) *memoryReplicator {
	logger.WithFields(logrus.Fields{
		"repl_id": "N/A",
	}).Debugf("Initializing cache")
	chanSize := max(cfg.GetInt("OM_CACHE_IN_MAX_UPDATES_PER_POLL"), cfg.GetInt("OM_CACHE_IN_QUEUE_BUFFER_SIZE"))
	return &memoryReplicator{
		// replIdValidator is a regular expression that can be used to match
		// replication id strings, which are redis stream entry ids.
		// https://redis.io/docs/data-types/streams/#entry-ids
		replIdValidator: regexp.MustCompile(`^\d{13}-\d+$`),
		replChan:        make(chan *store.StateUpdate, chanSize),
		replTS:          time.Now(),
		replCount:       0,
		cfg:             cfg,
	}
}

// GetUpdates mocks how the statestore/redis module processes a Redis Stream XREAD BLOCK command.
// It blocks up to OM_CACHE_IN_WAIT_TIMEOUT_MS for the first update, and once an update is
// available, drains up to OM_CACHE_IN_MAX_UPDATES_PER_POLL currently queued updates and returns.
func (rc *memoryReplicator) GetUpdates() (out []*store.StateUpdate) {
	logger := logger.WithFields(logrus.Fields{
		"direction": "getUpdates",
	})

	out = make([]*store.StateUpdate, 0)

	// Timeout
	timeoutTimer := time.NewTimer(time.Millisecond * time.Duration(rc.cfg.GetInt("OM_CACHE_IN_WAIT_TIMEOUT_MS")))
	defer timeoutTimer.Stop()
	maxUpdates := rc.cfg.GetInt("OM_CACHE_IN_MAX_UPDATES_PER_POLL")

	// Local vars
	var thisUpdate *store.StateUpdate
	more := true

	// Wait for the first available update or timeout (matching Redis XREAD BLOCK).
	select {
	case thisUpdate, more = <-rc.replChan:
		if !more {
			return out
		}
		out = append(out, thisUpdate)
	case <-timeoutTimer.C:
		return out
	}

	// Drain any additional updates already queued up to maxUpdates.
	for more && len(out) < maxUpdates {
		select {
		case thisUpdate, more = <-rc.replChan:
			if more {
				out = append(out, thisUpdate)
			}
		default:
			more = false
		}
	}

	if len(out) > 0 {
		logger.Debugf("read %v updates from state storage", len(out))
	}

	return out
}

// SendUpdates mocks how the statestore/redis module processes a Redis Stream XADD command.
// https://redis.io/docs/data-types/streams/#streams-basics
func (rc *memoryReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	logger := logger.WithFields(logrus.Fields{
		"direction": "sendUpdates",
	})

	out := make([]*store.StateResponse, len(updates))
	for i, up := range updates {
		updateCopy := store.StateUpdate{
			Cmd: up.Cmd,
		}
		var err error

		switch up.Cmd {
		case store.Ticket:
			if up.Value == "" {
				err = NoTicketDataErr
			} else {
				updateCopy.Value = up.Value
			}
		case store.Activate, store.Deactivate:
			if len(up.Keys) > 0 {
				validKeys := make([]string, 0, len(up.Keys))
				for _, k := range up.Keys {
					if k == "" {
						logger.Warn("Skipping empty ticket key in packed activate/deactivate update")
						continue
					}
					validKeys = append(validKeys, k)
				}
				if len(validKeys) == 0 {
					err = NoTicketKeyErr
				} else {
					updateCopy.Key = validKeys[0]
					if len(validKeys) > 1 {
						updateCopy.Keys = validKeys
					}
				}
			} else {
				if up.Key == "" {
					err = NoTicketKeyErr
				} else {
					updateCopy.Key = up.Key
				}
			}
		case store.Assign:
			if up.Key == "" {
				err = NoTicketKeyErr
			} else if up.Value == "" {
				err = NoAssignmentErr
			} else {
				updateCopy.Key = up.Key
				updateCopy.Value = up.Value
			}
		default:
			err = InvalidInputErr
		}

		if err != nil {
			// On error, StateResponse.Result carries the failing ticket's key so
			// callers of batched SendUpdates can identify which request failed.
			out[i] = &store.StateResponse{
				Result: up.PrimaryKey(),
				Err:    err,
			}
			continue
		}

		rc.mu.Lock()
		prevTS, prevCount := rc.replTS, rc.replCount
		replId := rc.getReplId()
		// Creating a new ticket generates a new ID in redis
		// so mock that here.
		if up.Cmd == store.Ticket {
			updateCopy.Key = replId
		}
		updateCopy.ReplId = replId
		select {
		case rc.replChan <- &updateCopy:
			out[i] = &store.StateResponse{Result: replId}
		default:
			rc.replTS, rc.replCount = prevTS, prevCount
			out[i] = &store.StateResponse{
				Result: up.PrimaryKey(),
				Err:    FullReplicationQueueErr,
			}
		}
		rc.mu.Unlock()
	}
	logger.Tracef("%v updates applied to state storage for replication (maximum number set by OM_CACHE_OUT_MAX_QUEUE_THRESHOLD config variable)", len(updates))

	return out
}

// getReplId mocks how Redis Streams generate entry IDs.
// Must be called with rc.mu held.
// https://redis.io/docs/data-types/streams/#entry-ids
func (rc *memoryReplicator) getReplId() string {
	now := time.Now()
	if now.UnixMilli() <= rc.replTS.UnixMilli() {
		rc.replCount += 1
	} else {
		rc.replTS = now
		rc.replCount = 0
	}
	id := fmt.Sprintf("%v-%v", strconv.FormatInt(rc.replTS.UnixMilli(), 10), rc.replCount)
	return id
}

// GetReplIdValidator returns a compiled regular expression that
// can be used to verify that a string has the format of a valid
// replication id (redis stream entry ID).
func (rc *memoryReplicator) GetReplIdValidator() *regexp.Regexp {
	return rc.replIdValidator
}
