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

package redisReplicator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/googleforgames/open-match2/v2/internal/config"
	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	memoryReplicator "github.com/googleforgames/open-match2/v2/internal/statestore/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedCmd captures a single Redis command name and its arguments sent via
// Send or Do on a test Redis connection.
type recordedCmd struct {
	cmd  string
	args []interface{}
}

// fakeRedisConn is a stateless mock redis.Conn that records sent commands and
// returns a preconfigured response or error from Do().
type fakeRedisConn struct {
	sent   []recordedCmd
	doResp interface{}
	doErr  error
}

func (f *fakeRedisConn) Close() error { return nil }
func (f *fakeRedisConn) Err() error   { return nil }
func (f *fakeRedisConn) Send(commandName string, args ...interface{}) error {
	f.sent = append(f.sent, recordedCmd{cmd: commandName, args: args})
	return nil
}
func (f *fakeRedisConn) Flush() error                  { return nil }
func (f *fakeRedisConn) Receive() (interface{}, error) { return nil, nil }
func (f *fakeRedisConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	if commandName != "" {
		f.sent = append(f.sent, recordedCmd{cmd: commandName, args: args})
	}
	return f.doResp, f.doErr
}

// fakeStreamEntry represents a single entry in the in-memory "om-replication"
// Redis stream maintained by statefulFakeRedisConn.
type fakeStreamEntry struct {
	id     string
	fields []string
}

// statefulFakeRedisConn is an in-memory Redis Streams emulator supporting
// pipelined XADD + XTRIM (via Send + Do("")) and blocking/non-blocking XREAD.
type statefulFakeRedisConn struct {
	mu       sync.Mutex
	pipeline []recordedCmd
	entries  []fakeStreamEntry
	lastMs   int64
	lastSeq  int64
}

func newStatefulFakeRedisConn() *statefulFakeRedisConn {
	return &statefulFakeRedisConn{}
}

func (c *statefulFakeRedisConn) Close() error                  { return nil }
func (c *statefulFakeRedisConn) Err() error                    { return nil }
func (c *statefulFakeRedisConn) Flush() error                  { return nil }
func (c *statefulFakeRedisConn) Receive() (interface{}, error) { return nil, nil }

func (c *statefulFakeRedisConn) Send(commandName string, args ...interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pipeline = append(c.pipeline, recordedCmd{cmd: commandName, args: args})
	return nil
}

func (c *statefulFakeRedisConn) nextStreamIDLocked() string {
	nowMs := time.Now().UnixMilli()
	if nowMs <= c.lastMs {
		c.lastSeq++
	} else {
		c.lastMs = nowMs
		c.lastSeq = 0
	}
	return fmt.Sprintf("%013d-%d", c.lastMs, c.lastSeq)
}

func (c *statefulFakeRedisConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if commandName == "" {
		queued := c.pipeline
		c.pipeline = nil
		results := make([]interface{}, 0, len(queued))
		for _, cmd := range queued {
			switch cmd.cmd {
			case "XADD":
				// args: ["om-replication", "*", field1, val1, ...]
				// Real Redis requires at least one field/value pair and an even number of field/value tokens.
				if len(cmd.args) < 4 || (len(cmd.args)-2)%2 != 0 {
					results = append(results, redis.Error("ERR wrong number of arguments for 'xadd' command"))
					continue
				}
				id := c.nextStreamIDLocked()
				fields := make([]string, 0, len(cmd.args)-2)
				for _, a := range cmd.args[2:] {
					fields = append(fields, fmt.Sprint(a))
				}
				c.entries = append(c.entries, fakeStreamEntry{
					id:     id,
					fields: fields,
				})
				results = append(results, []byte(id))
			case "XTRIM":
				// args: ["om-replication", "MINID", "~", expirationThresh]
				var trimmedCount int64
				if len(cmd.args) == 4 &&
					fmt.Sprint(cmd.args[0]) == "om-replication" &&
					fmt.Sprint(cmd.args[1]) == "MINID" &&
					fmt.Sprint(cmd.args[2]) == "~" {
					minId := fmt.Sprint(cmd.args[3])
					if !strings.Contains(minId, "-") {
						minId += "-0"
					}
					kept := make([]fakeStreamEntry, 0, len(c.entries))
					for _, entry := range c.entries {
						if store.CompareReplIds(entry.id, minId) < 0 {
							trimmedCount++
						} else {
							kept = append(kept, entry)
						}
					}
					c.entries = kept
				}
				results = append(results, trimmedCount)
			}
		}
		return results, nil
	}

	if commandName == "XREAD" {
		count := 0
		cursorId := "0-0"
		for i := 0; i < len(args); i++ {
			switch fmt.Sprint(args[i]) {
			case "COUNT":
				if i+1 < len(args) {
					count, _ = strconv.Atoi(fmt.Sprint(args[i+1]))
					i++
				}
			case "STREAMS":
				if i+2 < len(args) {
					cursorId = fmt.Sprint(args[i+2])
					i += 2
				}
			}
		}
		if !strings.Contains(cursorId, "-") {
			cursorId += "-0"
		}

		var matched []fakeStreamEntry
		for _, entry := range c.entries {
			if store.CompareReplIds(entry.id, cursorId) > 0 {
				matched = append(matched, entry)
				if count > 0 && len(matched) >= count {
					break
				}
			}
		}
		if len(matched) == 0 {
			return nil, nil
		}

		streamItems := make([]interface{}, 0, len(matched))
		for _, entry := range matched {
			fieldBytes := make([]interface{}, 0, len(entry.fields))
			for _, f := range entry.fields {
				fieldBytes = append(fieldBytes, []byte(f))
			}
			streamItems = append(streamItems, []interface{}{
				[]byte(entry.id),
				fieldBytes,
			})
		}
		return []interface{}{
			[]interface{}{
				[]byte("om-replication"),
				streamItems,
			},
		}, nil
	}

	return nil, nil
}

// newTestRedisReplicator constructs a redisReplicator backed by the given mock
// or fake redis.Conn starting from stream cursor "0-0".
func newTestRedisReplicator(conn redis.Conn) *redisReplicator {
	cfg := config.Read()
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return conn, nil
		},
	}
	return &redisReplicator{
		rConnPool:       pool,
		wConnPool:       pool,
		cfg:             cfg,
		replId:          "0-0",
		replIdValidator: regexp.MustCompile(`^\d{13}-\d+$`),
	}
}

// TestSendUpdatesReplyAlignmentWithSkippedInvalidUpdates verifies that when a
// batch sent to SendUpdates contains an invalid update in the middle (which skips
// rConn.Send("XADD", ...)), the pipelined Redis replies are still aligned with
// the corresponding valid caller input indices.
func TestSendUpdatesReplyAlignmentWithSkippedInvalidUpdates(t *testing.T) {
	conn := &fakeRedisConn{
		// Only 2 XADD commands + 1 XTRIM command are sent because update[1] fails validation.
		doResp: []interface{}{
			[]byte("1700000000010-0"),
			[]byte("1700000000020-0"),
			int64(0),
		},
	}
	rr := newTestRedisReplicator(conn)

	updates := []*store.StateUpdate{
		{
			Cmd: store.Deactivate,
			Key: "1700000000000-0",
		},
		{
			// Empty assignment Value triggers NoAssignmentErr and skips rConn.Send.
			Cmd:   store.Assign,
			Key:   "1700000000000-1",
			Value: "",
		},
		{
			Cmd: store.Activate,
			Key: "1700000000000-2",
		},
	}

	res := rr.SendUpdates(updates)
	require.Len(t, res, 3)

	// 2 XADD commands + 1 XTRIM command
	require.Len(t, conn.sent, 3)
	assert.Equal(t, "XADD", conn.sent[0].cmd)
	assert.Equal(t, "XADD", conn.sent[1].cmd)
	assert.Equal(t, "XTRIM", conn.sent[2].cmd)
	require.Len(t, conn.sent[2].args, 4)
	assert.Equal(t, "om-replication", conn.sent[2].args[0])
	assert.Equal(t, "MINID", conn.sent[2].args[1])
	assert.Equal(t, "~", conn.sent[2].args[2])
	assert.NotEmpty(t, conn.sent[2].args[3])

	assert.NoError(t, res[0].Err)
	assert.Equal(t, "1700000000010-0", res[0].Result)

	assert.ErrorIs(t, res[1].Err, NoAssignmentErr)
	assert.Equal(t, "1700000000000-1", res[1].Result)

	assert.NoError(t, res[2].Err)
	assert.Equal(t, "1700000000020-0", res[2].Result)
}

// TestSendUpdatesApproximateXTrimEvictsExpiredEntries verifies that the
// pipelined "XTRIM om-replication MINID ~ <now - TTL>" command evicts stream
// entries older than OM_CACHE_TICKET_TTL_MS while preserving unexpired
// pre-existing entries and newly added entries.
func TestSendUpdatesApproximateXTrimEvictsExpiredEntries(t *testing.T) {
	conn := newStatefulFakeRedisConn()
	rr := newTestRedisReplicator(conn)
	rr.cfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)

	nowMs := time.Now().UnixMilli()
	expiredID := fmt.Sprintf("%013d-0", nowMs-25000)
	unexpiredID := fmt.Sprintf("%013d-0", nowMs-2000)
	conn.entries = append(conn.entries,
		fakeStreamEntry{
			id:     expiredID,
			fields: []string{"deactivate", "1700000000000-0"},
		},
		fakeStreamEntry{
			id:     unexpiredID,
			fields: []string{"deactivate", "1700000000000-1"},
		},
	)

	res := rr.SendUpdates([]*store.StateUpdate{
		{
			Cmd: store.Activate,
			Key: "1700000000000-2",
		},
	})
	require.Len(t, res, 1)
	require.NoError(t, res[0].Err)

	// Expired entry older than OM_CACHE_TICKET_TTL_MS should be trimmed by XTRIM MINID ~,
	// while the unexpired pre-existing entry and newly added entry remain in the stream.
	got := rr.GetUpdates()
	require.Len(t, got, 2)
	assert.Equal(t, store.Deactivate, got[0].Cmd)
	assert.Equal(t, "1700000000000-1", got[0].Key)
	assert.Equal(t, unexpiredID, got[0].ReplId)

	assert.Equal(t, store.Activate, got[1].Cmd)
	assert.Equal(t, "1700000000000-2", got[1].Key)
	assert.Equal(t, res[0].Result, got[1].ReplId)
}

// TestSendUpdatesPackedSkipsEmptyKeys verifies that packed Activate/Deactivate
// updates skip empty strings in Keys, pad an even number of valid keys with a
// trailing "" so XADD receives an even number of field/value tokens, and reject
// updates whose Keys slice contains only empty strings.
func TestSendUpdatesPackedSkipsEmptyKeys(t *testing.T) {
	conn := &fakeRedisConn{
		doResp: []interface{}{
			[]byte("1700000000030-0"),
			[]byte("1700000000031-0"),
			int64(0),
		},
	}
	rr := newTestRedisReplicator(conn)

	updates := []*store.StateUpdate{
		{
			// 2 valid keys (even count): padded with trailing "" so XADD has an even number of field/value args.
			Cmd:  store.Deactivate,
			Keys: []string{"1700000000000-0", "", "1700000000000-2"},
		},
		{
			Cmd:  store.Activate,
			Keys: []string{"", ""},
		},
		{
			// 3 valid keys (odd count): "activate" + 3 keys = 4 field/value tokens (no trailing "" needed).
			Cmd:  store.Activate,
			Keys: []string{"1700000000000-3", "1700000000000-4", "1700000000000-5"},
		},
	}

	res := rr.SendUpdates(updates)
	require.Len(t, res, 3)

	// First and third packed updates succeed; second fails with NoTicketKeyErr.
	assert.NoError(t, res[0].Err)
	assert.Equal(t, "1700000000030-0", res[0].Result)
	assert.ErrorIs(t, res[1].Err, NoTicketKeyErr)
	assert.NoError(t, res[2].Err)
	assert.Equal(t, "1700000000031-0", res[2].Result)

	// Verify XADD for update[0] (even key count = 2) has stream key + "*" + "deactivate" + 2 keys + "" = 6 args.
	require.Len(t, conn.sent, 3)
	assert.Equal(t, "XADD", conn.sent[0].cmd)
	require.Len(t, conn.sent[0].args, 6)
	assert.Equal(t, "deactivate", conn.sent[0].args[2])
	assert.Equal(t, "1700000000000-0", conn.sent[0].args[3])
	assert.Equal(t, "1700000000000-2", conn.sent[0].args[4])
	assert.Equal(t, "", conn.sent[0].args[5])

	// Verify XADD for update[2] (odd key count = 3) has stream key + "*" + "activate" + 3 keys = 6 args.
	assert.Equal(t, "XADD", conn.sent[1].cmd)
	require.Len(t, conn.sent[1].args, 6)
	assert.Equal(t, "activate", conn.sent[1].args[2])
	assert.Equal(t, "1700000000000-3", conn.sent[1].args[3])
	assert.Equal(t, "1700000000000-4", conn.sent[1].args[4])
	assert.Equal(t, "1700000000000-5", conn.sent[1].args[5])
}

// TestFormatRedisCmdForLog verifies that formatRedisCmdForLog strips trailing
// empty padding tokens, truncates long packed argument lists with "...", and
// summarizes binary ticket protobuf payloads by byte length.
func TestFormatRedisCmdForLog(t *testing.T) {
	shortOddArgs := []interface{}{"om-replication", "*", "activate", "id1", "id2", "id3", "id4", "id5"}
	assert.Equal(t, "XADD om-replication * activate id1 id2 id3 id4 id5", formatRedisCmdForLog("XADD", shortOddArgs))

	shortEvenPaddedArgs := []interface{}{"om-replication", "*", "activate", "id1", "id2", "id3", "id4", ""}
	assert.Equal(t, "XADD om-replication * activate id1 id2 id3 id4", formatRedisCmdForLog("XADD", shortEvenPaddedArgs))

	longArgs := []interface{}{"om-replication", "*", "activate", "id1", "id2", "id3", "id4", "id5", "id6", "id7"}
	assert.Equal(t, "XADD om-replication * activate id1 id2 id3 id4 id5 ...", formatRedisCmdForLog("XADD", longArgs))

	ticketArgs := []interface{}{"om-replication", "*", "ticket", "raw-protobuf-binary-payload"}
	assert.Equal(t, "XADD om-replication * ticket <27 bytes>", formatRedisCmdForLog("XADD", ticketArgs))
}

// TestGetUpdatesPackedAndUnknownCommand verifies that GetUpdates decodes packed
// multi-key stream entries, skips unrecognized stream commands without failing,
// and still advances rr.replId past unknown commands (even when the unknown
// command is the final entry in the XREAD batch).
func TestGetUpdatesPackedAndUnknownCommand(t *testing.T) {
	// XREAD reply structure:
	// [ [ streamName, [ [ replId, [ field1, val1, field2, val2, ... ] ], ... ] ] ]
	conn := &fakeRedisConn{
		doResp: []interface{}{
			[]interface{}{
				[]byte("om-replication"),
				[]interface{}{
					[]interface{}{
						[]byte("1700000000100-0"),
						[]interface{}{
							[]byte("deactivate"), []byte("1700000000001-0"),
							[]byte("1700000000002-0"), []byte(""),
						},
					},
					[]interface{}{
						[]byte("1700000000101-0"),
						[]interface{}{
							[]byte("unknown_future_cmd"), []byte("some_payload"),
						},
					},
					[]interface{}{
						[]byte("1700000000102-0"),
						[]interface{}{
							[]byte("ticket"), []byte("serialized-ticket-bytes"),
						},
					},
					[]interface{}{
						// Trailing unknown command verifies that the default branch advances rr.replId.
						[]byte("1700000000103-0"),
						[]interface{}{
							[]byte("unknown_trailing_cmd"), []byte("ignored_payload"),
						},
					},
				},
			},
		},
	}
	rr := newTestRedisReplicator(conn)

	updates := rr.GetUpdates()
	// Unknown command entries are skipped; only the packed deactivate and ticket entries are returned.
	require.Len(t, updates, 2)

	assert.Equal(t, store.Deactivate, updates[0].Cmd)
	assert.Equal(t, "1700000000001-0", updates[0].Key)
	assert.Equal(t, []string{"1700000000001-0", "1700000000002-0"}, updates[0].Keys)
	assert.Equal(t, "1700000000100-0", updates[0].ReplId)

	assert.Equal(t, store.Ticket, updates[1].Cmd)
	assert.Equal(t, "1700000000102-0", updates[1].Key)
	assert.Equal(t, "serialized-ticket-bytes", updates[1].Value)
	assert.Equal(t, "1700000000102-0", updates[1].ReplId)

	// Cursor must advance to the trailing unknown command's stream ID so it is not re-read.
	assert.Equal(t, "1700000000103-0", rr.replId)
}

// TestReplicatorConformance runs a shared behavioral contract suite against
// both the in-memory StateReplicator and the Redis StateReplicator to verify
// identical round-trip normalization, error attribution, cursor advancement,
// and monotonic ReplId generation.
func TestReplicatorConformance(t *testing.T) {
	factories := []struct {
		name string
		make func() store.StateReplicator
	}{
		{
			name: "memory",
			make: func() store.StateReplicator {
				cfg := config.Read()
				cfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 5)
				return memoryReplicator.New(cfg)
			},
		},
		{
			name: "redis",
			make: func() store.StateReplicator {
				return newTestRedisReplicator(newStatefulFakeRedisConn())
			},
		},
	}

	for _, factory := range factories {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Run("TicketRoundTripAndInputNonMutation", func(t *testing.T) {
				rep := factory.make()
				input := &store.StateUpdate{
					Cmd:    store.Ticket,
					Key:    "caller-preset-key",
					Keys:   []string{"extraneous-key"},
					Value:  "serialized-ticket-payload",
					ReplId: "caller-preset-replid",
				}

				res := rep.SendUpdates([]*store.StateUpdate{input})
				require.Len(t, res, 1)
				require.NoError(t, res[0].Err)
				assert.True(t, rep.GetReplIdValidator().MatchString(res[0].Result), "invalid replId %q", res[0].Result)

				// Caller's input struct must not be mutated.
				assert.Equal(t, "caller-preset-key", input.Key)
				assert.Equal(t, "caller-preset-replid", input.ReplId)

				got := rep.GetUpdates()
				require.Len(t, got, 1)
				assert.Equal(t, store.Ticket, got[0].Cmd)
				assert.Equal(t, res[0].Result, got[0].Key)
				assert.Equal(t, res[0].Result, got[0].ReplId)
				assert.Equal(t, "serialized-ticket-payload", got[0].Value)
				assert.Nil(t, got[0].Keys)
			})

			t.Run("ActivateAndDeactivateSingleKeyRoundTrip", func(t *testing.T) {
				rep := factory.make()
				updates := []*store.StateUpdate{
					{
						Cmd:   store.Activate,
						Key:   "1700000000001-0",
						Value: "extraneous-activate-val",
					},
					{
						Cmd:   store.Deactivate,
						Key:   "1700000000002-0",
						Value: "extraneous-deactivate-val",
					},
				}

				res := rep.SendUpdates(updates)
				require.Len(t, res, 2)
				require.NoError(t, res[0].Err)
				require.NoError(t, res[1].Err)

				got := rep.GetUpdates()
				require.Len(t, got, 2)

				assert.Equal(t, store.Activate, got[0].Cmd)
				assert.Equal(t, "1700000000001-0", got[0].Key)
				assert.Nil(t, got[0].Keys)
				assert.Equal(t, "", got[0].Value)
				assert.Equal(t, res[0].Result, got[0].ReplId)

				assert.Equal(t, store.Deactivate, got[1].Cmd)
				assert.Equal(t, "1700000000002-0", got[1].Key)
				assert.Nil(t, got[1].Keys)
				assert.Equal(t, "", got[1].Value)
				assert.Equal(t, res[1].Result, got[1].ReplId)
			})

			t.Run("ActivateAndDeactivatePackedMultiKeyRoundTrip", func(t *testing.T) {
				rep := factory.make()
				updates := []*store.StateUpdate{
					{
						Cmd:   store.Activate,
						Key:   "ignored-key",
						Keys:  []string{"1700000000001-0", "", "1700000000002-0"},
						Value: "ignored-val",
					},
					{
						Cmd:   store.Deactivate,
						Key:   "ignored-key",
						Keys:  []string{"1700000000001-0", "", "1700000000002-0"},
						Value: "ignored-val",
					},
					{
						Cmd:   store.Activate,
						Key:   "ignored-key",
						Keys:  []string{"", "1700000000001-0"},
						Value: "ignored-val",
					},
					{
						Cmd:   store.Deactivate,
						Key:   "ignored-key",
						Keys:  []string{"", "1700000000001-0"},
						Value: "ignored-val",
					},
					{
						Cmd:   store.Activate,
						Key:   "ignored-key",
						Keys:  []string{"1700000000003-0", "1700000000004-0", "1700000000005-0"},
						Value: "ignored-val",
					},
				}

				res := rep.SendUpdates(updates)
				require.Len(t, res, 5)
				for i := range res {
					require.NoError(t, res[i].Err)
				}

				got := rep.GetUpdates()
				require.Len(t, got, 5)

				assert.Equal(t, store.Activate, got[0].Cmd)
				assert.Equal(t, "1700000000001-0", got[0].Key)
				assert.Equal(t, []string{"1700000000001-0", "1700000000002-0"}, got[0].Keys)
				assert.Equal(t, "", got[0].Value)
				assert.Equal(t, res[0].Result, got[0].ReplId)

				assert.Equal(t, store.Deactivate, got[1].Cmd)
				assert.Equal(t, "1700000000001-0", got[1].Key)
				assert.Equal(t, []string{"1700000000001-0", "1700000000002-0"}, got[1].Keys)
				assert.Equal(t, "", got[1].Value)
				assert.Equal(t, res[1].Result, got[1].ReplId)

				assert.Equal(t, store.Activate, got[2].Cmd)
				assert.Equal(t, "1700000000001-0", got[2].Key)
				assert.Nil(t, got[2].Keys)
				assert.Equal(t, "", got[2].Value)
				assert.Equal(t, res[2].Result, got[2].ReplId)

				assert.Equal(t, store.Deactivate, got[3].Cmd)
				assert.Equal(t, "1700000000001-0", got[3].Key)
				assert.Nil(t, got[3].Keys)
				assert.Equal(t, "", got[3].Value)
				assert.Equal(t, res[3].Result, got[3].ReplId)

				assert.Equal(t, store.Activate, got[4].Cmd)
				assert.Equal(t, "1700000000003-0", got[4].Key)
				assert.Equal(t, []string{"1700000000003-0", "1700000000004-0", "1700000000005-0"}, got[4].Keys)
				assert.Equal(t, "", got[4].Value)
				assert.Equal(t, res[4].Result, got[4].ReplId)
			})

			t.Run("AssignRoundTrip", func(t *testing.T) {
				rep := factory.make()
				input := &store.StateUpdate{
					Cmd:   store.Assign,
					Key:   "1700000000005-0",
					Keys:  []string{"extraneous-key-1", "extraneous-key-2"},
					Value: "10.0.0.1:7777",
				}

				res := rep.SendUpdates([]*store.StateUpdate{input})
				require.Len(t, res, 1)
				require.NoError(t, res[0].Err)

				got := rep.GetUpdates()
				require.Len(t, got, 1)
				assert.Equal(t, store.Assign, got[0].Cmd)
				assert.Equal(t, "1700000000005-0", got[0].Key)
				assert.Equal(t, "10.0.0.1:7777", got[0].Value)
				assert.Nil(t, got[0].Keys)
				assert.Equal(t, res[0].Result, got[0].ReplId)
			})

			t.Run("ValidationErrorsAndMixedBatchAlignment", func(t *testing.T) {
				rep := factory.make()

				errUpdates := []*store.StateUpdate{
					{Cmd: store.Ticket, Key: "t-err", Value: ""},
					{Cmd: store.Activate, Key: ""},
					{Cmd: store.Activate, Key: "", Keys: []string{"", ""}},
					{Cmd: store.Deactivate, Key: ""},
					{Cmd: store.Deactivate, Key: "", Keys: []string{"", ""}},
					{Cmd: store.Assign, Key: "", Value: "10.0.0.1:7777"},
					{Cmd: store.Assign, Key: "1700000000009-0", Value: ""},
					{Cmd: 999, Key: "bad-cmd-key"},
				}

				errRes := rep.SendUpdates(errUpdates)
				require.Len(t, errRes, len(errUpdates))

				assert.Error(t, errRes[0].Err)
				assert.Equal(t, "t-err", errRes[0].Result)

				assert.Error(t, errRes[1].Err)
				assert.Equal(t, "", errRes[1].Result)

				assert.Error(t, errRes[2].Err)
				assert.Equal(t, "", errRes[2].Result)

				assert.Error(t, errRes[3].Err)
				assert.Equal(t, "", errRes[3].Result)

				assert.Error(t, errRes[4].Err)
				assert.Equal(t, "", errRes[4].Result)

				assert.Error(t, errRes[5].Err)
				assert.Equal(t, "", errRes[5].Result)

				assert.Error(t, errRes[6].Err)
				assert.Equal(t, "1700000000009-0", errRes[6].Result)

				assert.Error(t, errRes[7].Err)
				assert.Equal(t, "bad-cmd-key", errRes[7].Result)

				mixedBatch := []*store.StateUpdate{
					{Cmd: store.Deactivate, Key: "1700000000010-0"},
					{Cmd: store.Assign, Key: "1700000000011-0", Value: ""},
					{Cmd: store.Activate, Key: "1700000000012-0"},
				}

				mixedRes := rep.SendUpdates(mixedBatch)
				require.Len(t, mixedRes, 3)
				assert.NoError(t, mixedRes[0].Err)
				assert.True(t, rep.GetReplIdValidator().MatchString(mixedRes[0].Result))

				assert.Error(t, mixedRes[1].Err)
				assert.Equal(t, "1700000000011-0", mixedRes[1].Result)

				assert.NoError(t, mixedRes[2].Err)
				assert.True(t, rep.GetReplIdValidator().MatchString(mixedRes[2].Result))

				got := rep.GetUpdates()
				require.Len(t, got, 2)
				assert.Equal(t, store.Deactivate, got[0].Cmd)
				assert.Equal(t, "1700000000010-0", got[0].Key)
				assert.Equal(t, mixedRes[0].Result, got[0].ReplId)

				assert.Equal(t, store.Activate, got[1].Cmd)
				assert.Equal(t, "1700000000012-0", got[1].Key)
				assert.Equal(t, mixedRes[2].Result, got[1].ReplId)
			})

			t.Run("MonotonicReplIdsAndValidator", func(t *testing.T) {
				rep := factory.make()
				batch := []*store.StateUpdate{
					{Cmd: store.Ticket, Value: "t1"},
					{Cmd: store.Activate, Key: "1700000000001-0"},
					{Cmd: store.Deactivate, Key: "1700000000001-0"},
					{Cmd: store.Assign, Key: "1700000000001-0", Value: "conn-1"},
				}

				res := rep.SendUpdates(batch)
				require.Len(t, res, len(batch))

				validator := rep.GetReplIdValidator()
				require.NotNil(t, validator)

				var prev string
				for i, r := range res {
					require.NoError(t, r.Err)
					assert.True(t, validator.MatchString(r.Result), "update %d replId %q failed regex", i, r.Result)
					if prev != "" {
						assert.Less(t, store.CompareReplIds(prev, r.Result), 0, "expected %q < %q", prev, r.Result)
					}
					prev = r.Result
				}
			})

			t.Run("IncrementalCursorAdvancementAcrossPolls", func(t *testing.T) {
				rep := factory.make()

				res1 := rep.SendUpdates([]*store.StateUpdate{
					{Cmd: store.Activate, Key: "1700000000001-0"},
				})
				require.Len(t, res1, 1)
				require.NoError(t, res1[0].Err)

				poll1 := rep.GetUpdates()
				require.Len(t, poll1, 1)
				assert.Equal(t, "1700000000001-0", poll1[0].Key)
				assert.Equal(t, res1[0].Result, poll1[0].ReplId)

				res2 := rep.SendUpdates([]*store.StateUpdate{
					{Cmd: store.Deactivate, Key: "1700000000001-0"},
				})
				require.Len(t, res2, 1)
				require.NoError(t, res2[0].Err)
				assert.Equal(t, -1, store.CompareReplIds(res1[0].Result, res2[0].Result))

				// Second poll must only return the newly sent update, not replay poll1.
				poll2 := rep.GetUpdates()
				require.Len(t, poll2, 1)
				assert.Equal(t, store.Deactivate, poll2[0].Cmd)
				assert.Equal(t, "1700000000001-0", poll2[0].Key)
				assert.Equal(t, res2[0].Result, poll2[0].ReplId)

				poll3 := rep.GetUpdates()
				assert.NotNil(t, poll3)
				assert.Empty(t, poll3)
			})

			t.Run("EmptyGetUpdatesReturnsNonNilEmptySlice", func(t *testing.T) {
				rep := factory.make()
				out := rep.GetUpdates()
				assert.NotNil(t, out)
				assert.Empty(t, out)
			})
		})
	}
}
