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
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleforgames/open-match2/v2/internal/config"
	"github.com/googleforgames/open-match2/v2/internal/mmfauth"
	"github.com/googleforgames/open-match2/v2/internal/statestore/cache"
	store "github.com/googleforgames/open-match2/v2/internal/statestore/datatypes"
	memoryReplicator "github.com/googleforgames/open-match2/v2/internal/statestore/memory"
	redisReplicator "github.com/googleforgames/open-match2/v2/internal/statestore/redis"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	testLogger = logrus.WithFields(logrus.Fields{
		"app":       "open_match_test_suite",
		"component": "core",
	})

	// tcs maps each configured replication backend ("memory" and optionally "redis")
	// to its running ReplicatedTicketCache instance for cross-backend conformance tests.
	tcs = map[string]*cache.ReplicatedTicketCache{}

	// Ticket ids are using the redis stream ID format.
	// https://redis.io/docs/latest/develop/data-types/streams/#entry-ids
	// The first number is a unix timestamp at millisecond precision (13 digits
	// as of 2024). The second number is the 'sequence number', basically a
	// counter of how many entries have happened in a given unix timestamp.
	invalidTicketIds = []string{
		"",                             // empty string
		"0",                            // no separation of timestamp from sequence number
		"-1",                           // no timestamp
		"0-",                           // no sequence number
		"123456789012-1",               // invalid timestamp (13 characters required to parse as a Unix timestamp in milliseconds)
		"12345678901234-1234567890123", // timestamp at greater than millisecond precision
	}
	validTicketIds = []string{
		"1234567890123-1",             // very low sequence number
		"1234567890123-1234567890123", // very high sequence number
	}
)

const redisUri = "redis://localhost:6379"

// TestMain sets up both the in-memory mock ticket replicator and the actual
// redis ticket replicator (using local redis and falling back to
// testcontainers) so tests can run against both types and confirm they produce
// the expected results.
func TestMain(m *testing.M) {
	logrus.SetLevel(logrus.FatalLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	os.Setenv("OM_REDIS_DIAL_MAX_BACKOFF_TIMEOUT", "10s")

	redisConfig := func(uri string) {
		if !strings.Contains(uri, "://") {
			uri = "redis://" + uri
		}
		redisUrl, err := url.Parse(uri)
		if err != nil {
			testLogger.Fatalf("failed to parse connection string for redis: %s", err)
		}
		os.Setenv("REDISHOST", redisUrl.Hostname())
		os.Setenv("REDISPORT", redisUrl.Port())
		os.Setenv("OM_REDIS_READ_HOST", redisUrl.Hostname())
		os.Setenv("OM_REDIS_WRITE_HOST", redisUrl.Hostname())
		os.Setenv("OM_REDIS_READ_PORT", redisUrl.Port())
		os.Setenv("OM_REDIS_WRITE_PORT", redisUrl.Port())
	}
	redisConfig(redisUri)
	cfg = config.Read() // Read env vars into config

	// Configure metrics. The tests don't actually utilize the metrics at all,
	// but unfortunately OTEL metrics cause panics when you try to record
	// metrics without initializing them first.
	meter, otelShutdownFunc = initializeOtel()
	defer otelShutdownFunc(ctx) //nolint:errcheck
	registerMetrics(meter)
	cache.RegisterMetrics(meter)

	if os.Getenv("OM_SKIP_REDIS_TESTS") == "true" {
		testLogger.Infof("OM_SKIP_REDIS_TESTS=true; skipping redis replicator setup")
	} else {
		rr, err := redisReplicator.New(cfg)
		if err == nil {
			testLogger.Infof("Connection to local redis returned no error")
		} else {
			// do a redis testcontainer instead
			testLogger.Infof("Unable to connect to local redis; falling back to testcontainer")
			redisContainer, err := redis.RunContainer(ctx,
				// Latest Redis containers fail with
				//	"# Fatal: Can't initialize Background Jobs. Error message: Operation not permitted"
				// https://discuss.circleci.com/t/redis-fatal-cant-initialize-background-jobs/48376/8
				//testcontainers.WithImage("redis:latest"))
				//
				// This is the latest version that works in my limited testing.
				testcontainers.WithImage("redis:7.0.10"))
			if err != nil {
				log.Fatalf("failed to start redis container: %s", err)
			}
			uri, err := redisContainer.ConnectionString(ctx)
			if err != nil {
				log.Fatalf("failed to get connection string for redis container: %s", err)
			}
			testLogger.Infof("got connection string for redis container: %s", uri)
			redisConfig(uri)
			cfg = config.Read() // Read latest REDISPORT/REDISHOST env vars into config
			rr, err = redisReplicator.New(cfg)
			if err != nil {
				log.Fatalf("failed to connect to redis testcontainer: %s", err)
			}
		}

		// Set up the redis-backed replicated ticket cache
		tcs["redis"] = cache.New(cfg, rr)
	}

	// Set up the memory-backed replicated ticket cache
	tcs["memory"] = cache.New(cfg, memoryReplicator.New(cfg))

	for _, v := range tcs {
		go v.OutgoingReplicationQueue(ctx)
		go v.IncomingReplicationQueue(ctx)
	}

	code := m.Run()
	cancel()
	if code != 0 {
		os.Exit(code)
	}
}

// TestCreateTicket tests validateAndMarshalIncomingTicket() and createTicket(),
// which implement the gRPC CreateTicket() RPC defined in proto/api.proto.
//
// This does combinatorial creation of tickets with every valid attribute we
// want to test, which makes it fail complexity linter.
//
//nolint:gocognit,cyclop
func TestCreateTicket(t *testing.T) {
	maxTTL := time.Millisecond * time.Duration(cfg.GetInt("OM_CACHE_TICKET_TTL_MS"))

	// Expiration times that should fail validation across all three rejection branches:
	// 1. exTime.AsTime().Before(cTime.AsTime()) ("past", "epoch")
	// 2. exTime.AsTime().After(maxExTime) ("exceeds_max_ttl")
	// 3. !exTime.IsValid() ("out_of_protobuf_range": 10000-01-01T00:00:00Z > max 9999-12-31T23:59:59Z)
	inValidEts := map[string]func() *pb.Ticket{
		"past": func() *pb.Ticket {
			return &pb.Ticket{ExpirationTime: timestamppb.New(time.Now().Add(-time.Second))}
		},
		"epoch": func() *pb.Ticket {
			return &pb.Ticket{ExpirationTime: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}}
		},
		"exceeds_max_ttl": func() *pb.Ticket {
			return &pb.Ticket{ExpirationTime: timestamppb.New(time.Now().Add(2 * maxTTL))}
		},
		"out_of_protobuf_range": func() *pb.Ticket {
			return &pb.Ticket{ExpirationTime: &timestamppb.Timestamp{Seconds: 253402300800, Nanos: 0}}
		},
	}

	// Expected failure status for invalid expiration times
	invalidExpTimeStatus := status.New(codes.InvalidArgument,
		fmt.Sprintf("specified expiration time not between (now, now + %v)",
			cfg.GetInt("OM_CACHE_TICKET_TTL_MS")))

	// Make sure all expected invalid expiration times fail validation with InvalidArgument
	for testCaseName, makeTicket := range inValidEts {
		makeTicket := makeTicket
		t.Run(fmt.Sprintf("InvalidExpTime=%v", testCaseName), func(t *testing.T) {
			t.Parallel()
			_, err := validateAndMarshalIncomingTicket(&pb.CreateTicketRequest{Ticket: makeTicket()})
			s := status.Convert(err)
			assert.Equal(t, invalidExpTimeStatus.Code(), s.Code())
			assert.Contains(t, s.Message(), invalidExpTimeStatus.Message())
		})
	}

	// Valid filterable data attribute test cases
	tags := []*pb.Ticket_FilterableData{
		{},
		{Tags: []string{"bonusxp"}},
	}
	sas := []*pb.Ticket_FilterableData{
		{},
		{StringArgs: map[string]string{"class": "tank"}},
	}
	das := []*pb.Ticket_FilterableData{
		{},
		{DoubleArgs: map[string]float64{"mmr": 1200.0}},
	}
	cts := []*pb.Ticket_FilterableData{
		{},
		{CreationTime: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}},
		{CreationTime: timestamppb.Now()},
	}
	validEts := map[string]func() *timestamppb.Timestamp{
		"empty": func() *timestamppb.Timestamp {
			return nil
		},
		"now+halfMaxTTL": func() *timestamppb.Timestamp {
			return timestamppb.New(time.Now().Add(maxTTL / 2))
		},
		"now+maxTTL-1ms": func() *timestamppb.Timestamp {
			return timestamppb.New(time.Now().Add(maxTTL - time.Millisecond))
		},
	}

	// Generate the Cartesian product of empty and non-empty FilterableData attributes.
	attrs := []*pb.Ticket_FilterableData{}
	for _, tags := range tags {
		for _, sas := range sas {
			for _, das := range das {
				for _, ct := range cts {
					attrs = append(attrs, &pb.Ticket_FilterableData{
						Tags:         tags.GetTags(),
						StringArgs:   sas.GetStringArgs(),
						DoubleArgs:   das.GetDoubleArgs(),
						CreationTime: ct.GetCreationTime(),
					})
				}
			}
		}
	}

	// Test that all valid combinations of expiration times and attributes are parsed and replicated.
	for eCaseName, makeExpTime := range validEts {
		eCaseName := eCaseName
		makeExpTime := makeExpTime
		for _, testAttrs := range attrs {
			t.Run(fmt.Sprintf("ValidTicketAttrs-ExpTime=%v-FilterableData=%v", eCaseName, testAttrs), func(attrs *pb.Ticket_FilterableData) func(t *testing.T) {
				return func(t *testing.T) {
					t.Parallel()
					reqExpTime := makeExpTime()
					request := &pb.CreateTicketRequest{
						Ticket: &pb.Ticket{
							ExpirationTime: reqExpTime,
							Attributes:     attrs,
						},
					}
					result := &pb.Ticket{}

					// First, check that the ticket processing works correctly
					marshalledTicket, err := validateAndMarshalIncomingTicket(request)
					require.NoError(t, err)
					require.NoError(t, proto.Unmarshal(marshalledTicket, result))

					// Filterable Data Attributes being copied correctly to the new ticket?
					if len(attrs.GetTags()) > 0 {
						assert.Equal(t, attrs.GetTags(), result.GetAttributes().GetTags())
					}
					if len(attrs.GetStringArgs()) > 0 {
						assert.Equal(t, attrs.GetStringArgs(), result.GetAttributes().GetStringArgs())
					}
					if len(attrs.GetDoubleArgs()) > 0 {
						assert.Equal(t, attrs.GetDoubleArgs(), result.GetAttributes().GetDoubleArgs())
					}
					if attrs.GetCreationTime().IsValid() {
						// Note: have to use AsTime() to convert
						// timestamppb.Timestamp to time.Time because
						// timestamppb.Timestamp has a DoNotCompare pragma set in
						// its golang object definition
						assert.Equal(t, attrs.GetCreationTime().AsTime(),
							result.GetAttributes().GetCreationTime().AsTime())
					}

					// Creation and expiration time being generated or preserved correctly?
					assert.True(t, result.GetAttributes().GetCreationTime().IsValid())
					assert.True(t, result.GetExpirationTime().IsValid())
					if reqExpTime != nil {
						assert.Equal(t, reqExpTime.AsTime(), result.GetExpirationTime().AsTime())
					}

					// Check that the ticket insertion code for the replication layer of
					// the ticket cache works correctly.
					for tcTypeName, tc := range tcs {
						response, err := createTicket(context.Background(), tc, &pb.CreateTicketRequest{
							Ticket: &pb.Ticket{
								ExpirationTime: makeExpTime(),
								Attributes:     attrs,
							},
						})
						require.NoError(t, err, "Error trying to replicate ticket using %v replication", tcTypeName)
						assert.NotEmpty(t, response.GetTicketId(), "%v replication didn't provide a ticket id", tcTypeName)
					}
				}
			}(testAttrs))
		}
	}
}

// TestValidateTicketStateUpdates tests the validateTicketStateUpdates function
// used by the internal implementations of the gRPC functions ActivateTickets()
// and DeactivateTickets() defined in proto/api.proto.
func TestValidateTicketStateUpdates(t *testing.T) {
	maxUpdates := cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL")
	tooManyIds := make([]string, maxUpdates+1)
	for i := range tooManyIds {
		tooManyIds[i] = validTicketIds[0]
	}

	// Table-driven test cases
	type testcase struct {
		in                   []string
		exValidIds           []string
		exInvalidDetailsKeys []string
		exError              error
	}
	tests := map[string]*testcase{
		"empty_input": {
			in:                   []string{},
			exValidIds:           nil,
			exInvalidDetailsKeys: nil,
			exError:              NoTicketIdsErr,
		},
		"too_many_updates": {
			in:                   tooManyIds,
			exValidIds:           nil,
			exInvalidDetailsKeys: nil,
			exError:              TooManyUpdatesErr,
		},
		"valid_input": {
			in:                   validTicketIds,
			exValidIds:           validTicketIds,
			exInvalidDetailsKeys: nil,
			exError:              nil,
		},
		"invalid_input": {
			in:                   invalidTicketIds,
			exValidIds:           nil,
			exInvalidDetailsKeys: nil,
			exError:              NoValidTicketIdsErr,
		},
		"partial_valid_input": {
			in:                   append(append([]string(nil), validTicketIds...), invalidTicketIds...),
			exValidIds:           validTicketIds,
			exInvalidDetailsKeys: invalidTicketIds,
			exError:              InvalidIdErr,
		},
	}

	// Validate that all types of ticket cache replication produce the same results.
	for replType, tc := range tcs {
		// Run all tests in the table.
		for testName, thisTestCase := range tests {
			t.Run(fmt.Sprintf("ValidateTicketStateUpdates-%v-%v", replType, testName), func(thisTestCase *testcase) func(t *testing.T) {
				return func(t *testing.T) {
					t.Parallel()

					// function call
					valid, invalid, err := validateTicketStateUpdates(testLogger, tc.Replicator.GetReplIdValidator(), thisTestCase.in)

					// Check that expected list of valid ids was returned
					assert.ElementsMatch(t, valid, thisTestCase.exValidIds)

					// Expected no error
					if thisTestCase.exError == nil {
						assert.NoError(t, err)
					} else {
						// Check expected error was returned
						require.ErrorIs(t, err, thisTestCase.exError)
					}

					// Get all keys from the invalid id details map
					invalidIds := []string{}
					for k, v := range invalid {
						// expected invalid ticket id error
						require.ErrorIs(t, v, InvalidIdErr)
						invalidIds = append(invalidIds, k)
					}

					// Check that map of invalid ids errors contained every key we expected
					assert.ElementsMatch(t, invalidIds, thisTestCase.exInvalidDetailsKeys)
				}
			}(thisTestCase))
		}
	}
}

// TestActivateDeactivateTickets tests that activateTickets() and
// deactivateTickets() validate ticket IDs, attach BadRequest.FieldViolations
// for invalid IDs, and complete even when the caller's context is cancelled.
//
// activateTickets() and deactivateTickets() are the internal implementations of
// the gRPC functions ActivateTickets() and DeactivateTickets() defined in
// proto/api.proto.
//
//nolint:gocognit
func TestActivateDeactivateTickets(t *testing.T) {
	// Table-driven test cases
	type testcase struct {
		in                   []string
		exStatus             *status.Status
		exInvalidDetailsKeys []string
	}
	tests := map[string]*testcase{
		"empty_input": {
			in:                   []string{},
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.InvalidArgument, NoTicketIdsErr.Error()),
		},
		"valid_input": {
			in:                   validTicketIds,
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.OK, ""),
		},
		"invalid_input": {
			in:                   invalidTicketIds,
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.InvalidArgument, NoValidTicketIdsErr.Error()),
		},
		"partial_valid_input": {
			in:                   append(append([]string(nil), validTicketIds...), invalidTicketIds...),
			exInvalidDetailsKeys: invalidTicketIds,
			exStatus:             status.New(codes.InvalidArgument, InvalidIdErr.Error()),
		},
	}

	// Validate that all types of ticket cache replication produce the same results.
	for replType, tc := range tcs {
		// Run all tests in the table.
		for testName, thisTestCase := range tests {
			// Check that each test in the table also completes even if context
			// is cancelled before the call (these operations do not support cancellation).
			for _, abort := range []bool{false, true} {
				abortTitleString := ""
				if abort {
					abortTitleString = "[context_cancelled]"
				}

				// Check that both kinds of state update (activation and deactivation) give the expected results.
				for _, updateType := range []string{"Activate", "Deactivate"} {
					updateType := updateType
					abort := abort
					tc := tc
					t.Run(fmt.Sprintf("%vTicket-%v-%v%v", updateType, replType, testName, abortTitleString),
						func(thisTestCase *testcase) func(t *testing.T) {
							return func(t *testing.T) {
								t.Parallel()

								ctx, cancel := context.WithCancel(context.Background())
								defer cancel()
								if abort {
									cancel()
								}

								var err error
								if updateType == "Activate" {
									_, err = activateTickets(ctx, testLogger, tc, &pb.ActivateTicketsRequest{TicketIds: thisTestCase.in})
								} else if updateType == "Deactivate" {
									_, err = deactivateTickets(ctx, testLogger, tc, &pb.DeactivateTicketsRequest{TicketIds: thisTestCase.in})
								}

								// Check expected error was returned
								s := status.Convert(err)
								assert.Equal(t, thisTestCase.exStatus.Code(), s.Code())
								assert.Contains(t, s.Message(), thisTestCase.exStatus.Message())

								// Verify that the BadRequest.FieldViolations details match expected invalid IDs.
								assert.ElementsMatch(t, getStatusDetailBrvKeys(s), thisTestCase.exInvalidDetailsKeys)
							}
						}(thisTestCase))
				}
			}
		}
	}
}

// getStatusDetailBrvKeys parses the gRPC Status object and returns the ticket
// IDs from all BadRequest.FieldViolation detail messages ("ticket_id/<id>").
func getStatusDetailBrvKeys(s *status.Status) []string {
	failedKeys := []string{}
	for _, detail := range s.Details() {
		if failure, ok := detail.(*errdetails.BadRequest); ok {
			for _, violation := range failure.GetFieldViolations() {
				key, _ := strings.CutPrefix(violation.GetField(), "ticket_id/")
				testLogger.Debugf("failed id: '%v'", key)
				failedKeys = append(failedKeys, key)
			}
		}
	}
	return failedKeys
}

// TestSetDifferenceSnapshotIsolation verifies that SnapshotActiveTickets and
// setDifference produce isolated point-in-time slice snapshots that are
// unaffected by subsequent ticket creations, activations, or deactivations.
func TestSetDifferenceSnapshotIsolation(t *testing.T) {
	t.Parallel()
	localCfg := config.Read()
	localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 10)
	localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
	localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)
	localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localTc := cache.New(localCfg, memoryReplicator.New(localCfg))
	go localTc.OutgoingReplicationQueue(ctx)
	go localTc.IncomingReplicationQueue(ctx)

	resp1, err := createTicket(ctx, localTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	require.NoError(t, err)
	resp2, err := createTicket(ctx, localTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	require.NoError(t, err)
	id1 := resp1.GetTicketId()
	id2 := resp2.GetTicketId()

	_, err = activateTickets(ctx, testLogger, localTc, &pb.ActivateTicketsRequest{TicketIds: []string{id1}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(localTc.SnapshotActiveTickets()) == 1
	}, 2*time.Second, 5*time.Millisecond)

	activeSnapshot := localTc.SnapshotActiveTickets()
	diffSnapshot := setDifference(&localTc.Tickets, &localTc.InactiveSet)
	require.Len(t, activeSnapshot, 1)
	assert.Equal(t, id1, activeSnapshot[0].(*pb.Ticket).GetId())
	require.Len(t, diffSnapshot, 1)
	assert.Equal(t, id1, diffSnapshot[0].(*pb.Ticket).GetId())

	// Mutate live cache via production paths after snapshots are taken;
	// previously captured snapshot slices must not change.
	resp3, err := createTicket(ctx, localTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	require.NoError(t, err)
	id3 := resp3.GetTicketId()

	_, err = deactivateTickets(ctx, testLogger, localTc, &pb.DeactivateTicketsRequest{TicketIds: []string{id1}})
	require.NoError(t, err)
	_, err = activateTickets(ctx, testLogger, localTc, &pb.ActivateTicketsRequest{TicketIds: []string{id2, id3}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cur := localTc.SnapshotActiveTickets()
		if len(cur) != 2 {
			return false
		}
		got := []string{cur[0].(*pb.Ticket).GetId(), cur[1].(*pb.Ticket).GetId()}
		return (got[0] == id2 && got[1] == id3) || (got[0] == id3 && got[1] == id2)
	}, 2*time.Second, 5*time.Millisecond)

	require.Len(t, activeSnapshot, 1)
	assert.Equal(t, id1, activeSnapshot[0].(*pb.Ticket).GetId())
	require.Len(t, diffSnapshot, 1)
	assert.Equal(t, id1, diffSnapshot[0].(*pb.Ticket).GetId())
}

// TestWaitForDeactivation verifies that WaitForDeactivation succeeds both when
// an active ticket is deactivated into InactiveSet and when the replication
// watermark advances past targetReplId for a ticket ID not present in InactiveSet.
func TestWaitForDeactivation(t *testing.T) {
	t.Parallel()
	for replType, tcInstance := range tcs {
		t.Run("WaitForDeactivation-"+replType, func(tc *cache.ReplicatedTicketCache) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				resp, err := createTicket(ctx, tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
				require.NoError(t, err)
				ticketId := resp.GetTicketId()

				// Activate the ticket first so it is removed from InactiveSet before we test deactivation.
				actErrs, actReplId := updateTicketsActiveState(ctx, testLogger, tc, []string{ticketId}, store.Activate)
				require.Empty(t, actErrs)
				require.Eventually(t, func() bool {
					return tc.HasAppliedReplId(actReplId)
				}, 5*time.Second, 5*time.Millisecond)

				errs, maxReplId := updateTicketsActiveState(ctx, testLogger, tc, []string{ticketId}, store.Deactivate)
				require.Empty(t, errs)
				require.NotEmpty(t, maxReplId)

				// 1. Succeeds via InactiveSet + watermark for the deactivated ticket.
				ok := tc.WaitForDeactivation(ticketId, maxReplId, 5*time.Second)
				assert.True(t, ok, "expected deactivation to replicate within timeout")

				// 2. Also succeeds via watermark alone when ticketId is absent from InactiveSet.
				okWatermark := tc.WaitForDeactivation("0000000000000-0", maxReplId, 5*time.Second)
				assert.True(t, okWatermark, "expected watermark fallback to succeed when maxReplId is applied")
			}
		}(tcInstance))
	}
}

// TestPackedUpdateTicketsActiveState verifies that updateTicketsActiveState
// sends a single packed multi-key StateUpdate when
// OM_CACHE_PACK_TICKET_STATE_UPDATES=true and deactivates all specified tickets.
func TestPackedUpdateTicketsActiveState(t *testing.T) {
	t.Parallel()
	localCfg := config.Read()
	localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 20)
	localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 10)
	localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
	localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
	localCfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localTc := cache.New(localCfg, memoryReplicator.New(localCfg))
	go localTc.OutgoingReplicationQueue(ctx)
	go localTc.IncomingReplicationQueue(ctx)

	ticketIds := []string{
		"1790307871351-21",
		"1790307871351-22",
		"1790307871351-23",
	}
	errs, maxReplId := updateTicketsActiveState(ctx, testLogger, localTc, ticketIds, store.Deactivate)
	require.Empty(t, errs)
	require.NotEmpty(t, maxReplId)

	require.True(t, localTc.WaitForDeactivation(ticketIds[len(ticketIds)-1], maxReplId, 2*time.Second))
	for _, id := range ticketIds {
		_, inactive := localTc.InactiveSet.Load(id)
		assert.True(t, inactive)
	}
}

// errorInjectingReplicator wraps a store.StateReplicator and invokes onSend to
// inject custom per-update errors or modify StateResponse results.
type errorInjectingReplicator struct {
	inner  store.StateReplicator
	onSend func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse
}

func (r *errorInjectingReplicator) GetUpdates() []*store.StateUpdate {
	return r.inner.GetUpdates()
}

func (r *errorInjectingReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	res := r.inner.SendUpdates(updates)
	if r.onSend != nil {
		return r.onSend(updates, res)
	}
	return res
}

func (r *errorInjectingReplicator) GetReplIdValidator() *regexp.Regexp {
	return r.inner.GetReplIdValidator()
}

// TestUpdateTicketsActiveStateErrorAttribution verifies that
// updateTicketsActiveState attributes replication errors to the exact failing
// ticket ID even when StateResponse.Result is empty or responses arrive out of order.
func TestUpdateTicketsActiveStateErrorAttribution(t *testing.T) {
	t.Parallel()

	t.Run("ValidAndEmptyTicketIdInUnpackedMode", func(t *testing.T) {
		t.Parallel()
		localCfg := config.Read()
		localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 20)
		localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 10)
		localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 10)
		localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
		localCfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		localTc := cache.New(localCfg, memoryReplicator.New(localCfg))
		go localTc.OutgoingReplicationQueue(ctx)
		go localTc.IncomingReplicationQueue(ctx)

		ticketIds := []string{
			"1790307871351-31",
			"",
			"1790307871351-33",
		}
		errs, maxReplId := updateTicketsActiveState(ctx, testLogger, localTc, ticketIds, store.Deactivate)
		require.Len(t, errs, 1)
		require.Contains(t, errs, "")
		assert.NotContains(t, errs, "1790307871351-31")
		assert.NotContains(t, errs, "1790307871351-33")
		assert.Equal(t, codes.Internal, status.Code(errs[""]))
		assert.NotEmpty(t, maxReplId)
	})

	t.Run("EmptyResultErrorAndOutOfOrderCompletionAttributedToExactTicketId", func(t *testing.T) {
		t.Parallel()
		localCfg := config.Read()
		localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 10)
		localCfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		failingId := "1790307871351-42"
		customRep := &errorInjectingReplicator{
			inner: memoryReplicator.New(localCfg),
			onSend: func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
				for i, u := range updates {
					if u.Key == failingId {
						// Return an error with an empty Result field to exercise the
						// per-request fallback attribution path in updateTicketsActiveState.
						results[i] = &store.StateResponse{
							Result: "",
							Err:    fmt.Errorf("injected failure for %s", failingId),
						}
					}
				}
				return results
			},
		}

		localTc := cache.New(localCfg, customRep)
		// Run a custom dispatcher that completes the requests in reverse order
		// (last request first, failing middle request second, first request last)
		// to verify per-request channel attribution never depends on completion order.
		go func() {
			reqs := make([]*cache.UpdateRequest, 0, 3)
			for len(reqs) < 3 {
				select {
				case <-ctx.Done():
					return
				case req := <-localTc.UpRequests:
					reqs = append(reqs, req)
				}
			}
			updates := make([]*store.StateUpdate, len(reqs))
			for i, r := range reqs {
				updates[i] = &r.Update
			}
			res := customRep.SendUpdates(updates)
			// Deliver responses in reverse order: index 2, then 1 (failing), then 0.
			for i := len(reqs) - 1; i >= 0; i-- {
				reqs[i].ResultsChan <- res[i]
			}
		}()

		ticketIds := []string{
			"1790307871351-41",
			failingId,
			"1790307871351-43",
		}
		errs, maxReplId := updateTicketsActiveState(ctx, testLogger, localTc, ticketIds, store.Deactivate)
		require.Len(t, errs, 1)
		require.Contains(t, errs, failingId, "error must be attributed to the exact failing ticket ID")
		assert.NotContains(t, errs, "1790307871351-41")
		assert.NotContains(t, errs, "1790307871351-43")
		assert.Equal(t, codes.Internal, status.Code(errs[failingId]))
		assert.Contains(t, errs[failingId].Error(), failingId)
		assert.NotEmpty(t, maxReplId)
	})
}

// TestMatchDeactivationTimeoutAnnotation verifies that waitForMatchDeactivation
// attaches the MatchDeactivationTimeoutExtensionKey BoolValue(true) extension
// to a returned Match when deactivation replication times out or fails (if
// annotateOnTimeout is enabled), without clobbering existing match extensions.
func TestMatchDeactivationTimeoutAnnotation(t *testing.T) {
	t.Parallel()
	localCfg := config.Read()
	localCfg.Set("OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS", 25)

	// Unstarted cache so WaitForDeactivation will time out.
	localTc := cache.New(localCfg, memoryReplicator.New(localCfg))

	customExt, err := anypb.New(wrapperspb.String("mmf-custom-data"))
	require.NoError(t, err)

	t.Run("AnnotatesMatchOnDeactivationTimeoutPreservingExistingExtensions", func(t *testing.T) {
		match := &pb.Match{
			Id: "match-timeout-1",
			Extensions: map[string]*anypb.Any{
				"custom_key": customExt,
			},
		}

		replicated := waitForMatchDeactivation(
			context.Background(),
			testLogger,
			localTc,
			match,
			"test-mmf",
			"test-profile",
			"1790307871351-99",
			"1790307871351-100",
			true,
		)
		assert.False(t, replicated)
		require.NotNil(t, match.GetExtensions())
		assert.Equal(t, customExt, match.GetExtensions()["custom_key"])

		timeoutExt, exists := match.GetExtensions()[MatchDeactivationTimeoutExtensionKey]
		require.True(t, exists, "expected %s extension to be set on timeout", MatchDeactivationTimeoutExtensionKey)

		var flag wrapperspb.BoolValue
		require.NoError(t, timeoutExt.UnmarshalTo(&flag))
		assert.True(t, flag.GetValue())
	})

	t.Run("DoesNotModifyExtensionsWhenDeactivationReplicatesInTime", func(t *testing.T) {
		runningCfg := config.Read()
		runningCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 10)
		runningCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
		runningCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)
		runningCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)
		runningCfg.Set("OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS", 2000)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		runningTc := cache.New(runningCfg, memoryReplicator.New(runningCfg))
		go runningTc.OutgoingReplicationQueue(ctx)
		go runningTc.IncomingReplicationQueue(ctx)

		createResp, err := createTicket(ctx, runningTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.NoError(t, err)
		ticketId := createResp.GetTicketId()

		errs, maxReplId := updateTicketsActiveState(ctx, testLogger, runningTc, []string{ticketId}, store.Deactivate)
		require.Empty(t, errs)
		require.NotEmpty(t, maxReplId)

		match := &pb.Match{
			Id: "match-ok-1",
		}

		replicated := waitForMatchDeactivation(
			ctx,
			testLogger,
			runningTc,
			match,
			"test-mmf",
			"test-profile",
			ticketId,
			maxReplId,
			true,
		)
		assert.True(t, replicated)
		assert.Nil(t, match.GetExtensions())
	})

	t.Run("DoesNotMutateMatchWhenAnnotateOnTimeoutIsFalse", func(t *testing.T) {
		match := &pb.Match{
			Id: "match-nowait-1",
		}

		replicated := waitForMatchDeactivation(
			context.Background(),
			testLogger,
			localTc,
			match,
			"test-mmf",
			"test-profile",
			"1790307871351-99",
			"1790307871351-100",
			false,
		)
		assert.False(t, replicated)
		assert.Nil(t, match.GetExtensions())
	})

	t.Run("AnnotatesMatchWhenAllDeactivationWritesFailed", func(t *testing.T) {
		match := &pb.Match{
			Id: "match-all-failed-1",
		}

		replicated := waitForMatchDeactivation(
			context.Background(),
			testLogger,
			localTc,
			match,
			"test-mmf",
			"test-profile",
			"",
			"",
			true,
		)
		assert.False(t, replicated)
		require.NotNil(t, match.GetExtensions())
		timeoutExt, exists := match.GetExtensions()[MatchDeactivationTimeoutExtensionKey]
		require.True(t, exists)
		var flag wrapperspb.BoolValue
		require.NoError(t, timeoutExt.UnmarshalTo(&flag))
		assert.True(t, flag.GetValue())
	})
}

// TestBuildChunkedRequestsDoesNotAliasPools verifies that buildChunkedRequests
// creates an independent Pool message per chunk without mutating the caller's
// request Profile or overwriting earlier chunks' Participants slices.
func TestBuildChunkedRequestsDoesNotAliasPools(t *testing.T) {
	t.Parallel()

	extVal, err := anypb.New(wrapperspb.String("ext-value"))
	require.NoError(t, err)

	reqProfile := &pb.Profile{
		Name: "chunked-profile",
		Pools: map[string]*pb.Pool{
			"all": {
				Name: "all",
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{Tag: "mode-a"},
				},
			},
		},
		Extensions: map[string]*anypb.Any{
			"ext": extVal,
		},
	}

	chunkedPools := []map[string][]*pb.Ticket{
		{
			"all": {{Id: "1790307871351-1"}, {Id: "1790307871351-2"}},
		},
		{
			"all": {{Id: "1790307871351-3"}, {Id: "1790307871351-4"}},
		},
		{
			"all": {{Id: "1790307871351-5"}},
		},
	}

	chunks := buildChunkedRequests(reqProfile, chunkedPools)
	require.Len(t, chunks, 3)

	// The original request profile pool must not have been mutated in place.
	assert.Nil(t, reqProfile.GetPools()["all"].GetParticipants())

	expectedIDs := [][]string{
		{"1790307871351-1", "1790307871351-2"},
		{"1790307871351-3", "1790307871351-4"},
		{"1790307871351-5"},
	}

	for idx, want := range expectedIDs {
		assert.Equal(t, int32(3), chunks[idx].GetNumChunks())
		pool := chunks[idx].GetProfile().GetPools()["all"]
		require.NotNil(t, pool)
		require.Len(t, pool.GetTagPresentFilters(), 1)
		assert.Equal(t, "mode-a", pool.GetTagPresentFilters()[0].GetTag())

		var got []string
		for _, tk := range pool.GetParticipants().GetTickets() {
			got = append(got, tk.GetId())
		}
		assert.Equal(t, want, got, "chunk %d participants should not be overwritten by later chunks", idx)
	}
}

// TestBuildChunkedRequestsPreservesEmptyPoolsAndClonesIndependently verifies that
// buildChunkedRequests preserves all valid pools defined on reqProfile across
// every chunk (including pools that matched 0 tickets in all chunks or in
// subsequent chunks), excludes pools that fail filter.ValidatePoolFilters,
// populates a non-nil "<pool>_roster" Participants message with cloned pool
// filters, handles empty chunkedPools input, and never aliases *pb.Pool or
// *pb.Roster pointers across chunks or with reqProfile.
func TestBuildChunkedRequestsPreservesEmptyPoolsAndClonesIndependently(t *testing.T) {
	t.Parallel()

	extVal, err := anypb.New(wrapperspb.String("profile-ext"))
	require.NoError(t, err)

	reqProfile := &pb.Profile{
		Name: "multi-pool-profile",
		Pools: map[string]*pb.Pool{
			"pool_a": {
				Name: "pool_a",
				TagPresentFilters: []*pb.Pool_TagPresentFilter{
					{Tag: "mode-a"},
				},
			},
			"pool_b": {
				Name: "pool_b",
				StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					{StringArg: "region", Value: "us-east"},
				},
			},
			"pool_empty": {
				Name: "pool_empty",
				DoubleRangeFilters: []*pb.Pool_DoubleRangeFilter{
					{DoubleArg: "mmr", Minimum: 1000, Maximum: 2000},
				},
			},
			"pool_invalid": {
				Name: "pool_invalid",
				// Invalid CreationTimeRangeFilter (out-of-range protobuf Timestamp
				// failing GetStart().IsValid()) should be excluded by ValidatePoolFilters.
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: &timestamppb.Timestamp{Seconds: 253402300800, Nanos: 0},
					End:   timestamppb.New(time.Now().Add(time.Hour)),
				},
			},
		},
		Extensions: map[string]*anypb.Any{
			"ext": extVal,
		},
	}

	t.Run("MultiChunkWithEmptyAndPartiallyPopulatedPools", func(t *testing.T) {
		t.Parallel()
		chunkedPools := []map[string][]*pb.Ticket{
			{
				"pool_a":     {{Id: "1790307871351-1"}, {Id: "1790307871351-2"}},
				"pool_b":     {{Id: "1790307871351-3"}},
				"pool_empty": {},
			},
			{
				// In chunk 1, only pool_a received an additional ticket; pool_b and
				// pool_empty are omitted from the chunk map.
				"pool_a": {{Id: "1790307871351-4"}},
			},
		}

		chunks := buildChunkedRequests(reqProfile, chunkedPools)
		require.Len(t, chunks, 2)

		// Original request profile pools must remain untouched.
		for name, origPool := range reqProfile.GetPools() {
			assert.Nil(t, origPool.GetParticipants(), "reqProfile pool %s must not be mutated", name)
		}

		expectedByChunk := []map[string][]string{
			{
				"pool_a":     {"1790307871351-1", "1790307871351-2"},
				"pool_b":     {"1790307871351-3"},
				"pool_empty": {},
			},
			{
				"pool_a":     {"1790307871351-4"},
				"pool_b":     {},
				"pool_empty": {},
			},
		}

		for chunkIdx, wantPools := range expectedByChunk {
			assert.Equal(t, int32(2), chunks[chunkIdx].GetNumChunks())
			gotPools := chunks[chunkIdx].GetProfile().GetPools()
			require.Len(t, gotPools, 3, "chunk %d must contain the 3 valid profile pools and exclude pool_invalid", chunkIdx)
			assert.NotContains(t, gotPools, "pool_invalid", "chunk %d must not include invalid pool", chunkIdx)

			// Verify filters are preserved on every pool in every chunk.
			require.Len(t, gotPools["pool_a"].GetTagPresentFilters(), 1)
			assert.Equal(t, "mode-a", gotPools["pool_a"].GetTagPresentFilters()[0].GetTag())
			require.Len(t, gotPools["pool_b"].GetStringEqualsFilters(), 1)
			assert.Equal(t, "us-east", gotPools["pool_b"].GetStringEqualsFilters()[0].GetValue())
			require.Len(t, gotPools["pool_empty"].GetDoubleRangeFilters(), 1)
			assert.Equal(t, 1000.0, gotPools["pool_empty"].GetDoubleRangeFilters()[0].GetMinimum())

			for poolName, wantIDs := range wantPools {
				pool := gotPools[poolName]
				require.NotNil(t, pool, "chunk %d missing pool %s", chunkIdx, poolName)
				require.NotNil(t, pool.GetParticipants(), "chunk %d pool %s must have non-nil Participants roster", chunkIdx, poolName)
				assert.Equal(t, poolName+"_roster", pool.GetParticipants().GetName())

				gotIDs := make([]string, 0, len(pool.GetParticipants().GetTickets()))
				for _, tk := range pool.GetParticipants().GetTickets() {
					gotIDs = append(gotIDs, tk.GetId())
				}
				assert.Equal(t, wantIDs, gotIDs, "chunk %d pool %s ticket IDs mismatch", chunkIdx, poolName)

				// Ensure no pointer aliasing with reqProfile or across chunks.
				assert.NotSame(t, reqProfile.GetPools()[poolName], pool)
				if chunkIdx > 0 {
					prevPool := chunks[chunkIdx-1].GetProfile().GetPools()[poolName]
					assert.NotSame(t, prevPool, pool)
					assert.NotSame(t, prevPool.GetParticipants(), pool.GetParticipants())
				}
			}
		}
	})

	t.Run("NilOrEmptyChunkedPoolsSliceProducesSingleChunkWithEmptyRosters", func(t *testing.T) {
		t.Parallel()
		chunks := buildChunkedRequests(reqProfile, nil)
		require.Len(t, chunks, 1)
		assert.Equal(t, int32(1), chunks[0].GetNumChunks())

		gotPools := chunks[0].GetProfile().GetPools()
		require.Len(t, gotPools, 3)
		assert.NotContains(t, gotPools, "pool_invalid")
		for _, poolName := range []string{"pool_a", "pool_b", "pool_empty"} {
			pool := gotPools[poolName]
			require.NotNil(t, pool)
			require.NotNil(t, pool.GetParticipants())
			assert.Equal(t, poolName+"_roster", pool.GetParticipants().GetName())
			assert.Empty(t, pool.GetParticipants().GetTickets())
		}
	})
}

// fakeInvokeMMFServerStream is an in-processpb.OpenMatchService_InvokeMatchmakingFunctionsServer
// test double that records matches streamed back to the caller.
type fakeInvokeMMFServerStream struct {
	grpc.ServerStream
	ctx     context.Context
	mu      sync.Mutex
	matches []*pb.Match
}

func (f *fakeInvokeMMFServerStream) Context() context.Context {
	return f.ctx
}

func (f *fakeInvokeMMFServerStream) Send(resp *pb.StreamedMmfResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if resp.GetMatch() != nil {
		f.matches = append(f.matches, resp.GetMatch())
	}
	return nil
}

// testAuthMMFServer is an in-process gRPC MatchMakingFunctionServiceServer that
// records incoming Authorization metadata headers, optionally rejects a stale
// Bearer token with Unauthenticated, requires a specific Bearer token, and
// returns a single Match containing ticketID.
type testAuthMMFServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
	mu          sync.Mutex
	authHeaders [][]string
	matchID     string
	ticketID    string
	rejectToken string
	requireAuth string
}

func (s *testAuthMMFServer) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	auths := append([]string(nil), md.Get("authorization")...)

	s.mu.Lock()
	s.authHeaders = append(s.authHeaders, auths)
	s.mu.Unlock()

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}

	if len(auths) != 1 {
		return status.Errorf(codes.PermissionDenied, "expected exactly 1 authorization header, got %v", auths)
	}
	if s.rejectToken != "" && auths[0] == "Bearer "+s.rejectToken {
		return status.Error(codes.Unauthenticated, "token rejected by API")
	}
	if s.requireAuth != "" && auths[0] != "Bearer "+s.requireAuth {
		return status.Errorf(codes.PermissionDenied, "unexpected token %q, want %q", auths[0], "Bearer "+s.requireAuth)
	}

	return stream.Send(&pb.StreamedMmfResponse{
		Match: &pb.Match{
			Id: s.matchID,
			Rosters: map[string]*pb.Roster{
				"roster": {
					Name: "roster",
					Tickets: []*pb.Ticket{
						{Id: s.ticketID},
					},
				},
			},
		},
	})
}

// staticSeqTokenSource is a test oauth2.TokenSource that returns a sequence of
// valid Bearer token strings across successive Token() calls.
type staticSeqTokenSource struct {
	mu     sync.Mutex
	tokens []string
	idx    int
}

func (s *staticSeqTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok := s.tokens[s.idx]
	if s.idx < len(s.tokens)-1 {
		s.idx++
	}
	return &oauth2.Token{
		AccessToken: tok,
		Expiry:      time.Now().Add(time.Hour),
	}, nil
}

// newLocalTestTLS generates an ephemeral self-signed ECDSA TLS certificate for
// localhost / 127.0.0.1 and returns matching gRPC server credentials and client
// tls.Config.
func newLocalTestTLS(t *testing.T) (serverCreds credentials.TransportCredentials, clientTLS *tls.Config) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	tlsCert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{tlsCert}}), &tls.Config{RootCAs: pool}
}

var globalTestTCOnce sync.Once

// ensureGlobalTestTicketCache initializes the package-global tc instance and
// starts its background replication queue goroutines once per test binary
// execution so sequential tests exercising InvokeMatchmakingFunctions never
// overwrite tc while its replication goroutines are actively running.
func ensureGlobalTestTicketCache() {
	globalTestTCOnce.Do(func() {
		localCfg := config.Read()
		localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 10)
		localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
		localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)
		localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)

		tc = cache.ReplicatedTicketCache{}
		tc.Init(localCfg, memoryReplicator.New(localCfg))
		go tc.OutgoingReplicationQueue(context.Background())
		go tc.IncomingReplicationQueue(context.Background())
	})
}

// TestInvokeMMFsPerMMFAuthAndRefreshOnRejection verifies that
// InvokeMatchmakingFunctions mints audience-specific OIDC Bearer tokens per
// HTTPS MMF host, automatically refreshes and retries once when an MMF rejects
// a cached token with Unauthenticated, streams matches from all MMFs, and
// deactivates the matched tickets in the cache.
func TestInvokeMMFsPerMMFAuthAndRefreshOnRejection(t *testing.T) {
	serverCreds1, clientTLS := newLocalTestTLS(t)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()
	_, portStr1, err := net.SplitHostPort(lis1.Addr().String())
	require.NoError(t, err)
	port1, err := strconv.Atoi(portStr1)
	require.NoError(t, err)

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()
	_, portStr2, err := net.SplitHostPort(lis2.Addr().String())
	require.NoError(t, err)
	port2, err := strconv.Atoi(portStr2)
	require.NoError(t, err)

	// Ensure global tc has running replication queues and create and activate
	// two tickets through the production code paths.
	ensureGlobalTestTicketCache()
	tcCtx := context.Background()

	createResp1, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	require.NoError(t, err)
	createResp2, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	require.NoError(t, err)
	ticketID1 := createResp1.GetTicketId()
	ticketID2 := createResp2.GetTicketId()
	t.Cleanup(func() {
		_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
			TicketIds: []string{ticketID1, ticketID2},
		})
		require.Eventually(t, func() bool {
			return len(tc.SnapshotActiveTickets()) == 0
		}, 2*time.Second, 5*time.Millisecond)
	})

	_, err = activateTickets(tcCtx, testLogger, &tc, &pb.ActivateTicketsRequest{
		TicketIds: []string{ticketID1, ticketID2},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(tc.SnapshotActiveTickets()) == 2
	}, 2*time.Second, 5*time.Millisecond)

	mmfSrv1 := &testAuthMMFServer{
		matchID:     "match-from-mmf1",
		ticketID:    ticketID1,
		rejectToken: "stale-valid-mmf1",
		requireAuth: "fresh-valid-mmf1",
	}
	grpcSrv1 := grpc.NewServer(grpc.Creds(serverCreds1))
	pb.RegisterMatchMakingFunctionServiceServer(grpcSrv1, mmfSrv1)
	go grpcSrv1.Serve(lis1) //nolint:errcheck
	defer grpcSrv1.Stop()

	mmfSrv2 := &testAuthMMFServer{
		matchID:     "match-from-mmf2",
		ticketID:    ticketID2,
		requireAuth: "valid-mmf2",
	}
	grpcSrv2 := grpc.NewServer(grpc.Creds(serverCreds1))
	pb.RegisterMatchMakingFunctionServiceServer(grpcSrv2, mmfSrv2)
	go grpcSrv2.Serve(lis2) //nolint:errcheck
	defer grpcSrv2.Stop()

	prevTLS := tlsConfig
	prevTokens := mmfIDTokens
	tlsConfig = clientTLS
	var mmf1SourceCalls atomic.Int32
	var mmf2SourceCalls atomic.Int32
	mmfIDTokens = mmfauth.NewIDTokenCache(func(_ context.Context, audience string) (oauth2.TokenSource, error) {
		switch audience {
		case "https://localhost":
			n := mmf1SourceCalls.Add(1)
			if n == 1 {
				return &staticSeqTokenSource{tokens: []string{"stale-valid-mmf1"}}, nil
			}
			return &staticSeqTokenSource{tokens: []string{"fresh-valid-mmf1"}}, nil
		case "https://127.0.0.1":
			mmf2SourceCalls.Add(1)
			return &staticSeqTokenSource{tokens: []string{"valid-mmf2"}}, nil
		default:
			return nil, fmt.Errorf("unexpected audience %s", audience)
		}
	})
	defer func() {
		tlsConfig = prevTLS
		mmfIDTokens = prevTokens
	}()

	req := &pb.MmfRequest{
		Profile: &pb.Profile{
			Name: "auth-test-profile",
			Pools: map[string]*pb.Pool{
				"all": {
					Name: "all",
					CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
						Start: timestamppb.New(time.Now().Add(-time.Hour)),
						End:   timestamppb.New(time.Now().Add(time.Hour)),
					},
				},
			},
		},
		Mmfs: []*pb.MatchmakingFunctionSpec{
			{
				Name: "mmf-1",
				Host: "https://localhost",
				Port: int32(port1),
				Type: pb.MatchmakingFunctionSpec_GRPC,
			},
			{
				Name: "mmf-2",
				Host: "https://127.0.0.1",
				Port: int32(port2),
				Type: pb.MatchmakingFunctionSpec_GRPC,
			},
		},
	}

	stream := &fakeInvokeMMFServerStream{ctx: context.Background()}
	err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
	require.NoError(t, err)

	stream.mu.Lock()
	var gotMatchIDs []string
	for _, m := range stream.matches {
		gotMatchIDs = append(gotMatchIDs, m.GetId())
	}
	stream.mu.Unlock()
	assert.ElementsMatch(t, []string{"match-from-mmf1", "match-from-mmf2"}, gotMatchIDs)

	mmfSrv1.mu.Lock()
	assert.Equal(t, [][]string{
		{"Bearer stale-valid-mmf1"},
		{"Bearer fresh-valid-mmf1"},
	}, mmfSrv1.authHeaders)
	mmfSrv1.mu.Unlock()

	mmfSrv2.mu.Lock()
	assert.Equal(t, [][]string{
		{"Bearer valid-mmf2"},
	}, mmfSrv2.authHeaders)
	mmfSrv2.mu.Unlock()

	assert.Equal(t, int32(2), mmf1SourceCalls.Load(), "mmf1 should have created initial source + 1 refreshed source")
	assert.Equal(t, int32(1), mmf2SourceCalls.Load(), "mmf2 should have created 1 source")
	assert.Empty(t, tc.SnapshotActiveTickets(), "both matched tickets should be deactivated after InvokeMatchmakingFunctions completes")
}

// testMidStreamErrorMMFServer is an in-process gRPC MatchMakingFunctionServiceServer
// that optionally runs beforeStream, streams a configured sequence of Matches,
// and then immediately returns terminalErr (or nil for a clean io.EOF stream
// completion). It is used to test InvokeMatchmakingFunctions when an MMF fails
// mid-stream while match ticket deactivation goroutines (tdwg) are still in flight.
type testMidStreamErrorMMFServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
	beforeStream func(context.Context)
	matches      []*pb.Match
	terminalErr  error
}

func (s *testMidStreamErrorMMFServer) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}
	}

	if s.beforeStream != nil {
		s.beforeStream(stream.Context())
	}

	for _, m := range s.matches {
		if err := stream.Send(&pb.StreamedMmfResponse{Match: m}); err != nil {
			return err
		}
	}
	return s.terminalErr
}

// findInt64SumPointByMMF searches rm for an Int64Counter / Sum[int64] metric named
// metricName and returns the value recorded with attributes mmf.name == mmfName
// and profile.name == profileName, along with whether a matching data point was found.
func findInt64SumPointByMMF(rm metricdata.ResourceMetrics, metricName, mmfName, profileName string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				gotMMF, hasMMF := dp.Attributes.Value(attribute.Key("mmf.name"))
				gotProfile, hasProfile := dp.Attributes.Value(attribute.Key("profile.name"))
				if hasMMF && hasProfile && gotMMF.AsString() == mmfName && gotProfile.AsString() == profileName {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

// TestInvokeMMFsMidStreamErrorWaitsForDeactivationAndRecordsMetrics verifies that
// when an MMF streams one or more valid matches and then fails mid-stream with a
// non-io.EOF gRPC error while ticket deactivation goroutines (tdwg) are in flight
// (tested across both OM_MATCH_TICKET_DEACTIVATION_WAIT=true and false):
//  1. InvokeMatchmakingFunctions waits for all in-flight tdwg goroutines before
//     closing matchChan (preventing a "send on closed channel" panic).
//  2. All matches streamed before the mid-stream error (as well as matches from
//     any sibling MMFs) are delivered to the caller stream.
//  3. All tickets contained in those pre-error matches are deactivated in the
//     ReplicatedTicketCache.
//  4. OpenTelemetry metrics otelMatches ("om_core.match.received"),
//     otelMmfFailures ("om_core.mmf.failures"), and otelMmfTicketDeactivations
//     ("om_core.mmf.deactivations") are accurately recorded via defer.
func TestInvokeMMFsMidStreamErrorWaitsForDeactivationAndRecordsMetrics(t *testing.T) {
	ensureGlobalTestTicketCache()

	for _, deactivationWait := range []bool{true, false} {
		deactivationWait := deactivationWait
		t.Run(fmt.Sprintf("DeactivationWait=%v", deactivationWait), func(t *testing.T) {
			// Swap the package-level cfg pointer to a fresh Viper instance rather
			// than calling cfg.Set on the shared TestMain Viper instance that
			// tcs["memory"]'s background replication goroutines read concurrently.
			prevCfg := cfg
			subCfg := config.Read()
			subCfg.Set("OM_MATCH_TICKET_DEACTIVATION_WAIT", deactivationWait)
			cfg = subCfg
			defer func() {
				cfg = prevCfg
			}()

			metricReader := sdkmetric.NewManualReader()
			testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
			defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
			testMeter := testMeterProvider.Meter("open_match.core.midstream_test")

			prevMeter := meter
			registerMetrics(&testMeter)
			defer func() {
				if prevMeter != nil {
					registerMetrics(prevMeter)
				}
			}()

			lisCrash, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer lisCrash.Close()
			_, crashPortStr, err := net.SplitHostPort(lisCrash.Addr().String())
			require.NoError(t, err)
			crashPort, err := strconv.Atoi(crashPortStr)
			require.NoError(t, err)

			lisHealthy, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer lisHealthy.Close()
			_, healthyPortStr, err := net.SplitHostPort(lisHealthy.Addr().String())
			require.NoError(t, err)
			healthyPort, err := strconv.Atoi(healthyPortStr)
			require.NoError(t, err)

			tcCtx := context.Background()
			createResp1, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
			require.NoError(t, err)
			createResp2, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
			require.NoError(t, err)
			createResp3, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
			require.NoError(t, err)
			ticketID1 := createResp1.GetTicketId()
			ticketID2 := createResp2.GetTicketId()
			ticketID3 := createResp3.GetTicketId()
			allTicketIDs := []string{ticketID1, ticketID2, ticketID3}
			t.Cleanup(func() {
				_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
					TicketIds: allTicketIDs,
				})
				require.Eventually(t, func() bool {
					return len(tc.SnapshotActiveTickets()) == 0
				}, 2*time.Second, 5*time.Millisecond)
			})

			_, err = activateTickets(tcCtx, testLogger, &tc, &pb.ActivateTicketsRequest{
				TicketIds: allTicketIDs,
			})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				return len(tc.SnapshotActiveTickets()) == 3
			}, 2*time.Second, 5*time.Millisecond)

			// crashingMMF waits for healthy-mmf's ticket (ticketID3) to be deactivated so
			// healthy-mmf has already completed and cannot mask a missing tdwg.Wait() on
			// crashing-mmf, then streams two valid matches (spawning two concurrent tdwg
			// ticket deactivation goroutines that must wait for replication before
			// completing) and immediately aborts the stream with codes.Internal.
			crashingMMFSrv := &testMidStreamErrorMMFServer{
				beforeStream: func(streamCtx context.Context) {
					deadline := time.NewTimer(time.Second)
					defer deadline.Stop()
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						if _, deactivated := tc.InactiveSet.Load(ticketID3); deactivated {
							// Give healthy-mmf's goroutine a brief moment after replication
							// wakeup to finish sending on matchChan and call mmfwg.Done().
							time.Sleep(2 * time.Millisecond)
							return
						}
						select {
						case <-streamCtx.Done():
							return
						case <-deadline.C:
							return
						case <-ticker.C:
						}
					}
				},
				matches: []*pb.Match{
					{
						Id: "match-before-crash-1",
						Rosters: map[string]*pb.Roster{
							"roster": {
								Name:    "roster",
								Tickets: []*pb.Ticket{{Id: ticketID1}},
							},
						},
					},
					{
						Id: "match-before-crash-2",
						Rosters: map[string]*pb.Roster{
							"roster": {
								Name:    "roster",
								Tickets: []*pb.Ticket{{Id: ticketID2}},
							},
						},
					},
				},
				terminalErr: status.Error(codes.Internal, "simulated mid-stream MMF crash"),
			}
			grpcCrashSrv := grpc.NewServer()
			pb.RegisterMatchMakingFunctionServiceServer(grpcCrashSrv, crashingMMFSrv)
			go grpcCrashSrv.Serve(lisCrash) //nolint:errcheck
			defer grpcCrashSrv.Stop()

			// healthyMMF runs concurrently, streams one match, and closes cleanly with io.EOF.
			healthyMMFSrv := &testMidStreamErrorMMFServer{
				matches: []*pb.Match{
					{
						Id: "match-from-healthy-mmf",
						Rosters: map[string]*pb.Roster{
							"roster": {
								Name:    "roster",
								Tickets: []*pb.Ticket{{Id: ticketID3}},
							},
						},
					},
				},
				terminalErr: nil,
			}
			grpcHealthySrv := grpc.NewServer()
			pb.RegisterMatchMakingFunctionServiceServer(grpcHealthySrv, healthyMMFSrv)
			go grpcHealthySrv.Serve(lisHealthy) //nolint:errcheck
			defer grpcHealthySrv.Stop()

			req := &pb.MmfRequest{
				Profile: &pb.Profile{
					Name: "com.example.midstream.test_profile",
					Pools: map[string]*pb.Pool{
						"all": {
							Name: "all",
							CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
								Start: timestamppb.New(time.Now().Add(-time.Hour)),
								End:   timestamppb.New(time.Now().Add(time.Hour)),
							},
						},
					},
				},
				Mmfs: []*pb.MatchmakingFunctionSpec{
					{
						Name: "crashing-mmf",
						Host: "http://127.0.0.1",
						Port: int32(crashPort),
						Type: pb.MatchmakingFunctionSpec_GRPC,
					},
					{
						Name: "healthy-mmf",
						Host: "http://127.0.0.1",
						Port: int32(healthyPort),
						Type: pb.MatchmakingFunctionSpec_GRPC,
					},
				},
			}

			stream := &fakeInvokeMMFServerStream{ctx: context.Background()}
			err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
			require.NoError(t, err)

			// 1 & 2. All matches streamed before the mid-stream error (plus the sibling
			// healthy MMF's match) must be delivered to the caller stream without racing
			// against close(matchChan).
			stream.mu.Lock()
			var gotMatchIDs []string
			for _, m := range stream.matches {
				gotMatchIDs = append(gotMatchIDs, m.GetId())
			}
			stream.mu.Unlock()
			assert.ElementsMatch(t, []string{
				"match-before-crash-1",
				"match-before-crash-2",
				"match-from-healthy-mmf",
			}, gotMatchIDs)

			// 3. All three matched tickets must be deactivated in the ReplicatedTicketCache.
			assert.Empty(t, tc.SnapshotActiveTickets(), "all tickets from pre-error and healthy matches must be deactivated")

			// 4. Verify OpenTelemetry metrics recorded for both the mid-stream failing MMF
			// and the healthy MMF. Note that InvokeMatchmakingFunctions strips the segment
			// after the final '.' in reverse-DNS profile names ("com.example.midstream.test_profile" -> "com.example.midstream").
			const expectedProfileAttr = "com.example.midstream"
			var rm metricdata.ResourceMetrics
			require.NoError(t, metricReader.Collect(context.Background(), &rm))

			crashMatches, found := findInt64SumPointByMMF(rm, "om_core.match.received", "crashing-mmf", expectedProfileAttr)
			require.True(t, found, "expected om_core.match.received data point for crashing-mmf")
			assert.Equal(t, int64(2), crashMatches, "otelMatches should record the 2 matches received before the mid-stream error")

			healthyMatches, found := findInt64SumPointByMMF(rm, "om_core.match.received", "healthy-mmf", expectedProfileAttr)
			require.True(t, found, "expected om_core.match.received data point for healthy-mmf")
			assert.Equal(t, int64(1), healthyMatches, "otelMatches should record the 1 match received from healthy-mmf")

			crashFailures, found := findInt64SumPointByMMF(rm, "om_core.mmf.failures", "crashing-mmf", expectedProfileAttr)
			require.True(t, found, "expected om_core.mmf.failures data point for crashing-mmf")
			assert.Equal(t, int64(1), crashFailures, "otelMmfFailures should record 1 failure for crashing-mmf")

			_, foundHealthyFailure := findInt64SumPointByMMF(rm, "om_core.mmf.failures", "healthy-mmf", expectedProfileAttr)
			assert.False(t, foundHealthyFailure, "healthy-mmf should not record any om_core.mmf.failures data point")

			crashDeactivations, found := findInt64SumPointByMMF(rm, "om_core.mmf.deactivations", "crashing-mmf", expectedProfileAttr)
			require.True(t, found, "expected om_core.mmf.deactivations data point for crashing-mmf")
			assert.Equal(t, int64(2), crashDeactivations, "otelMmfTicketDeactivations should record 2 deactivated tickets for crashing-mmf")

			healthyDeactivations, found := findInt64SumPointByMMF(rm, "om_core.mmf.deactivations", "healthy-mmf", expectedProfileAttr)
			require.True(t, found, "expected om_core.mmf.deactivations data point for healthy-mmf")
			assert.Equal(t, int64(1), healthyDeactivations, "otelMmfTicketDeactivations should record 1 deactivated ticket for healthy-mmf")
		})
	}
}

// testChunkInspectingMMFServer is an in-process gRPC MatchMakingFunctionServiceServer
// that reads ChunkedMmfRunRequest messages from stream.Recv() in a loop until
// io.EOF (verifying that InvokeMatchmakingFunctions calls CloseSend()), optionally
// aborts early after abortAfterChunks chunks with abortErr to simulate a stream
// failure during client chunk upload, and otherwise streams back matches.
type testChunkInspectingMMFServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
	mu               sync.Mutex
	receivedChunks   []*pb.ChunkedMmfRunRequest
	sawEOF           bool
	abortAfterChunks int
	abortErr         error
	matches          []*pb.Match
}

func (s *testChunkInspectingMMFServer) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.mu.Lock()
			s.sawEOF = true
			s.mu.Unlock()
			break
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.receivedChunks = append(s.receivedChunks, chunk)
		count := len(s.receivedChunks)
		s.mu.Unlock()

		if s.abortErr != nil && s.abortAfterChunks > 0 && count >= s.abortAfterChunks {
			return s.abortErr
		}
	}

	if s.abortErr != nil && s.abortAfterChunks == 0 {
		return s.abortErr
	}

	for _, m := range s.matches {
		if err := stream.Send(&pb.StreamedMmfResponse{Match: m}); err != nil {
			return err
		}
	}
	return nil
}

// TestInvokeMMFsCloseSendEmptyPoolsAndSendError verifies Task M-02 end-to-end:
//  1. openAndSendChunks calls streamClient.CloseSend() after sending all
//     ChunkedMmfRunRequest chunks so an MMF reading stream.Recv() in a loop
//     until io.EOF (per proto/messages.proto) receives all chunks and io.EOF
//     without hanging.
//  2. Pools that match 0 active tickets (empty pools) and pools that only match
//     tickets in a subset of chunks are preserved in every streamed
//     ChunkedMmfRunRequest with their original filters and a non-nil
//     "<pool>_roster" Participants message.
//  3. When an MMF aborts the stream while multi-chunk requests are being sent
//     (causing streamClient.Send to fail / return io.EOF), openAndSendChunks
//     unwraps and returns the error and records otelMmfFailures ("om_core.mmf.failures").
func TestInvokeMMFsCloseSendEmptyPoolsAndSendError(t *testing.T) {
	ensureGlobalTestTicketCache()

	// Set a 1-second MMF timeout on a fresh Viper instance so that if
	// streamClient.CloseSend() were ever omitted, eof-reading-mmf would fail fast
	// within 1s instead of hanging for the 600s default timeout.
	prevCfg := cfg
	subCfg := config.Read()
	subCfg.Set("OM_MMF_TIMEOUT_SECS", 1)
	cfg = subCfg
	defer func() {
		cfg = prevCfg
	}()

	metricReader := sdkmetric.NewManualReader()
	testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
	testMeter := testMeterProvider.Meter("open_match.core.m02_test")

	prevMeter := meter
	registerMetrics(&testMeter)
	defer func() {
		if prevMeter != nil {
			registerMetrics(prevMeter)
		}
	}()

	lisEOF, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lisEOF.Close()
	_, eofPortStr, err := net.SplitHostPort(lisEOF.Addr().String())
	require.NoError(t, err)
	eofPort, err := strconv.Atoi(eofPortStr)
	require.NoError(t, err)

	lisAbort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lisAbort.Close()
	_, abortPortStr, err := net.SplitHostPort(lisAbort.Addr().String())
	require.NoError(t, err)
	abortPort, err := strconv.Atoi(abortPortStr)
	require.NoError(t, err)

	// Create 3 active tickets with ~2.1MB extensions so they exceed maxPbSize (4MB)
	// and force InvokeMatchmakingFunctions to split the request across multiple
	// ChunkedMmfRunRequest messages.
	largePayload, err := anypb.New(wrapperspb.Bytes(make([]byte, 2_100_000)))
	require.NoError(t, err)

	tcCtx := context.Background()
	createResp1, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				Tags: []string{"mode-a", "subset-b"},
			},
			Extensions: map[string]*anypb.Any{"payload": largePayload},
		},
	})
	require.NoError(t, err)
	createResp2, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				Tags: []string{"mode-a"},
			},
			Extensions: map[string]*anypb.Any{"payload": largePayload},
		},
	})
	require.NoError(t, err)
	createResp3, err := createTicket(tcCtx, &tc, &pb.CreateTicketRequest{
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				Tags: []string{"mode-a"},
			},
			Extensions: map[string]*anypb.Any{"payload": largePayload},
		},
	})
	require.NoError(t, err)

	ticketID1 := createResp1.GetTicketId()
	ticketID2 := createResp2.GetTicketId()
	ticketID3 := createResp3.GetTicketId()
	allTicketIDs := []string{ticketID1, ticketID2, ticketID3}
	t.Cleanup(func() {
		_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
			TicketIds: allTicketIDs,
		})
		require.Eventually(t, func() bool {
			return len(tc.SnapshotActiveTickets()) == 0
		}, 2*time.Second, 5*time.Millisecond)
	})

	_, err = activateTickets(tcCtx, testLogger, &tc, &pb.ActivateTicketsRequest{
		TicketIds: allTicketIDs,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(tc.SnapshotActiveTickets()) == 3
	}, 2*time.Second, 5*time.Millisecond)

	// eofMMFSrv strictly loops stream.Recv() until io.EOF and returns 1 match
	// deactivating all three tickets.
	eofMMFSrv := &testChunkInspectingMMFServer{
		matches: []*pb.Match{
			{
				Id: "match-from-eof-mmf",
				Rosters: map[string]*pb.Roster{
					"roster": {
						Name: "roster",
						Tickets: []*pb.Ticket{
							{Id: ticketID1},
							{Id: ticketID2},
							{Id: ticketID3},
						},
					},
				},
			},
		},
	}
	grpcEOFSrv := grpc.NewServer()
	pb.RegisterMatchMakingFunctionServiceServer(grpcEOFSrv, eofMMFSrv)
	go grpcEOFSrv.Serve(lisEOF) //nolint:errcheck
	defer grpcEOFSrv.Stop()

	// abortSendMMFSrv is configured with MaxRecvMsgSize(1024) so the server
	// rejects the first 2.1MB chunk with ResourceExhausted while openAndSendChunks
	// is sending chunks, exercising the Send/CloseSend error path in openAndSendChunks.
	abortSendMMFSrv := &testChunkInspectingMMFServer{
		abortAfterChunks: 1,
		abortErr:         status.Error(codes.ResourceExhausted, "chunk rejected during Send"),
	}
	grpcAbortSrv := grpc.NewServer(grpc.MaxRecvMsgSize(1024))
	pb.RegisterMatchMakingFunctionServiceServer(grpcAbortSrv, abortSendMMFSrv)
	go grpcAbortSrv.Serve(lisAbort) //nolint:errcheck
	defer grpcAbortSrv.Stop()

	req := &pb.MmfRequest{
		Profile: &pb.Profile{
			Name: "com.example.m02.test_profile",
			Pools: map[string]*pb.Pool{
				"matched_all_pool": {
					Name: "matched_all_pool",
					TagPresentFilters: []*pb.Pool_TagPresentFilter{
						{Tag: "mode-a"},
					},
					CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
						Start: timestamppb.New(time.Now().Add(-time.Hour)),
						End:   nil,
					},
				},
				"subset_pool": {
					Name: "subset_pool",
					TagPresentFilters: []*pb.Pool_TagPresentFilter{
						{Tag: "subset-b"},
					},
				},
				"empty_pool": {
					Name: "empty_pool",
					TagPresentFilters: []*pb.Pool_TagPresentFilter{
						{Tag: "tag-matching-zero-tickets"},
					},
				},
				"invalid_pool": {
					Name: "invalid_pool",
					CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
						Start: &timestamppb.Timestamp{Nanos: -1},
					},
				},
			},
		},
		Mmfs: []*pb.MatchmakingFunctionSpec{
			{
				Name: "eof-reading-mmf",
				Host: "http://127.0.0.1",
				Port: int32(eofPort),
				Type: pb.MatchmakingFunctionSpec_GRPC,
			},
			{
				Name: "send-abort-mmf",
				Host: "http://127.0.0.1",
				Port: int32(abortPort),
				Type: pb.MatchmakingFunctionSpec_GRPC,
			},
		},
	}

	var logBuf syncBuffer
	prevOut := logger.Logger.Out
	prevLevel := logger.Logger.GetLevel()
	logger.Logger.SetOutput(&logBuf)
	logger.Logger.SetLevel(logrus.ErrorLevel)
	defer func() {
		logger.Logger.SetLevel(prevLevel)
		logger.Logger.SetOutput(prevOut)
	}()

	stream := &fakeInvokeMMFServerStream{ctx: context.Background()}
	err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
	require.NoError(t, err)

	// Verify invalid_pool triggered the formatted Errorf log in getFilteredTickets
	// (main.go:794) without raw "%w" or "%!w" formatting artifacts.
	loggedOutput := logBuf.String()
	assert.Contains(t, loggedOutput, "Unable to fill pool with tickets, invalid:")
	assert.Contains(t, loggedOutput, ".invalid creation time filter start value")
	assert.NotContains(t, loggedOutput, "%w")
	assert.NotContains(t, loggedOutput, "%!w")

	// 1. Verify eof-reading-mmf received multiple chunks and terminated its Recv()
	// loop cleanly on io.EOF (because openAndSendChunks called CloseSend()).
	eofMMFSrv.mu.Lock()
	sawEOF := eofMMFSrv.sawEOF
	chunks := append([]*pb.ChunkedMmfRunRequest(nil), eofMMFSrv.receivedChunks...)
	eofMMFSrv.mu.Unlock()

	assert.True(t, sawEOF, "MMF server reading stream.Recv() until io.EOF must observe io.EOF after CloseSend()")
	require.GreaterOrEqual(t, len(chunks), 3, "3x 2.1MB tickets must be split across at least 3 chunks")

	// 2. Verify every chunk contains all 3 pools ("matched_all_pool", "subset_pool",
	// and "empty_pool") with preserved filters and non-nil "<pool>_roster" Participants,
	// and that reassembling rosters across chunks yields the exact expected tickets.
	var reassembledAllPool []string
	var reassembledSubsetPool []string
	for idx, chunk := range chunks {
		assert.Equal(t, int32(len(chunks)), chunk.GetNumChunks())
		pools := chunk.GetProfile().GetPools()
		require.Len(t, pools, 3, "chunk %d must preserve all 3 pools including empty pools", idx)

		for _, poolName := range []string{"matched_all_pool", "subset_pool", "empty_pool"} {
			p := pools[poolName]
			require.NotNil(t, p, "chunk %d missing pool %s", idx, poolName)
			require.Len(t, p.GetTagPresentFilters(), 1, "chunk %d pool %s must preserve TagPresentFilters", idx, poolName)
			require.NotNil(t, p.GetParticipants(), "chunk %d pool %s must have non-nil Participants", idx, poolName)
			assert.Equal(t, poolName+"_roster", p.GetParticipants().GetName())
		}

		assert.Empty(t, pools["empty_pool"].GetParticipants().GetTickets(), "empty_pool must have 0 tickets in chunk %d", idx)

		for _, tk := range pools["matched_all_pool"].GetParticipants().GetTickets() {
			reassembledAllPool = append(reassembledAllPool, tk.GetId())
		}
		for _, tk := range pools["subset_pool"].GetParticipants().GetTickets() {
			reassembledSubsetPool = append(reassembledSubsetPool, tk.GetId())
		}
	}
	assert.ElementsMatch(t, allTicketIDs, reassembledAllPool)
	assert.Equal(t, []string{ticketID1}, reassembledSubsetPool)

	// 3. Verify match from eof-reading-mmf was delivered and all 3 tickets deactivated.
	stream.mu.Lock()
	require.Len(t, stream.matches, 1)
	assert.Equal(t, "match-from-eof-mmf", stream.matches[0].GetId())
	stream.mu.Unlock()
	assert.Empty(t, tc.SnapshotActiveTickets())

	// 4. Verify OpenTelemetry metrics: send-abort-mmf recorded 1 failure in
	// om_core.mmf.failures, while eof-reading-mmf recorded 1 match and 0 failures.
	const expectedProfileAttr = "com.example.m02"
	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))

	abortFailures, found := findInt64SumPointByMMF(rm, "om_core.mmf.failures", "send-abort-mmf", expectedProfileAttr)
	require.True(t, found, "expected om_core.mmf.failures data point for send-abort-mmf")
	assert.Equal(t, int64(1), abortFailures)

	_, foundAbortMatches := findInt64SumPointByMMF(rm, "om_core.match.received", "send-abort-mmf", expectedProfileAttr)
	assert.False(t, foundAbortMatches, "send-abort-mmf must abort in openAndSendChunks before entering the match receive loop")

	_, foundEOFFailure := findInt64SumPointByMMF(rm, "om_core.mmf.failures", "eof-reading-mmf", expectedProfileAttr)
	assert.False(t, foundEOFFailure, "eof-reading-mmf should not record any om_core.mmf.failures")

	eofMatches, found := findInt64SumPointByMMF(rm, "om_core.match.received", "eof-reading-mmf", expectedProfileAttr)
	require.True(t, found, "expected om_core.match.received data point for eof-reading-mmf")
	assert.Equal(t, int64(1), eofMatches)
}

// syncBuffer is a concurrency-safe io.Writer for capturing logrus output
// across concurrent MMF goroutines under -race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
