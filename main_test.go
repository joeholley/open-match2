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
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
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

			for tcTypeName, tcInstance := range tcs {
				_, createErr := createTicket(context.Background(), tcInstance, &pb.CreateTicketRequest{Ticket: makeTicket()})
				createStatus := status.Convert(createErr)
				assert.Equal(t, invalidExpTimeStatus.Code(), createStatus.Code(), "%v createTicket must preserve InvalidArgument status code", tcTypeName)
				assert.Contains(t, createStatus.Message(), invalidExpTimeStatus.Message())
			}
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
					} else {
						assert.Equal(t, result.GetAttributes().GetCreationTime().AsTime().Add(maxTTL), result.GetExpirationTime().AsTime())
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

// errorInjectingReplicator wraps a store.StateReplicator and invokes beforeSend
// (before delegating to inner.SendUpdates) and onSend (after delegating to
// inner.SendUpdates) to synchronize or inject custom per-update errors in tests.
type errorInjectingReplicator struct {
	mu         sync.RWMutex
	inner      store.StateReplicator
	beforeSend func(updates []*store.StateUpdate)
	onSend     func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse
}

func (r *errorInjectingReplicator) setBeforeSend(fn func(updates []*store.StateUpdate)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.beforeSend = fn
}

func (r *errorInjectingReplicator) setOnSend(fn func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onSend = fn
}

func (r *errorInjectingReplicator) GetUpdates() []*store.StateUpdate {
	return r.inner.GetUpdates()
}

func (r *errorInjectingReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	r.mu.RLock()
	beforeSend := r.beforeSend
	r.mu.RUnlock()
	if beforeSend != nil {
		beforeSend(updates)
	}
	res := r.inner.SendUpdates(updates)
	r.mu.RLock()
	onSend := r.onSend
	r.mu.RUnlock()
	if onSend != nil {
		return onSend(updates, res)
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

// fakeInvokeMMFServerStream is an in-process pb.OpenMatchService_InvokeMatchmakingFunctionsServer
// test double that records matches streamed back to the caller.
type fakeInvokeMMFServerStream struct {
	grpc.ServerStream
	ctx     context.Context
	mu      sync.Mutex
	matches []*pb.Match
	onSend  func(*pb.StreamedMmfResponse)
}

func (f *fakeInvokeMMFServerStream) Context() context.Context {
	return f.ctx
}

func (f *fakeInvokeMMFServerStream) Send(resp *pb.StreamedMmfResponse) error {
	f.mu.Lock()
	if resp.GetMatch() != nil {
		f.matches = append(f.matches, resp.GetMatch())
	}
	onSend := f.onSend
	f.mu.Unlock()
	if onSend != nil {
		onSend(resp)
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

var (
	globalTestTCOnce     sync.Once
	globalTestReplicator *errorInjectingReplicator
)

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

		globalTestReplicator = &errorInjectingReplicator{
			inner: memoryReplicator.New(localCfg),
		}
		tc = cache.ReplicatedTicketCache{}
		tc.Init(localCfg, globalTestReplicator)
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

// fakeWatchAssignmentsServerStream captures StreamedWatchAssignmentsResponse
// messages emitted by WatchAssignments for unit test assertions.
type fakeWatchAssignmentsServerStream struct {
	grpc.ServerStream
	ctx       context.Context
	mu        sync.Mutex
	responses []*pb.StreamedWatchAssignmentsResponse
	onSend    func(*pb.StreamedWatchAssignmentsResponse)
}

func (f *fakeWatchAssignmentsServerStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeWatchAssignmentsServerStream) Send(resp *pb.StreamedWatchAssignmentsResponse) error {
	f.mu.Lock()
	f.responses = append(f.responses, resp)
	onSend := f.onSend
	f.mu.Unlock()
	if onSend != nil {
		onSend(resp)
	}
	return nil
}

// findInt64SumTotal returns the cumulative sum across all data points of an
// Int64Counter or Int64UpDownCounter metric with the given name.
func findInt64SumTotal(rm metricdata.ResourceMetrics, metricName string) (int64, bool) {
	var total int64
	var found bool
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
				total += dp.Value
				found = true
			}
		}
	}
	return total, found
}

// findInt64HistogramCountAndSum returns the total sample count and sum across
// all data points of an Int64Histogram metric with the given name.
func findInt64HistogramCountAndSum(rm metricdata.ResourceMetrics, metricName string) (uint64, int64, bool) {
	var count uint64
	var sum int64
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				count += dp.Count
				sum += dp.Sum
				found = true
			}
		}
	}
	return count, sum, found
}

// TestCreateAssignmentsValidationReplicationAndMetrics verifies that CreateAssignments:
//  1. Rejects nil rosters, nil assignments, empty assignments, and rosters exceeding
//     OM_MAX_STATE_UPDATES_PER_CALL with codes.InvalidArgument (wrapping TooManyUpdatesErr).
//  2. Replicates valid assignments to tc.Assignments and records otelAssignmentsPerCall
//     ("om_core.ticket.assignment").
//  3. Returns codes.Internal ("Empty ticket ID in assignment request!") when any ticket in
//     the roster has an empty ID instead of swallowing the error, while still replicating
//     valid tickets in the same roster.
//  4. Returns codes.Internal ("Unable to assign ticket: ...") when state replication fails.
func TestCreateAssignmentsValidationReplicationAndMetrics(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("InputValidation", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_MAX_STATE_UPDATES_PER_CALL", 2)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		srv := &grpcServer{}
		ctx := context.Background()

		// 1. Nil AssignmentRoster
		_, err := srv.CreateAssignments(ctx, &pb.CreateAssignmentsRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "roster with assignment is required")

		// 2. Nil Assignment inside AssignmentRoster
		_, err = srv.CreateAssignments(ctx, &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:    "r1",
				Tickets: []*pb.Ticket{{Id: "1700000000001-0"}},
			},
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "roster with assignment is required")

		// 3. Empty Assignment (proto.Marshal produces 0 bytes)
		_, err = srv.CreateAssignments(ctx, &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:       "r1",
				Assignment: &pb.Assignment{},
				Tickets:    []*pb.Ticket{{Id: "1700000000001-0"}},
			},
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "roster with non-empty assignment is required")

		// 4. Roster ticket count exceeds OM_MAX_STATE_UPDATES_PER_CALL (3 > 2)
		_, err = srv.CreateAssignments(ctx, &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:       "r1",
				Assignment: &pb.Assignment{Connection: "10.0.0.1:7777"},
				Tickets: []*pb.Ticket{
					{Id: "1700000000001-0"},
					{Id: "1700000000002-0"},
					{Id: "1700000000003-0"},
				},
			},
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), TooManyUpdatesErr.Error())
		assert.Contains(t, err.Error(), "configured maximum 2, requested 3")
	})

	t.Run("ValidRosterReplicatesAssignmentsAndRecordsMetric", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.create_assignments_valid")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		ticketID1 := fmt.Sprintf("%013d-501", time.Now().UnixMilli())
		ticketID2 := fmt.Sprintf("%013d-502", time.Now().UnixMilli())
		t.Cleanup(func() {
			tc.Assignments.Delete(ticketID1)
			tc.Assignments.Delete(ticketID2)
		})

		_, err := (&grpcServer{}).CreateAssignments(context.Background(), &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:       "valid_roster",
				Assignment: &pb.Assignment{Connection: "10.0.0.10:7777"},
				Tickets: []*pb.Ticket{
					{Id: ticketID1},
					{Id: ticketID2},
				},
			},
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			v1, ok1 := tc.Assignments.Load(ticketID1)
			v2, ok2 := tc.Assignments.Load(ticketID2)
			return ok1 && ok2 &&
				v1.(*pb.Assignment).GetConnection() == "10.0.0.10:7777" &&
				v2.(*pb.Assignment).GetConnection() == "10.0.0.10:7777"
		}, 2*time.Second, 5*time.Millisecond)

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		count, sum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.assignment")
		require.True(t, found, "expected om_core.ticket.assignment histogram data point")
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, int64(2), sum)
	})

	t.Run("EmptyTicketIDInRosterProcessesValidTicketsAndReturnsInternalError", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.create_assignments_empty_id")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		validID := fmt.Sprintf("%013d-503", time.Now().UnixMilli())
		t.Cleanup(func() {
			tc.Assignments.Delete(validID)
		})

		_, err := (&grpcServer{}).CreateAssignments(context.Background(), &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:       "mixed_roster",
				Assignment: &pb.Assignment{Connection: "10.0.0.11:7777"},
				Tickets: []*pb.Ticket{
					{Id: validID},
					{Id: ""},
				},
			},
		})
		require.Error(t, err, "CreateAssignments must return the empty ticket ID error instead of nil")
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "Empty ticket ID in assignment request!")

		// Valid ticket in the same roster must still be replicated.
		require.Eventually(t, func() bool {
			v, ok := tc.Assignments.Load(validID)
			return ok && v.(*pb.Assignment).GetConnection() == "10.0.0.11:7777"
		}, 2*time.Second, 5*time.Millisecond)

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		count, sum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.assignment")
		require.True(t, found)
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, int64(1), sum)
	})

	t.Run("ReplicationFailureReturnsUnableToAssignTicketInternalError", func(t *testing.T) {
		failingID := fmt.Sprintf("%013d-504", time.Now().UnixMilli())
		globalTestReplicator.setOnSend(func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
			for i, u := range updates {
				if u.Cmd == store.Assign && u.Key == failingID {
					results[i] = &store.StateResponse{
						Result: failingID,
						Err:    errors.New("simulated redis assign error"),
					}
				}
			}
			return results
		})
		defer globalTestReplicator.setOnSend(nil)
		t.Cleanup(func() {
			tc.Assignments.Delete(failingID)
		})

		_, err := (&grpcServer{}).CreateAssignments(context.Background(), &pb.CreateAssignmentsRequest{
			AssignmentRoster: &pb.Roster{
				Name:       "failing_roster",
				Assignment: &pb.Assignment{Connection: "10.0.0.12:7777"},
				Tickets: []*pb.Ticket{
					{Id: failingID},
				},
			},
		})
		require.Error(t, err, "CreateAssignments must return replication error instead of swallowing it")
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "Unable to assign ticket: simulated redis assign error")
		assert.NotContains(t, err.Error(), "Unable to delete ticket")
	})
}

// TestWatchAssignmentsDeliveryTimeoutCancellationAndMetrics verifies that WatchAssignments:
//  1. Streams assignments for all watched tickets when present and leaves
//     otelAssignmentWatches ("om_core.ticket.assignment.watch") at net 0.
//  2. On partial delivery followed by TTL timeout (<-timeout), decrements the
//     remaining (len(req.GetTicketIds()) - i) watches so otelAssignmentWatches
//     returns to 0 (instead of adding +len-i), cancels and waits for all polling
//     goroutines before closing updateChan, and does not panic if late assignments
//     arrive after WatchAssignments returns.
//  3. When the caller cancels stream.Context() (<-stream.Context().Done()),
//     unblocks immediately without waiting for the 1s poll interval or TTL timeout,
//     returns stream.Context().Err(), decrements remaining watches to 0, and cleanly
//     stops all polling goroutines before closing updateChan.
func TestWatchAssignmentsDeliveryTimeoutCancellationAndMetrics(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("AllTicketsAssignedDeliversAllAndBalancesGauge", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.watch_assignments_all")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		nowMs := time.Now().UnixMilli()
		id1 := fmt.Sprintf("%013d-601", nowMs)
		id2 := fmt.Sprintf("%013d-602", nowMs)
		tc.Assignments.Store(id1, &pb.Assignment{Connection: "10.0.0.21:7777"})
		tc.Assignments.Store(id2, &pb.Assignment{Connection: "10.0.0.22:7777"})
		t.Cleanup(func() {
			tc.Assignments.Delete(id1)
			tc.Assignments.Delete(id2)
		})

		stream := &fakeWatchAssignmentsServerStream{ctx: context.Background()}
		err := (&grpcServer{}).WatchAssignments(&pb.WatchAssignmentsRequest{
			TicketIds: []string{id1, id2},
		}, stream)
		require.NoError(t, err)

		stream.mu.Lock()
		got := make(map[string]string, len(stream.responses))
		for _, r := range stream.responses {
			got[r.GetId()] = r.GetAssignment().GetConnection()
		}
		stream.mu.Unlock()
		assert.Equal(t, map[string]string{
			id1: "10.0.0.21:7777",
			id2: "10.0.0.22:7777",
		}, got)

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		activeWatches, found := findInt64SumTotal(rm, "om_core.ticket.assignment.watch")
		require.True(t, found, "expected om_core.ticket.assignment.watch data point")
		assert.Equal(t, int64(0), activeWatches, "active watch gauge must return to 0 after all assignments are delivered")
	})

	t.Run("PartialAssignmentAndTimeoutDecrementsRemainingWatchesWithoutPanicOnLateAssignment", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_CACHE_TICKET_TTL_MS", 80)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.watch_assignments_timeout")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		nowMs := time.Now().UnixMilli()
		id1 := fmt.Sprintf("%013d-603", nowMs)
		id2 := fmt.Sprintf("%013d-604", nowMs)
		id3 := fmt.Sprintf("%013d-605", nowMs)
		// Only id1 is assigned before timeout; id2 and id3 remain unassigned until after WatchAssignments exits.
		tc.Assignments.Store(id1, &pb.Assignment{Connection: "10.0.0.23:7777"})
		t.Cleanup(func() {
			tc.Assignments.Delete(id1)
			tc.Assignments.Delete(id2)
			tc.Assignments.Delete(id3)
		})

		stream := &fakeWatchAssignmentsServerStream{ctx: context.Background()}
		baselineGoroutines := runtime.NumGoroutine()
		err := (&grpcServer{}).WatchAssignments(&pb.WatchAssignmentsRequest{
			TicketIds: []string{id1, id2, id3},
		}, stream)
		require.NoError(t, err)
		assert.LessOrEqual(t, runtime.NumGoroutine(), baselineGoroutines, "WatchAssignments must cancel and wait for all per-ticket polling goroutines before returning")

		stream.mu.Lock()
		require.Len(t, stream.responses, 1)
		assert.Equal(t, id1, stream.responses[0].GetId())
		assert.Equal(t, "10.0.0.23:7777", stream.responses[0].GetAssignment().GetConnection())
		stream.mu.Unlock()

		// Store late assignments for id2 and id3 after WatchAssignments has timed out
		// and closed updateChan; if polling goroutines were leaked, they would panic
		// with "send on closed channel" on their next wakeup.
		tc.Assignments.Store(id2, &pb.Assignment{Connection: "10.0.0.24:7777"})
		tc.Assignments.Store(id3, &pb.Assignment{Connection: "10.0.0.25:7777"})

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		activeWatches, found := findInt64SumTotal(rm, "om_core.ticket.assignment.watch")
		require.True(t, found, "expected om_core.ticket.assignment.watch data point")
		assert.Equal(t, int64(0), activeWatches, "timeout branch must subtract remaining (len-i) watches so gauge returns to 0")
	})

	t.Run("StreamContextCancellationUnblocksImmediatelyDecrementsGaugeAndStopsGoroutines", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		// Long TTL (10s) ensures only stream context cancellation can unblock WatchAssignments.
		subCfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.watch_assignments_cancel")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		nowMs := time.Now().UnixMilli()
		id1 := fmt.Sprintf("%013d-606", nowMs)
		id2 := fmt.Sprintf("%013d-607", nowMs)
		id3 := fmt.Sprintf("%013d-608", nowMs)
		// Pre-populate both id1 and id2 while leaving id3 unassigned:
		// - One pre-populated goroutine delivers the first response on updateChan.
		// - While WatchAssignments is inside stream.Send (onSend), the second
		//   pre-populated goroutine is blocked at select { case updateChan <- ...: case <-watchCtx.Done(): }
		//   and id3's goroutine is blocked at select { case <-watchCtx.Done(): case <-ticker.C: }.
		tc.Assignments.Store(id1, &pb.Assignment{Connection: "10.0.0.26:7777"})
		tc.Assignments.Store(id2, &pb.Assignment{Connection: "10.0.0.27:7777"})
		t.Cleanup(func() {
			tc.Assignments.Delete(id1)
			tc.Assignments.Delete(id2)
			tc.Assignments.Delete(id3)
		})

		streamCtx, cancelStream := context.WithCancel(context.Background())
		defer cancelStream()

		stream := &fakeWatchAssignmentsServerStream{
			ctx: streamCtx,
			onSend: func(_ *pb.StreamedWatchAssignmentsResponse) {
				// Allow the sibling pre-populated goroutine to park on updateChan <-
				// and the unassigned goroutine to park on <-ticker.C before cancelling.
				time.Sleep(5 * time.Millisecond)
				cancelStream()
				// Allow the sibling pre-populated goroutine to observe <-watchCtx.Done()
				// while WatchAssignments is still inside stream.Send so WatchAssignments
				// deterministically selects <-stream.Context().Done() on the next iteration.
				time.Sleep(5 * time.Millisecond)
			},
		}

		baselineGoroutines := runtime.NumGoroutine()
		start := time.Now()
		err := (&grpcServer{}).WatchAssignments(&pb.WatchAssignmentsRequest{
			TicketIds: []string{id1, id2, id3},
		}, stream)
		elapsed := time.Since(start)

		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, elapsed, 500*time.Millisecond, "WatchAssignments must unblock immediately on stream.Context().Done() without waiting for 1s poll sleep or 10s TTL")
		assert.LessOrEqual(t, runtime.NumGoroutine(), baselineGoroutines, "WatchAssignments must cancel and wait for all per-ticket polling goroutines before returning")

		stream.mu.Lock()
		require.Len(t, stream.responses, 1)
		assert.Contains(t, []string{id1, id2}, stream.responses[0].GetId())
		stream.mu.Unlock()

		// Store assignment for id3 after cancellation to confirm polling goroutines exited cleanly.
		tc.Assignments.Store(id3, &pb.Assignment{Connection: "10.0.0.28:7777"})

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		activeWatches, found := findInt64SumTotal(rm, "om_core.ticket.assignment.watch")
		require.True(t, found, "expected om_core.ticket.assignment.watch data point")
		assert.Equal(t, int64(0), activeWatches, "stream cancellation branch must subtract remaining (len-i) watches so gauge returns to 0")
	})

	t.Run("AsyncAssignmentViaCreateAssignmentsDeliversOnTickerPollAndBalancesGauge", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_CACHE_TICKET_TTL_MS", 5000)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.watch_assignments_ticker")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		nowMs := time.Now().UnixMilli()
		id1 := fmt.Sprintf("%013d-609", nowMs)
		id2 := fmt.Sprintf("%013d-610", nowMs)
		// Only id1 is pre-assigned before WatchAssignments starts; id2 starts unassigned
		// so its polling goroutine initializes the 1s time.Ticker and waits on <-ticker.C.
		tc.Assignments.Store(id1, &pb.Assignment{Connection: "10.0.0.29:7777"})
		t.Cleanup(func() {
			tc.Assignments.Delete(id1)
			tc.Assignments.Delete(id2)
		})

		srv := &grpcServer{}
		stream := &fakeWatchAssignmentsServerStream{
			ctx: context.Background(),
			onSend: func(resp *pb.StreamedWatchAssignmentsResponse) {
				if resp.GetId() == id1 {
					// Give id2's polling goroutine a brief moment to miss the initial
					// tc.Assignments.Load(id2) and enter <-ticker.C, then assign id2 via CreateAssignments.
					time.Sleep(15 * time.Millisecond)
					_, createErr := srv.CreateAssignments(context.Background(), &pb.CreateAssignmentsRequest{
						AssignmentRoster: &pb.Roster{
							Name:       "async_roster",
							Assignment: &pb.Assignment{Connection: "10.0.0.30:7777"},
							Tickets:    []*pb.Ticket{{Id: id2}},
						},
					})
					assert.NoError(t, createErr)
				}
			},
		}

		baselineGoroutines := runtime.NumGoroutine()
		err := srv.WatchAssignments(&pb.WatchAssignmentsRequest{
			TicketIds: []string{id1, id2},
		}, stream)
		require.NoError(t, err)
		assert.LessOrEqual(t, runtime.NumGoroutine(), baselineGoroutines, "WatchAssignments must stop ticker and exit all polling goroutines before returning")

		stream.mu.Lock()
		got := make(map[string]string, len(stream.responses))
		for _, r := range stream.responses {
			got[r.GetId()] = r.GetAssignment().GetConnection()
		}
		stream.mu.Unlock()
		assert.Equal(t, map[string]string{
			id1: "10.0.0.29:7777",
			id2: "10.0.0.30:7777",
		}, got)

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		activeWatches, found := findInt64SumTotal(rm, "om_core.ticket.assignment.watch")
		require.True(t, found, "expected om_core.ticket.assignment.watch data point")
		assert.Equal(t, int64(0), activeWatches, "active watch gauge must return to 0 after async ticker-polled delivery")
	})
}

// TestInvokeMMFsDeactivationWaitOrderingAndSemantics verifies the ordering and
// behavioral contract of OM_MATCH_TICKET_DEACTIVATION_WAIT in InvokeMatchmakingFunctions:
//  1. When OM_MATCH_TICKET_DEACTIVATION_WAIT=false, matches are queued onto
//     matchChan and streamed to the caller BEFORE updateTicketsActiveState and
//     waitForMatchDeactivation complete, while still completing ticket
//     deactivation asynchronously before InvokeMatchmakingFunctions returns.
//  2. When OM_MATCH_TICKET_DEACTIVATION_WAIT=true, updateTicketsActiveState and
//     waitForMatchDeactivation complete BEFORE the match is queued onto matchChan
//     and streamed to the caller.
//  3. When OM_MATCH_TICKET_DEACTIVATION_WAIT=false and the caller's stream.Context()
//     is already cancelled when the match arrives from the MMF, the match is
//     dropped before matchChan <- res and its tickets are not deactivated.
func TestInvokeMMFsDeactivationWaitOrderingAndSemantics(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("DeactivationWaitFalseStreamsMatchBeforeDeactivationWriteCompletes", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_MATCH_TICKET_DEACTIVATION_WAIT", false)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.deactivation_wait_false")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		createResp, err := createTicket(context.Background(), &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.NoError(t, err)
		ticketID := createResp.GetTicketId()
		t.Cleanup(func() {
			globalTestReplicator.setBeforeSend(nil)
			_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: []string{ticketID},
			})
			require.Eventually(t, func() bool {
				return len(tc.SnapshotActiveTickets()) == 0
			}, 2*time.Second, 5*time.Millisecond)
		})

		_, err = activateTickets(context.Background(), testLogger, &tc, &pb.ActivateTicketsRequest{
			TicketIds: []string{ticketID},
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return len(tc.SnapshotActiveTickets()) == 1
		}, 2*time.Second, 5*time.Millisecond)

		// Block the store.Deactivate replication write for ticketID until the
		// caller stream has already received the match via stream.Send.
		// Under the old code (where updateTicketsActiveState ran synchronously
		// before matchChan <- res even when OM_MATCH_TICKET_DEACTIVATION_WAIT=false),
		// stream.Send could never be reached while beforeSend blocked, causing
		// the 1s safety timer below to fire and fail the test deterministically.
		matchStreamedToCaller := make(chan struct{})
		var closeOnce sync.Once
		var deactivateBlockedUntilAfterStreamSend atomic.Bool
		var timedOutWaitingForStreamSend atomic.Bool

		globalTestReplicator.setBeforeSend(func(updates []*store.StateUpdate) {
			for _, u := range updates {
				if u.Cmd == store.Deactivate && (u.Key == ticketID || len(u.Keys) > 0) {
					select {
					case <-matchStreamedToCaller:
						deactivateBlockedUntilAfterStreamSend.Store(true)
					case <-time.After(time.Second):
						timedOutWaitingForStreamSend.Store(true)
					}
					return
				}
			}
		})
		defer globalTestReplicator.setBeforeSend(nil)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer lis.Close()
		_, portStr, err := net.SplitHostPort(lis.Addr().String())
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)

		mmfSrv := &testMidStreamErrorMMFServer{
			matches: []*pb.Match{
				{
					Id: "match-deactivation-wait-false",
					Rosters: map[string]*pb.Roster{
						"roster": {
							Name:    "roster",
							Tickets: []*pb.Ticket{{Id: ticketID}},
						},
					},
				},
			},
		}
		grpcSrv := grpc.NewServer()
		pb.RegisterMatchMakingFunctionServiceServer(grpcSrv, mmfSrv)
		go grpcSrv.Serve(lis) //nolint:errcheck
		defer grpcSrv.Stop()

		var wasInactiveAtStreamSend atomic.Bool
		var streamSendObserved atomic.Bool
		stream := &fakeInvokeMMFServerStream{
			ctx: context.Background(),
			onSend: func(_ *pb.StreamedMmfResponse) {
				_, inactive := tc.InactiveSet.Load(ticketID)
				wasInactiveAtStreamSend.Store(inactive)
				streamSendObserved.Store(true)
				closeOnce.Do(func() {
					close(matchStreamedToCaller)
				})
			},
		}

		req := &pb.MmfRequest{
			Profile: &pb.Profile{
				Name: "com.example.m06.wait_false",
				Pools: map[string]*pb.Pool{
					"all": {Name: "all"},
				},
			},
			Mmfs: []*pb.MatchmakingFunctionSpec{
				{
					Name: "async-deact-mmf",
					Host: "http://127.0.0.1",
					Port: int32(port),
					Type: pb.MatchmakingFunctionSpec_GRPC,
				},
			},
		}

		err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
		require.NoError(t, err)

		assert.True(t, streamSendObserved.Load(), "expected match to be delivered to stream.Send")
		assert.False(t, timedOutWaitingForStreamSend.Load(), "OM_MATCH_TICKET_DEACTIVATION_WAIT=false must stream match before updateTicketsActiveState blocks")
		assert.True(t, deactivateBlockedUntilAfterStreamSend.Load(), "store.Deactivate write must unblock via matchStreamedToCaller closed inside stream.Send")
		assert.False(t, wasInactiveAtStreamSend.Load(), "ticket must still be active at the moment stream.Send delivers the match when OM_MATCH_TICKET_DEACTIVATION_WAIT=false")

		stream.mu.Lock()
		require.Len(t, stream.matches, 1)
		assert.Equal(t, "match-deactivation-wait-false", stream.matches[0].GetId())
		assert.NotContains(t, stream.matches[0].GetExtensions(), MatchDeactivationTimeoutExtensionKey)
		stream.mu.Unlock()

		// By the time InvokeMatchmakingFunctions returns, tdwg.Wait() has waited
		// for the asynchronous deactivation to complete and replicate to tc.InactiveSet.
		_, inactiveAfterReturn := tc.InactiveSet.Load(ticketID)
		assert.True(t, inactiveAfterReturn, "ticket must be deactivated in tc.InactiveSet before InvokeMatchmakingFunctions returns")

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		deactCount, found := findInt64SumPointByMMF(rm, "om_core.mmf.deactivations", "async-deact-mmf", "com.example.m06")
		require.True(t, found, "expected om_core.mmf.deactivations data point")
		assert.Equal(t, int64(1), deactCount)
	})

	t.Run("DeactivationWaitTrueWaitsForLocalReplicationBeforeStreamingMatch", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_MATCH_TICKET_DEACTIVATION_WAIT", true)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		createResp, err := createTicket(context.Background(), &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.NoError(t, err)
		ticketID := createResp.GetTicketId()
		t.Cleanup(func() {
			_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: []string{ticketID},
			})
			require.Eventually(t, func() bool {
				return len(tc.SnapshotActiveTickets()) == 0
			}, 2*time.Second, 5*time.Millisecond)
		})

		_, err = activateTickets(context.Background(), testLogger, &tc, &pb.ActivateTicketsRequest{
			TicketIds: []string{ticketID},
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return len(tc.SnapshotActiveTickets()) == 1
		}, 2*time.Second, 5*time.Millisecond)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer lis.Close()
		_, portStr, err := net.SplitHostPort(lis.Addr().String())
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)

		mmfSrv := &testMidStreamErrorMMFServer{
			matches: []*pb.Match{
				{
					Id: "match-deactivation-wait-true",
					Rosters: map[string]*pb.Roster{
						"roster": {
							Name:    "roster",
							Tickets: []*pb.Ticket{{Id: ticketID}},
						},
					},
				},
			},
		}
		grpcSrv := grpc.NewServer()
		pb.RegisterMatchMakingFunctionServiceServer(grpcSrv, mmfSrv)
		go grpcSrv.Serve(lis) //nolint:errcheck
		defer grpcSrv.Stop()

		var wasInactiveAtStreamSend atomic.Bool
		var streamSendObserved atomic.Bool
		stream := &fakeInvokeMMFServerStream{
			ctx: context.Background(),
			onSend: func(_ *pb.StreamedMmfResponse) {
				_, inactive := tc.InactiveSet.Load(ticketID)
				wasInactiveAtStreamSend.Store(inactive)
				streamSendObserved.Store(true)
			},
		}

		req := &pb.MmfRequest{
			Profile: &pb.Profile{
				Name: "com.example.m06.wait_true",
				Pools: map[string]*pb.Pool{
					"all": {Name: "all"},
				},
			},
			Mmfs: []*pb.MatchmakingFunctionSpec{
				{
					Name: "sync-deact-mmf",
					Host: "http://127.0.0.1",
					Port: int32(port),
					Type: pb.MatchmakingFunctionSpec_GRPC,
				},
			},
		}

		err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
		require.NoError(t, err)

		assert.True(t, streamSendObserved.Load(), "expected match to be delivered to stream.Send")
		assert.True(t, wasInactiveAtStreamSend.Load(), "ticket must already be deactivated in tc.InactiveSet when stream.Send is called with OM_MATCH_TICKET_DEACTIVATION_WAIT=true")
	})

	t.Run("DeactivationWaitFalseSkipsDeactivationWhenCallerStreamAlreadyCancelled", func(t *testing.T) {
		prevCfg := cfg
		subCfg := config.Read()
		subCfg.Set("OM_MATCH_TICKET_DEACTIVATION_WAIT", false)
		cfg = subCfg
		defer func() {
			cfg = prevCfg
		}()

		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.deactivation_wait_false_cancelled")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		createResp, err := createTicket(context.Background(), &tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.NoError(t, err)
		ticketID := createResp.GetTicketId()
		t.Cleanup(func() {
			_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: []string{ticketID},
			})
			require.Eventually(t, func() bool {
				return len(tc.SnapshotActiveTickets()) == 0
			}, 2*time.Second, 5*time.Millisecond)
		})

		_, err = activateTickets(context.Background(), testLogger, &tc, &pb.ActivateTicketsRequest{
			TicketIds: []string{ticketID},
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return len(tc.SnapshotActiveTickets()) == 1
		}, 2*time.Second, 5*time.Millisecond)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer lis.Close()
		_, portStr, err := net.SplitHostPort(lis.Addr().String())
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)

		mmfSrv := &testMidStreamErrorMMFServer{
			matches: []*pb.Match{
				{
					Id: "match-dropped-on-cancelled-stream",
					Rosters: map[string]*pb.Roster{
						"roster": {
							Name:    "roster",
							Tickets: []*pb.Ticket{{Id: ticketID}},
						},
					},
				},
			},
		}
		grpcSrv := grpc.NewServer()
		pb.RegisterMatchMakingFunctionServiceServer(grpcSrv, mmfSrv)
		go grpcSrv.Serve(lis) //nolint:errcheck
		defer grpcSrv.Stop()

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		stream := &fakeInvokeMMFServerStream{ctx: cancelledCtx}

		req := &pb.MmfRequest{
			Profile: &pb.Profile{
				Name: "com.example.m06.wait_false_cancelled",
				Pools: map[string]*pb.Pool{
					"all": {Name: "all"},
				},
			},
			Mmfs: []*pb.MatchmakingFunctionSpec{
				{
					Name: "cancelled-stream-mmf",
					Host: "http://127.0.0.1",
					Port: int32(port),
					Type: pb.MatchmakingFunctionSpec_GRPC,
				},
			},
		}

		err = (&grpcServer{}).InvokeMatchmakingFunctions(req, stream)
		require.NoError(t, err)

		stream.mu.Lock()
		assert.Empty(t, stream.matches, "match must not be queued when stream.Context() is already cancelled")
		stream.mu.Unlock()

		// Because OM_MATCH_TICKET_DEACTIVATION_WAIT=false checks stream.Context().Err()
		// before matchChan <- res and before deactivateMatchTickets(false), the
		// unstreamed match's ticket remains active.
		_, inactive := tc.InactiveSet.Load(ticketID)
		assert.False(t, inactive, "ticket from dropped match must remain active when OM_MATCH_TICKET_DEACTIVATION_WAIT=false and stream is already cancelled")

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		_, foundDeact := findInt64SumPointByMMF(rm, "om_core.mmf.deactivations", "cancelled-stream-mmf", "com.example.m06")
		assert.False(t, foundDeact, "no ticket deactivations should be recorded for dropped match on cancelled stream")
	})
}

// findFloat64HistogramCountAndSum returns the total sample count and sum across
// all data points of a Float64Histogram metric with the given name.
func findFloat64HistogramCountAndSum(rm metricdata.ResourceMetrics, metricName string) (uint64, float64, bool) {
	var count uint64
	var sum float64
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				count += dp.Count
				sum += dp.Sum
				found = true
			}
		}
	}
	return count, sum, found
}

// TestCreateTicketStatusMetricsAndDefaultExpirationFromCreationTime verifies that CreateTicket:
//  1. Preserves codes.InvalidArgument when validateAndMarshalIncomingTicket rejects an
//     invalid ExpirationTime (instead of overwriting it with codes.Internal) and does
//     not record otelTicketSize ("om_core.ticket.size").
//  2. Only records otelTicketSize ("om_core.ticket.size") when state storage replication
//     succeeds (results.Err == nil), and does not record it when replication fails.
//  3. Computes the default ExpirationTime (when ExpirationTime == nil) as
//     CreationTime + OM_CACHE_TICKET_TTL_MS rather than time.Now() + OM_CACHE_TICKET_TTL_MS,
//     so a ticket created with an old CreationTime whose (CreationTime + TTL) has already
//     elapsed is accepted and immediately expired by the cache expiration sweep.
func TestCreateTicketStatusMetricsAndDefaultExpirationFromCreationTime(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("InvalidExpirationTimePreservesInvalidArgumentAndDoesNotRecordTicketSizeMetric", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.create_ticket_invalid_exp")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		maxTTL := time.Millisecond * time.Duration(cfg.GetInt("OM_CACHE_TICKET_TTL_MS"))
		invalidTickets := []*pb.Ticket{
			{ExpirationTime: timestamppb.New(time.Now().Add(-time.Second))},
			{ExpirationTime: timestamppb.New(time.Now().Add(2 * maxTTL))},
			{ExpirationTime: &timestamppb.Timestamp{Seconds: 253402300800, Nanos: 0}},
		}

		srv := &grpcServer{}
		for _, tk := range invalidTickets {
			_, err := srv.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: tk})
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "CreateTicket must preserve codes.InvalidArgument from validateAndMarshalIncomingTicket")
			assert.Contains(t, err.Error(), "specified expiration time not between (now, now +")
		}

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))
		count, _, _ := findFloat64HistogramCountAndSum(rm, "om_core.ticket.size")
		assert.Equal(t, uint64(0), count, "om_core.ticket.size must not be recorded when ticket validation fails")
	})

	t.Run("ReplicationFailureReturnsErrorAndDoesNotRecordTicketSizeMetricWhileSuccessRecordsSize", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.create_ticket_repl_err")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		simErr := errors.New("simulated ticket storage write failure")
		globalTestReplicator.setOnSend(func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
			for i, u := range updates {
				if u.Cmd == store.Ticket {
					results[i] = &store.StateResponse{
						Err: simErr,
					}
				}
			}
			return results
		})
		defer globalTestReplicator.setOnSend(nil)

		srv := &grpcServer{}
		req := &pb.CreateTicketRequest{
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{"ranked", "duo"},
				},
			},
		}

		// 1. Failing replication write must return the replication error and record 0 samples in om_core.ticket.size.
		_, err := srv.CreateTicket(context.Background(), req)
		require.ErrorIs(t, err, simErr)

		var rmFailed metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rmFailed))
		countFailed, _, _ := findFloat64HistogramCountAndSum(rmFailed, "om_core.ticket.size")
		assert.Equal(t, uint64(0), countFailed, "om_core.ticket.size must not be recorded when ticket replication fails")

		// 2. Successful replication write must record 1 sample with positive size in om_core.ticket.size.
		globalTestReplicator.setOnSend(nil)
		resp, err := srv.CreateTicket(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetTicketId())
		t.Cleanup(func() {
			_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: []string{resp.GetTicketId()},
			})
		})

		var rmSuccess metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rmSuccess))
		countSuccess, sumSuccess, found := findFloat64HistogramCountAndSum(rmSuccess, "om_core.ticket.size")
		require.True(t, found, "expected om_core.ticket.size histogram data point after successful CreateTicket")
		assert.Equal(t, uint64(1), countSuccess)
		assert.Greater(t, sumSuccess, 0.0)
	})

	t.Run("DefaultExpirationTimeComputedFromCreationTimeAndOldCreationTimeExpiresImmediately", func(t *testing.T) {
		localCfg := config.Read()
		localCfg.Set("OM_CACHE_TICKET_TTL_MS", 10000)
		localCfg.Set("OM_CACHE_EXPIRATION_INTERVAL_MS", 25)
		localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 10)
		localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
		localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)
		localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)

		prevCfg := cfg
		cfg = localCfg
		defer func() {
			cfg = prevCfg
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var capturedOldTicket pb.Ticket
		var capturedOldTicketMu sync.Mutex
		customRep := &errorInjectingReplicator{
			inner: memoryReplicator.New(localCfg),
		}
		customRep.setBeforeSend(func(updates []*store.StateUpdate) {
			for _, u := range updates {
				if u.Cmd == store.Ticket {
					var tk pb.Ticket
					if err := proto.Unmarshal([]byte(u.Value), &tk); err == nil &&
						tk.GetAttributes().GetCreationTime().AsTime().Equal(time.Unix(0, 0).UTC()) {
						capturedOldTicketMu.Lock()
						proto.Merge(&capturedOldTicket, &tk)
						capturedOldTicketMu.Unlock()
					}
				}
			}
		})

		localTc := cache.New(localCfg, customRep)
		go localTc.OutgoingReplicationQueue(ctx)
		go localTc.IncomingReplicationQueue(ctx)

		ticketTTL := 10000 * time.Millisecond
		epochCreation := timestamppb.New(time.Unix(0, 0).UTC())

		// 1. Create a ticket with CreationTime = 1970-01-01T00:00:00Z and ExpirationTime = nil.
		oldResp, err := createTicket(ctx, localTc, &pb.CreateTicketRequest{
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: epochCreation,
				},
			},
		})
		require.NoError(t, err)
		oldTicketID := oldResp.GetTicketId()

		// 2. Create a control ticket with default CreationTime (now) and ExpirationTime = nil.
		freshResp, err := createTicket(ctx, localTc, &pb.CreateTicketRequest{
			Ticket: &pb.Ticket{},
		})
		require.NoError(t, err)
		freshTicketID := freshResp.GetTicketId()

		// Verify the marshalled old ticket's ExpirationTime was set to CreationTime + TTL (1970-01-01T00:00:10Z),
		// not time.Now() + TTL.
		capturedOldTicketMu.Lock()
		gotExpTime := capturedOldTicket.GetExpirationTime().AsTime()
		capturedOldTicketMu.Unlock()
		assert.Equal(t, time.Unix(0, 0).UTC().Add(ticketTTL), gotExpTime)

		// Wait until both ticket creation updates have been applied by IncomingReplicationQueue.
		require.Eventually(t, func() bool {
			return localTc.HasAppliedReplId(oldTicketID) && localTc.HasAppliedReplId(freshTicketID)
		}, 2*time.Second, 5*time.Millisecond)

		// Because oldTicketID's ExpirationTime (1970 + 10s) has already passed, the
		// periodic cache expiration sweep removes it from localTc.Tickets while
		// freshTicketID (now + 10s) remains present.
		require.Eventually(t, func() bool {
			_, oldExists := localTc.Tickets.Load(oldTicketID)
			_, freshExists := localTc.Tickets.Load(freshTicketID)
			return !oldExists && freshExists
		}, 2*time.Second, 10*time.Millisecond, "ticket with old CreationTime and nil ExpirationTime must expire immediately while fresh control ticket remains in cache")
	})
}

// TestMetricsRpcErrorsCardinalityActivationDeactivationFailureCountsAndWatchMetric
// verifies REC-10 (Task M-09):
//  1. Handler.HandleRPC records om_core.rpc.errors with only bounded ("rpc", "grpc.status")
//     attributes and never attaches the raw dynamic error string ("error" attribute),
//     collapsing distinct error messages with the same (rpc, grpc.status) into a single
//     counter time series.
//  2. activateTickets and deactivateTickets record otelFailedIdsPerActivateCall and
//     otelFailedIdsPerDeactivateCall ("*.failures.unspecified") before merging
//     invalidIdErrorDetails into updateStateErrDetails so invalid ticket IDs are not
//     double-counted in "*.failures.unspecified", while still returning all invalid and
//     replication-failed ticket IDs in the gRPC status BadRequest_FieldViolation details.
//  3. otelAssignmentWatches is registered under "om_core.ticket.assignment.watch" (fixing
//     the "assigment" typo) with description "Number of tickets currently being watched
//     for assignment", distinct from "om_core.ticket.assignment".
func TestMetricsRpcErrorsCardinalityActivationDeactivationFailureCountsAndWatchMetric(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("HandleRPCBoundsRpcErrorsAttributesWithoutRawErrorString", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.m09_rpc_errors")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		h := New()
		createCtx := h.TagRPC(context.Background(), &stats.RPCTagInfo{
			FullMethodName: "/openmatch.OpenMatchService/CreateTicket",
		})
		activateCtx := h.TagRPC(context.Background(), &stats.RPCTagInfo{
			FullMethodName: "/openmatch.OpenMatchService/ActivateTickets",
		})

		t0 := time.Now()
		// 3 CreateTicket InvalidArgument errors with distinct dynamic error messages.
		for i := 1; i <= 3; i++ {
			h.HandleRPC(createCtx, &stats.End{
				BeginTime: t0,
				EndTime:   t0.Add(10 * time.Millisecond),
				Error:     status.Errorf(codes.InvalidArgument, "invalid ticket expiration for ticket-%d at %d", i, t0.UnixNano()+int64(i)),
			})
		}
		// 1 CreateTicket Internal error with dynamic error message.
		h.HandleRPC(createCtx, &stats.End{
			BeginTime: t0,
			EndTime:   t0.Add(15 * time.Millisecond),
			Error:     status.Errorf(codes.Internal, "redis connection reset on 10.0.0.%d:6379", 42),
		})
		// 1 successful CreateTicket call (Error: nil) -> increments rpc.duration but not rpc.errors.
		h.HandleRPC(createCtx, &stats.End{
			BeginTime: t0,
			EndTime:   t0.Add(20 * time.Millisecond),
			Error:     nil,
		})
		// 2 ActivateTickets non-gRPC errors -> status.FromError maps to codes.Unknown.
		for i := 1; i <= 2; i++ {
			h.HandleRPC(activateCtx, &stats.End{
				BeginTime: t0,
				EndTime:   t0.Add(5 * time.Millisecond),
				Error:     fmt.Errorf("unexpected transport error #%d", i),
			})
		}

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(context.Background(), &rm))

		var foundErrorsMetric, foundDurationMetric bool
		errorSeries := make(map[string]int64)
		durationCounts := make(map[string]uint64)

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				switch m.Name {
				case "om_core.rpc.errors":
					foundErrorsMetric = true
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, dp := range sum.DataPoints {
						_, hasErrorAttr := dp.Attributes.Value(attribute.Key("error"))
						assert.False(t, hasErrorAttr, "om_core.rpc.errors must not attach unbounded 'error' attribute")
						assert.Equal(t, 2, dp.Attributes.Len(), "om_core.rpc.errors data point must have exactly 2 attributes (rpc, grpc.status)")

						rpcVal, hasRPC := dp.Attributes.Value(attribute.Key("rpc"))
						statusVal, hasStatus := dp.Attributes.Value(attribute.Key("grpc.status"))
						require.True(t, hasRPC)
						require.True(t, hasStatus)
						errorSeries[rpcVal.AsString()+"|"+statusVal.AsString()] = dp.Value
					}
				case "om_core.rpc.duration":
					foundDurationMetric = true
					hist, ok := m.Data.(metricdata.Histogram[float64])
					require.True(t, ok)
					for _, dp := range hist.DataPoints {
						rpcVal, hasRPC := dp.Attributes.Value(attribute.Key("rpc"))
						require.True(t, hasRPC)
						durationCounts[rpcVal.AsString()] = dp.Count
						assert.Greater(t, dp.Sum, 0.0)
					}
				}
			}
		}

		require.True(t, foundErrorsMetric, "expected om_core.rpc.errors metric")
		require.True(t, foundDurationMetric, "expected om_core.rpc.duration metric")
		assert.Equal(t, map[string]int64{
			"CreateTicket|InvalidArgument": 3,
			"CreateTicket|Internal":        1,
			"ActivateTickets|Unknown":      2,
		}, errorSeries, "distinct dynamic error messages with identical (rpc, grpc.status) must aggregate into bounded series")
		assert.Equal(t, map[string]uint64{
			"CreateTicket":    5,
			"ActivateTickets": 2,
		}, durationCounts)
	})

	t.Run("ActivateAndDeactivateTicketsDoNotDoubleCountInvalidIdsInUnspecifiedFailures", func(t *testing.T) {
		t.Run("MixedValidAndInvalidIdsWithAllValidSucceeding", func(t *testing.T) {
			metricReader := sdkmetric.NewManualReader()
			testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
			defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
			testMeter := testMeterProvider.Meter("open_match.core.m09_mixed_all_valid_ok")

			prevMeter := meter
			registerMetrics(&testMeter)
			defer func() {
				if prevMeter != nil {
					registerMetrics(prevMeter)
				}
			}()

			nowMs := time.Now().UnixMilli()
			valid1 := fmt.Sprintf("%013d-901", nowMs)
			valid2 := fmt.Sprintf("%013d-902", nowMs)
			badIDs := []string{"bad-ticket-1", "bad-ticket-2", "bad-ticket-3"}
			mixedIDs := []string{valid1, badIDs[0], valid2, badIDs[1], badIDs[2]}

			// 1. ActivateTickets with 2 valid IDs + 3 invalid IDs (all valid IDs succeed replication).
			_, err := activateTickets(context.Background(), testLogger, &tc, &pb.ActivateTicketsRequest{
				TicketIds: mixedIDs,
			})
			require.Error(t, err)
			stAct := status.Convert(err)
			assert.Equal(t, codes.InvalidArgument, stAct.Code())
			assert.ElementsMatch(t, badIDs, getStatusDetailBrvKeys(stAct))

			// 2. DeactivateTickets with 2 valid IDs + 3 invalid IDs (all valid IDs succeed replication).
			_, err = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: mixedIDs,
			})
			require.Error(t, err)
			stDeact := status.Convert(err)
			assert.Equal(t, codes.InvalidArgument, stDeact.Code())
			assert.ElementsMatch(t, badIDs, getStatusDetailBrvKeys(stDeact))

			var rm metricdata.ResourceMetrics
			require.NoError(t, metricReader.Collect(context.Background(), &rm))

			actReqCount, actReqSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.requests")
			require.True(t, found)
			assert.Equal(t, uint64(1), actReqCount)
			assert.Equal(t, int64(2), actReqSum, "activation.requests must record the 2 valid ticket IDs")

			actInvCount, actInvSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.failures.invalid_id")
			require.True(t, found)
			assert.Equal(t, uint64(1), actInvCount)
			assert.Equal(t, int64(3), actInvSum, "activation.failures.invalid_id must record the 3 invalid ticket IDs")

			actFailCount, actFailSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.failures.unspecified")
			require.True(t, found)
			assert.Equal(t, uint64(1), actFailCount)
			assert.Equal(t, int64(0), actFailSum, "activation.failures.unspecified must be 0 when all valid IDs succeed (never double-counting the 3 invalid IDs)")

			deactReqCount, deactReqSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.requests")
			require.True(t, found)
			assert.Equal(t, uint64(1), deactReqCount)
			assert.Equal(t, int64(2), deactReqSum, "deactivation.requests must record the 2 valid ticket IDs")

			deactInvCount, deactInvSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.failures.invalid_id")
			require.True(t, found)
			assert.Equal(t, uint64(1), deactInvCount)
			assert.Equal(t, int64(3), deactInvSum, "deactivation.failures.invalid_id must record the 3 invalid ticket IDs")

			deactFailCount, deactFailSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.failures.unspecified")
			require.True(t, found)
			assert.Equal(t, uint64(1), deactFailCount)
			assert.Equal(t, int64(0), deactFailSum, "deactivation.failures.unspecified must be 0 when all valid IDs succeed (never double-counting the 3 invalid IDs)")
		})

		t.Run("MixedValidAndInvalidIdsWithSubsetOfValidFailingReplication", func(t *testing.T) {
			metricReader := sdkmetric.NewManualReader()
			testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
			defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
			testMeter := testMeterProvider.Meter("open_match.core.m09_mixed_subset_repl_fail")

			prevMeter := meter
			registerMetrics(&testMeter)
			defer func() {
				if prevMeter != nil {
					registerMetrics(prevMeter)
				}
			}()

			localCfg := config.Read()
			localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)
			localCfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			nowMs := time.Now().UnixMilli()
			validOK := fmt.Sprintf("%013d-911", nowMs)
			validFail1 := fmt.Sprintf("%013d-912", nowMs)
			validFail2 := fmt.Sprintf("%013d-913", nowMs)
			invalidIDs := []string{"invalid-id-1", "invalid-id-2", "invalid-id-3", "invalid-id-4"}

			selectiveRep := &errorInjectingReplicator{
				inner: memoryReplicator.New(localCfg),
				onSend: func(updates []*store.StateUpdate, results []*store.StateResponse) []*store.StateResponse {
					for i, u := range updates {
						if u.Key == validFail1 || u.Key == validFail2 {
							results[i] = &store.StateResponse{
								Result: u.Key,
								Err:    fmt.Errorf("simulated replication failure for %s", u.Key),
							}
						}
					}
					return results
				},
			}
			localTc := cache.New(localCfg, selectiveRep)
			go localTc.OutgoingReplicationQueue(ctx)

			mixedInput := []string{
				validOK,
				invalidIDs[0],
				validFail1,
				invalidIDs[1],
				validFail2,
				invalidIDs[2],
				invalidIDs[3],
			}
			expectedViolationKeys := []string{
				validFail1,
				validFail2,
				invalidIDs[0],
				invalidIDs[1],
				invalidIDs[2],
				invalidIDs[3],
			}

			// 1. ActivateTickets: 3 valid IDs (1 succeeds, 2 fail replication) + 4 invalid IDs.
			_, err := activateTickets(ctx, testLogger, localTc, &pb.ActivateTicketsRequest{
				TicketIds: mixedInput,
			})
			require.Error(t, err)
			stAct := status.Convert(err)
			assert.Equal(t, codes.InvalidArgument, stAct.Code())
			assert.ElementsMatch(t, expectedViolationKeys, getStatusDetailBrvKeys(stAct),
				"returned status details must still include all 6 failed keys (2 replication failures + 4 invalid IDs)")

			// 2. DeactivateTickets: 3 valid IDs (1 succeeds, 2 fail replication) + 4 invalid IDs.
			_, err = deactivateTickets(ctx, testLogger, localTc, &pb.DeactivateTicketsRequest{
				TicketIds: mixedInput,
			})
			require.Error(t, err)
			stDeact := status.Convert(err)
			assert.Equal(t, codes.InvalidArgument, stDeact.Code())
			assert.ElementsMatch(t, expectedViolationKeys, getStatusDetailBrvKeys(stDeact),
				"returned status details must still include all 6 failed keys (2 replication failures + 4 invalid IDs)")

			var rm metricdata.ResourceMetrics
			require.NoError(t, metricReader.Collect(context.Background(), &rm))

			_, actReqSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.requests")
			require.True(t, found)
			assert.Equal(t, int64(3), actReqSum, "activation.requests must record the 3 valid ticket IDs")

			_, actInvSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.failures.invalid_id")
			require.True(t, found)
			assert.Equal(t, int64(4), actInvSum, "activation.failures.invalid_id must record the 4 invalid ticket IDs")

			_, actFailSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.activation.failures.unspecified")
			require.True(t, found)
			assert.Equal(t, int64(2), actFailSum, "activation.failures.unspecified must record only the 2 replication failures (not 2+4=6)")

			_, deactReqSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.requests")
			require.True(t, found)
			assert.Equal(t, int64(3), deactReqSum, "deactivation.requests must record the 3 valid ticket IDs")

			_, deactInvSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.failures.invalid_id")
			require.True(t, found)
			assert.Equal(t, int64(4), deactInvSum, "deactivation.failures.invalid_id must record the 4 invalid ticket IDs")

			_, deactFailSum, found := findInt64HistogramCountAndSum(rm, "om_core.ticket.deactivation.failures.unspecified")
			require.True(t, found)
			assert.Equal(t, int64(2), deactFailSum, "deactivation.failures.unspecified must record only the 2 replication failures (not 2+4=6)")
		})
	})

	t.Run("AssignmentWatchMetricNameAndDescription", func(t *testing.T) {
		metricReader := sdkmetric.NewManualReader()
		testMeterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
		defer testMeterProvider.Shutdown(context.Background()) //nolint:errcheck
		testMeter := testMeterProvider.Meter("open_match.core.m09_watch_metric")

		prevMeter := meter
		registerMetrics(&testMeter)
		defer func() {
			if prevMeter != nil {
				registerMetrics(prevMeter)
			}
		}()

		ctx := context.Background()
		otelAssignmentsPerCall.Record(ctx, 2)
		otelAssignmentWatches.Add(ctx, 3)
		otelAssignmentWatches.Add(ctx, -3)

		var rm metricdata.ResourceMetrics
		require.NoError(t, metricReader.Collect(ctx, &rm))

		descriptions := make(map[string]string)
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				descriptions[m.Name] = m.Description
			}
		}

		assert.NotContains(t, descriptions, "om_core.ticket.assigment.watch", "misspelled om_core.ticket.assigment.watch metric must not be registered")
		require.Contains(t, descriptions, "om_core.ticket.assignment.watch", "expected corrected om_core.ticket.assignment.watch metric")
		assert.Equal(t, "Number of tickets currently being watched for assignment", descriptions["om_core.ticket.assignment.watch"])
		assert.Equal(t, "Number of tickets assignments included in each CreateAssignments() call", descriptions["om_core.ticket.assignment"])
	})
}

// TestGracefulShutdownAndContextCancellation verifies REC-11 (Task M-10):
//  1. createTicket respects parentCtx cancellation both when tc.UpRequests is
//     full (blocking at send) and when waiting on <-rChan, and uses a 1-buffered
//     rChan so a late writer never blocks after caller cancellation.
//  2. updateTicketsActiveState respects parentCtx cancellation in both packed
//     and unpacked modes (both when blocked enqueuing to tc.UpRequests and when
//     blocked waiting on <-rChan / <-rChans[i]), attributing the context error
//     to each affected ticket ID.
//  3. start(ctx, cfg) starts the gRPC server and HTTP gRPC-Gateway proxy and,
//     when ctx is cancelled, gracefully shuts down both servers and returns
//     promptly without hanging on an unbuffered signal channel.
func TestGracefulShutdownAndContextCancellation(t *testing.T) {
	ensureGlobalTestTicketCache()

	t.Run("CreateTicketRespectsContextCancellationWhenQueueFullOrWaitingForResult", func(t *testing.T) {
		localCfg := config.Read()
		localCfg.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 1)

		// Case 1: tc.UpRequests is full and no OutgoingReplicationQueue is running ->
		// createTicket unblocks at `select { case tc.UpRequests <- reqUpdate: case <-parentCtx.Done(): }`.
		fullTc := cache.New(localCfg, memoryReplicator.New(localCfg))
		fullTc.UpRequests <- &cache.UpdateRequest{
			ResultsChan: make(chan *store.StateResponse, 1),
			Update:      store.StateUpdate{Cmd: store.Ticket},
		}

		timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancelTimeout()

		startTime := time.Now()
		_, err := createTicket(timeoutCtx, fullTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		assert.Less(t, time.Since(startTime), 250*time.Millisecond)
		assert.Len(t, fullTc.UpRequests, 1)

		// Case 2: tc.UpRequests has buffer space so reqUpdate is enqueued, and createTicket
		// blocks at `select { case results = <-rChan: case <-parentCtx.Done(): }`.
		waitTc := cache.New(localCfg, memoryReplicator.New(localCfg))
		cancelCtx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		startWait := time.Now()
		_, err = createTicket(cancelCtx, waitTc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.Error(t, err)
		assert.Equal(t, codes.Canceled, status.Code(err))
		assert.Less(t, time.Since(startWait), 250*time.Millisecond)

		// Verify reqUpdate was enqueued with a 1-buffered ResultsChan so a late
		// OutgoingReplicationQueue write never blocks after caller cancellation.
		require.Len(t, waitTc.UpRequests, 1)
		enqueued := <-waitTc.UpRequests
		select {
		case enqueued.ResultsChan <- &store.StateResponse{Result: "1790307871351-1"}:
		default:
			t.Fatal("createTicket ResultsChan must be 1-buffered so late replication responses do not block")
		}
	})

	t.Run("UpdateTicketsActiveStateRespectsContextCancellationInPackedAndUnpackedModes", func(t *testing.T) {
		t.Run("PackedMode_BlockedOnEnqueueAndBlockedOnResponse", func(t *testing.T) {
			localCfg := config.Read()
			localCfg.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", true)
			localCfg.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 1)

			ids := []string{"1790307871351-701", "1790307871351-702"}

			// Subcase 1: tc.UpRequests is full -> unblocks at enqueue on <-parentCtx.Done().
			fullTc := cache.New(localCfg, memoryReplicator.New(localCfg))
			fullTc.UpRequests <- &cache.UpdateRequest{
				ResultsChan: make(chan *store.StateResponse, 1),
			}
			ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel1()

			errs, maxReplId := updateTicketsActiveState(ctx1, testLogger, fullTc, ids, store.Activate)
			assert.Empty(t, maxReplId)
			require.Len(t, errs, 2)
			for _, id := range ids {
				assert.Equal(t, codes.DeadlineExceeded, status.Code(errs[id]))
			}

			// Subcase 2: tc.UpRequests accepts reqUpdate, then blocks at <-rChan until ctx2 is cancelled.
			waitTc := cache.New(localCfg, memoryReplicator.New(localCfg))
			ctx2, cancel2 := context.WithCancel(context.Background())
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancel2()
			}()

			errs, maxReplId = updateTicketsActiveState(ctx2, testLogger, waitTc, ids, store.Deactivate)
			assert.Empty(t, maxReplId)
			require.Len(t, errs, 2)
			for _, id := range ids {
				assert.Equal(t, codes.Canceled, status.Code(errs[id]))
			}

			// Late send to ResultsChan must not block.
			require.Len(t, waitTc.UpRequests, 1)
			enqueued := <-waitTc.UpRequests
			select {
			case enqueued.ResultsChan <- &store.StateResponse{Result: "1790307871351-702"}:
			default:
				t.Fatal("packed updateTicketsActiveState ResultsChan must be 1-buffered")
			}
		})

		t.Run("UnpackedMode_PartialEnqueueAndPartialResponseWait", func(t *testing.T) {
			// Subcase 1: Buffer size 1 with 3 ticket IDs -> id1 enqueues, id2 blocks at
			// enqueue until context cancellation, attributing codes.Canceled to all 3 IDs.
			cfgSmallBuf := config.Read()
			cfgSmallBuf.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)
			cfgSmallBuf.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 1)

			ids := []string{"1790307871351-711", "1790307871351-712", "1790307871351-713"}
			smallBufTc := cache.New(cfgSmallBuf, memoryReplicator.New(cfgSmallBuf))

			ctx1, cancel1 := context.WithCancel(context.Background())
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancel1()
			}()

			errs, maxReplId := updateTicketsActiveState(ctx1, testLogger, smallBufTc, ids, store.Activate)
			assert.Empty(t, maxReplId)
			require.Len(t, errs, 3)
			for _, id := range ids {
				assert.Equal(t, codes.Canceled, status.Code(errs[id]))
			}

			// Subcase 2: Buffer size 10 with 3 ticket IDs -> all 3 enqueue; id1 receives a
			// valid response before ctx2 times out while waiting on id2's response channel.
			cfgLargeBuf := config.Read()
			cfgLargeBuf.Set("OM_CACHE_PACK_TICKET_STATE_UPDATES", false)
			cfgLargeBuf.Set("OM_CACHE_OUT_QUEUE_BUFFER_SIZE", 10)

			partialTc := cache.New(cfgLargeBuf, memoryReplicator.New(cfgLargeBuf))
			ctx2, cancel2 := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel2()

			go func() {
				// Deliver response only to the first queued request (id1), leaving id2 and id3 pending.
				firstReq := <-partialTc.UpRequests
				firstReq.ResultsChan <- &store.StateResponse{Result: "1790307871351-799"}
			}()

			errs, maxReplId = updateTicketsActiveState(ctx2, testLogger, partialTc, ids, store.Deactivate)
			assert.Equal(t, "1790307871351-799", maxReplId, "completed update before cancellation must preserve maxReplId")
			assert.NotContains(t, errs, ids[0], "id1 succeeded before timeout and must not be in errs")
			require.Contains(t, errs, ids[1])
			require.Contains(t, errs, ids[2])
			assert.Equal(t, codes.DeadlineExceeded, status.Code(errs[ids[1]]))
			assert.Equal(t, codes.DeadlineExceeded, status.Code(errs[ids[2]]))
		})
	})

	t.Run("ServerStartAndGracefulShutdownOnContextCancel", func(t *testing.T) {
		// Reserve two ephemeral TCP ports for OM_GRPC_PORT and PORT.
		grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		_, grpcPort, err := net.SplitHostPort(grpcLis.Addr().String())
		require.NoError(t, err)

		httpLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		_, httpPort, err := net.SplitHostPort(httpLis.Addr().String())
		require.NoError(t, err)

		require.NoError(t, grpcLis.Close())
		require.NoError(t, httpLis.Close())

		serverCfg := config.Read()
		serverCfg.Set("OM_GRPC_PORT", grpcPort)
		serverCfg.Set("PORT", httpPort)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		startDone := make(chan struct{})
		go func() {
			defer close(startDone)
			start(ctx, serverCfg)
		}()

		// Wait until both the gRPC server and the HTTP gRPC-Gateway proxy accept connections,
		// and issue a real gRPC request and HTTP request before cancelling ctx.
		require.Eventually(t, func() bool {
			conn, dialErr := net.DialTimeout("tcp", "127.0.0.1:"+grpcPort, 25*time.Millisecond)
			if dialErr != nil {
				return false
			}
			_ = conn.Close()
			httpConn, httpDialErr := net.DialTimeout("tcp", "127.0.0.1:"+httpPort, 25*time.Millisecond)
			if httpDialErr != nil {
				return false
			}
			_ = httpConn.Close()
			return true
		}, 2*time.Second, 10*time.Millisecond, "gRPC and HTTP servers should start listening")

		// Verify live gRPC server responds to an RPC.
		grpcConn, err := grpc.NewClient("127.0.0.1:"+grpcPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer grpcConn.Close()
		client := pb.NewOpenMatchServiceClient(grpcConn)
		createResp, err := client.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
		require.NoError(t, err)
		require.NotEmpty(t, createResp.GetTicketId())
		t.Cleanup(func() {
			_, _ = deactivateTickets(context.Background(), testLogger, &tc, &pb.DeactivateTicketsRequest{
				TicketIds: []string{createResp.GetTicketId()},
			})
		})

		// Verify HTTP gRPC-Gateway server responds to an HTTP request.
		httpResp, err := http.Get("http://127.0.0.1:" + httpPort + "/")
		require.NoError(t, err)
		_ = httpResp.Body.Close()

		// Cancel ctx to trigger graceful shutdown of both HTTP and gRPC servers.
		shutdownStart := time.Now()
		cancel()

		select {
		case <-startDone:
		case <-time.After(2 * time.Second):
			t.Fatal("start(ctx, cfg) did not shut down and return within 2s after context cancellation")
		}
		assert.Less(t, time.Since(shutdownStart), 2*time.Second)

		// Both server listeners must be closed after start() returns.
		_, grpcErr := net.DialTimeout("tcp", "127.0.0.1:"+grpcPort, 25*time.Millisecond)
		assert.Error(t, grpcErr, "gRPC listener must be closed after start() returns")
		_, httpErr := net.DialTimeout("tcp", "127.0.0.1:"+httpPort, 25*time.Millisecond)
		assert.Error(t, httpErr, "HTTP listener must be closed after start() returns")
	})
}


