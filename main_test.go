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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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

	// One global instance for the local ticket cache. Everything reads and
	// writes to this one instance which contains concurrent-safe data
	// structures where necessary.
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

const redisUri = "localhost:6379"

// TestMain sets up both the in-memory mock ticket replicator and the actual
// redis ticket replicator (using local redis and falling back to
// testcontainers) so tests can run against both types and confirm they produce
// the expected results.
func TestMain(m *testing.M) {
	logrus.SetLevel(logrus.FatalLevel)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetLevel(logrus.TraceLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	os.Setenv("OM_REDIS_DIAL_MAX_BACKOFF_TIMEOUT", "10s")

	redisConfig := func(uri string) {
		// Attempt to connect to locally running redis
		redisUrl, err := url.Parse(uri)
		if err != nil {
			testLogger.Fatalf("failed to parse connection string for redis: %s", err)
		}
		os.Setenv("REDISHOST", redisUrl.Hostname())
		os.Setenv("REDISPORT", redisUrl.Port())
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
			testLogger.Infof("got connection string for redis container: %s", err)
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

// TestCreateTicket tests the createTicket() function, which is the internal
// implementation of the gRPC CreateTicket() function defined in
// proto/v2/api.proto .
//
// This does combinatorial creation of tickets with every valid attribute we
// want to test, which makes it fail complexity linter.
//
//nolint:gocognit,cyclop
func TestCreateTicket(t *testing.T) {
	// expiration times that should fail validation
	inValidEts := map[string]*pb.Ticket{
		// Now() will always fail, because by the time you send the timestamp to
		// the validation function, time will have advanced past it.
		"now": &pb.Ticket{ExpirationTime: timestamppb.Now()},
		// 1/1/1970
		"epoch": &pb.Ticket{ExpirationTime: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}},
		// Timestamppb max allowed timestamp value
		"last_possible": &pb.Ticket{ExpirationTime: &timestamppb.Timestamp{Seconds: 253402300800, Nanos: 0}},
	}

	// Expected failure status for invalid expiration times
	invalidExpTimeStatus := status.New(codes.InvalidArgument,
		fmt.Sprintf("specified expiration time not between (now, now + %v)",
			cfg.GetInt("OM_CACHE_TICKET_TTL_MS")))

	// Make sure all expected invalid expiration times do fail validation correctly
	for testCaseName, thisTestCase := range inValidEts {
		t.Run(fmt.Sprintf("InvalidExpTime=%v", testCaseName), func() func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				_, err := validateAndMarshalIncomingTicket(&pb.CreateTicketRequest{Ticket: thisTestCase})
				// Check expected error was returned
				s := status.Convert(err)
				assert.Equal(t, invalidExpTimeStatus.Code(), s.Code())
				assert.Contains(t, s.Message(), invalidExpTimeStatus.Message())
			}
		}())
	}

	// valid filterable data attribute test cases
	tags := []*pb.Ticket_FilterableData{
		&pb.Ticket_FilterableData{},
		&pb.Ticket_FilterableData{Tags: []string{"bonusxp"}},
	}
	sas := []*pb.Ticket_FilterableData{
		&pb.Ticket_FilterableData{},
		&pb.Ticket_FilterableData{StringArgs: map[string]string{"class": "tank"}},
	}
	das := []*pb.Ticket_FilterableData{
		&pb.Ticket_FilterableData{},
		&pb.Ticket_FilterableData{DoubleArgs: map[string]float64{"mmr": 1200.0}},
	}
	cts := []*pb.Ticket_FilterableData{
		&pb.Ticket_FilterableData{},
		&pb.Ticket_FilterableData{CreationTime: &timestamppb.Timestamp{Seconds: 0, Nanos: 0}},
		&pb.Ticket_FilterableData{CreationTime: timestamppb.Now()},
	}
	validEts := map[string]*pb.Ticket{
		"empty": &pb.Ticket{},
		"now+halfMaxTTL": &pb.Ticket{ExpirationTime: timestamppb.New(time.Now().Add(
			(time.Millisecond * time.Duration(cfg.GetInt("OM_CACHE_TICKET_TTL_MS"))) / 2))},
		"now+maxTTL": &pb.Ticket{ExpirationTime: timestamppb.New(time.Now().Add(
			time.Millisecond * time.Duration(cfg.GetInt("OM_CACHE_TICKET_TTL_MS"))))},
		"now+matTTL-1": &pb.Ticket{ExpirationTime: timestamppb.New(time.Now().Add(
			time.Millisecond * time.Duration(cfg.GetInt("OM_CACHE_TICKET_TTL_MS")-1)))},
	}

	// Combine the ways of making empty contents to generate different empty
	// valid Attributes possibilities.
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

	// Test that all valid combinations of expiration times, and attributes are parsed correctly.
	for eCaseName, testExpirationTime := range validEts {
		for _, testAttrs := range attrs {
			t.Run(fmt.Sprintf("ValidTicketAttrs-ExpTime=%v-FilterableData=%v", eCaseName, testAttrs), func(attrs *pb.Ticket_FilterableData) func(t *testing.T) {
				return func(t *testing.T) {
					t.Parallel()
					request := &pb.CreateTicketRequest{
						Ticket: &pb.Ticket{ExpirationTime: testExpirationTime.GetExpirationTime(),
							Attributes: attrs}}
					result := &pb.Ticket{}

					// First, check that the ticket processing works correctly
					marshalledTicket, err := validateAndMarshalIncomingTicket(request)
					require.NoError(t, err)
					proto.Unmarshal(marshalledTicket, result) //nolint:errcheck
					// Filterable Data Attributes being copied correctly to the new
					// ticket?
					if len(attrs.GetTags()) > 0 {
						assert.Equal(t, attrs.GetTags(),
							result.GetAttributes().GetTags())
					}
					if len(attrs.GetStringArgs()) > 0 {
						assert.Equal(t,
							attrs.GetStringArgs(),
							result.GetAttributes().GetStringArgs())
					}
					if len(attrs.GetDoubleArgs()) > 0 {
						assert.Equal(t, attrs.GetDoubleArgs(),
							result.GetAttributes().GetDoubleArgs())
					}
					if attrs.GetCreationTime().IsValid() {
						// Note: have to use AsTime() to convert
						// timestamppb.Timestamp to time.Time because
						// timestamppb.Timestamp has a DoNotCompare pragma set in
						// its golang object definition
						assert.Equal(t, attrs.GetCreationTime().AsTime(),
							result.GetAttributes().GetCreationTime().AsTime())
					}

					// Creation and expiration time being generated correctly?
					assert.True(t, result.GetAttributes().GetCreationTime().IsValid())
					assert.True(t, result.GetExpirationTime().IsValid())

					// Check that the ticket insertion code for the replication layer of
					// the ticket cache works correctly.
					for tcTypeName, tc := range tcs {
						response, err := createTicket(context.Background(), tc, request)
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
// and DeactivateTickets() defined in proto/v2/api.proto .
func TestValidateTicketStateUpdates(t *testing.T) {

	// Table-driven test cases
	type testcase struct {
		in                   []string
		exValidIds           []string
		exInvalidDetailsKeys []string
		exError              error
	}
	tests := map[string]*testcase{
		//testcase{name:, in:, exValidIds:, exInvalidDetails:, exError:},
		"empty_input": &testcase{
			in:                   []string{},
			exValidIds:           nil,
			exInvalidDetailsKeys: nil,
			exError:              NoTicketIdsErr},
		"valid_input": &testcase{
			in:                   validTicketIds,
			exValidIds:           validTicketIds,
			exInvalidDetailsKeys: nil,
			exError:              nil},
		"invalid_input": &testcase{
			in:                   invalidTicketIds,
			exValidIds:           nil,
			exInvalidDetailsKeys: nil,
			exError:              NoValidTicketIdsErr},
		"partial_valid_input": &testcase{
			in:                   append(validTicketIds, invalidTicketIds...),
			exValidIds:           validTicketIds,
			exInvalidDetailsKeys: invalidTicketIds,
			exError:              InvalidIdErr},
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
						assert.Empty(t, err)
					} else {
						// Check expected error was returned
						require.EqualError(t, err, thisTestCase.exError.Error())
					}

					// Get all keys from the invalid id details map
					invalidIds := []string{}
					for k, v := range invalid {
						// expected invalid ticket id error
						require.EqualError(t, v, InvalidIdErr.Error())
						invalidIds = append(invalidIds, k)
					}

					// Check that map of invalid ids errors contained every key we expected
					assert.ElementsMatch(t, invalidIds, thisTestCase.exInvalidDetailsKeys)
				}
			}(thisTestCase))
		}
	}
}

// TestActivateDeactivateTickets tests only that the activateTickets() and
// deactivateTickets() function calls successfully perform their validation
// tasks. It does not validate that the actual ticket state updates are
// successful (that is a job for unit tests of the state storage layer and e2e
// tests that are generating real ticket IDs).
//
// activateTickets() and deactivateTickets() are the internal implementations of the gRPC functions ActivateTickets() and DeactivateTickets() defined in
// proto/v2/api.proto .
//
// This loops over every testcase and runs it against activation and
// deactivation validation codepaths, which makes it fail the complexity linter.
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
		"empty_input": &testcase{
			in:                   []string{},
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.InvalidArgument, NoTicketIdsErr.Error())},
		"valid_input": &testcase{
			in:                   validTicketIds,
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.OK, "")},
		"invalid_input": &testcase{
			in:                   invalidTicketIds,
			exInvalidDetailsKeys: nil,
			exStatus:             status.New(codes.InvalidArgument, NoValidTicketIdsErr.Error())},
		"partial_valid_input": &testcase{
			in:                   append(validTicketIds, invalidTicketIds...),
			exInvalidDetailsKeys: invalidTicketIds,
			exStatus:             status.New(codes.InvalidArgument, InvalidIdErr.Error())},
	}

	// Validate that all types of ticket cache replication produce the same results.
	for replType, tc := range tcs {

		// Run all tests in the table.
		for testName, thisTestCase := range tests {

			// Check that each test in the table also completes even if context
			// is cancelled (these operations do not support cancellation)
			for _, abort := range []bool{false, true} {
				abortTitleString := ""
				if abort {
					abortTitleString = "[context_cancelled]"
				}

				// Check that both kinds of state update (activation and deactivation) give the expected results.
				for _, updateType := range []string{"Activate", "Deactivate"} {
					t.Run(fmt.Sprintf("%vTicket-%v-%v%v", updateType, replType, testName, abortTitleString),
						func(thisTestCase *testcase) func(t *testing.T) {
							return func(t *testing.T) {
								t.Parallel()

								// function call
								ctx, cancel := context.WithCancel(context.Background())
								defer cancel()
								if abort {
									go func() {
										time.Sleep(25 * time.Millisecond)
										cancel() // test that the function works properly when context is cancelled
									}()
								}

								var err error
								if updateType == "Activate" {
									// Activation is successful if ticketid does NOT appear in the inactive set
									_, err = activateTickets(ctx, testLogger, tc, &pb.ActivateTicketsRequest{TicketIds: thisTestCase.in})
								} else if updateType == "Deactivate" {
									// Activation is successful if ticketid does appear in the inactive set
									_, err = deactivateTickets(ctx, testLogger, tc, &pb.DeactivateTicketsRequest{TicketIds: thisTestCase.in})
								}

								// Check expected error was returned
								s := status.Convert(err)
								assert.Equal(t, thisTestCase.exStatus.Code(), s.Code())
								assert.Contains(t, s.Message(), thisTestCase.exStatus.Message())

								// Get all keys from the invalid id details map of the status BadRequest.FieldViolations array
								// and make sure it contains all expected keys.
								deets := s.Details()
								if len(deets) > 0 {
									assert.ElementsMatch(t, getStatusDetailBrvKeys(s), thisTestCase.exInvalidDetailsKeys)
								}

							}
						}(thisTestCase))
				}
			}
		}
	}
}

// Parse the grpc Status object and return the ticket ids (i.e. 'keys') of all
// the BadRequest.FieldViolation detail messages.
func getStatusDetailBrvKeys(s *status.Status) []string {
	failedKeys := []string{}
	// invalid IDs are returned in the details field of the grpc Status message type
	for _, detail := range s.Details() {
		switch failure := detail.(type) {
		case *errdetails.BadRequest:
			// There should be one violation for each invalid ID
			for _, violation := range failure.GetFieldViolations() {
				// The 'Field' fields of the FieldViolations message contains the string 'ticket_id/<ticketId>'
				testLogger.Debugf("failed id: '%v'", strings.Split(violation.GetField(), "/")[1])
				failedKeys = append(failedKeys, strings.Split(violation.GetField(), "/")[1])
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
// a ticket appears in InactiveSet and when the replication watermark advances
// past targetReplId (e.g. if the ticket expired during the same cycle).
func TestWaitForDeactivation(t *testing.T) {
	t.Parallel()
	for replType, tcInstance := range tcs {
		t.Run("WaitForDeactivation-"+replType, func(tc *cache.ReplicatedTicketCache) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				resp, err := createTicket(context.Background(), tc, &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
				require.NoError(t, err)
				ticketId := resp.GetTicketId()

				errs, maxReplId := updateTicketsActiveState(context.Background(), testLogger, tc, []string{ticketId}, 2) // store.Deactivate == 2
				require.Empty(t, errs)
				require.NotEmpty(t, maxReplId)

				ok := tc.WaitForDeactivation(ticketId, maxReplId, 5*time.Second)
				assert.True(t, ok, "expected deactivation to replicate within timeout")
			}
		}(tcInstance))
	}
}

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
	errs, maxReplId := updateTicketsActiveState(ctx, testLogger, localTc, ticketIds, 2) // store.Deactivate
	require.Empty(t, errs)
	require.NotEmpty(t, maxReplId)

	require.True(t, localTc.WaitForDeactivation(ticketIds[len(ticketIds)-1], maxReplId, 2*time.Second))
	for _, id := range ticketIds {
		_, inactive := localTc.InactiveSet.Load(id)
		assert.True(t, inactive)
	}
}

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

		errs, maxReplId := updateTicketsActiveState(ctx, testLogger, runningTc, []string{ticketId}, 2) // store.Deactivate
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
			// Still drain or proceed if client didn't CloseSend before reading.
			break
		}
		// InvokeMatchmakingFunctions sends NumChunks chunks without calling CloseSend,
		// so break once we receive the single chunk in this test.
		break
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

	// Configure global tc with running replication queues and two active tickets
	// created and activated through the production code paths.
	localCfg := config.Read()
	localCfg.Set("OM_CACHE_IN_WAIT_TIMEOUT_MS", 10)
	localCfg.Set("OM_CACHE_IN_POLL_WAIT_MS", 5)
	localCfg.Set("OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS", 5)
	localCfg.Set("OM_CACHE_OUT_WAIT_TIMEOUT_MS", 5)

	tcCtx, tcCancel := context.WithCancel(context.Background())
	defer tcCancel()
	tc = cache.ReplicatedTicketCache{}
	tc.Init(localCfg, memoryReplicator.New(localCfg))
	go tc.OutgoingReplicationQueue(tcCtx)
	go tc.IncomingReplicationQueue(tcCtx)

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
}
