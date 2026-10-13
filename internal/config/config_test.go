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
package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRead verifies that Read() registers expected default values and types for
// all Open Match configuration keys, applies environment variable overrides,
// and resets non-positive values for positive-required integer keys back to
// their defaults.
func TestRead(t *testing.T) {
	// Subtest 1: Verify every configuration key is registered with its expected
	// default value and underlying type.
	t.Run("default values and types", func(t *testing.T) {
		cfg := Read()
		require.NotNil(t, cfg)

		expectedStrings := map[string]string{
			"OM_LOGGING_FORMAT":       "json",
			"OM_LOGGING_LEVEL":        "info",
			"OM_STATE_STORAGE_TYPE":   "redis",
			"REDISHOST":               "127.0.0.1",
			"OM_REDIS_WRITE_HOST":     "REDISHOST",
			"OM_REDIS_WRITE_PORT":     "REDISPORT",
			"OM_REDIS_WRITE_USER":     "default",
			"OM_REDIS_WRITE_PASSWORD": "om-redis",
			"OM_REDIS_READ_HOST":      "REDISHOST",
			"OM_REDIS_READ_PORT":      "REDISPORT",
			"OM_REDIS_READ_USER":      "default",
			"OM_REDIS_READ_PASSWORD":  "om-redis",
			"K_SERVICE":               "open_match_core",
			"K_REVISION":              "open_match_core_rev.1",
			"K_CONFIGURATION":         "open_match_core_cfg",
		}
		for key, want := range expectedStrings {
			assert.True(t, cfg.IsSet(key), "expected string key %s to be registered", key)
			assert.IsType(t, "", cfg.Get(key), "expected raw default for %s to be string", key)
			assert.Equal(t, want, cfg.GetString(key), "unexpected default for %s", key)
		}

		expectedInts := map[string]int{
			"OM_PROM_PORT":                                 2223,
			"OM_MAX_STATE_UPDATES_PER_CALL":                500,
			"OM_MMF_TIMEOUT_SECS":                          600,
			"OM_REDIS_POOL_MAX_IDLE":                       500,
			"OM_REDIS_POOL_MAX_ACTIVE":                     500,
			"REDISPORT":                                    6379,
			"OM_CACHE_IN_MAX_UPDATES_PER_POLL":             10000,
			"OM_CACHE_IN_WAIT_TIMEOUT_MS":                  1500,
			"OM_CACHE_IN_POLL_WAIT_MS":                     250,
			"OM_CACHE_IN_FULL_POLL_WAIT_MS":                100,
			"OM_CACHE_IN_QUEUE_BUFFER_SIZE":                20000,
			"OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS": 500,
			"OM_CACHE_IN_FULL_APPLY_SLEEP_MS":              100,
			"OM_CACHE_IN_MAX_APPLY_DURATION_MS":            500,
			"OM_CACHE_OUT_MAX_QUEUE_THRESHOLD":             50,
			"OM_CACHE_OUT_WAIT_TIMEOUT_MS":                 500,
			"OM_CACHE_OUT_QUEUE_BUFFER_SIZE":               500,
			"OM_CACHE_OUT_MAX_PACKED_KEYS":                 500,
			"OM_CACHE_TICKET_TTL_MS":                       600000,
			"OM_CACHE_EXPIRATION_INTERVAL_MS":              1000,
			"OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE":    5000,
			"OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS":      60000,
			"OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS":        600000,
			"PORT":                                         8080,
			"OM_GRPC_PORT":                                 50581,
		}
		for key, want := range expectedInts {
			assert.True(t, cfg.IsSet(key), "expected int key %s to be registered", key)
			assert.IsType(t, 0, cfg.Get(key), "expected raw default for %s to be int", key)
			assert.Equal(t, want, cfg.GetInt(key), "unexpected default for %s", key)
		}

		expectedBools := map[string]bool{
			"OM_OTEL_SIDECAR":                  true,
			"OM_MATCH_TICKET_DEACTIVATION_WAIT": true,
			"OM_REDIS_USE_TLS":                 false,
			"OM_REDIS_TLS_SKIP_VERIFY":         false,
			"OM_CACHE_PACK_TICKET_STATE_UPDATES": false,
			"OM_VERBOSE":                       false,
		}
		for key, want := range expectedBools {
			assert.True(t, cfg.IsSet(key), "expected bool key %s to be registered", key)
			assert.IsType(t, false, cfg.Get(key), "expected raw default for %s to be bool", key)
			assert.Equal(t, want, cfg.GetBool(key), "unexpected default for %s", key)
		}

		expectedDurations := map[string]time.Duration{
			"OM_REDIS_DIAL_MAX_BACKOFF_TIMEOUT": 2 * time.Minute,
			"OM_REDIS_DIAL_CONNECT_TIMEOUT":     5 * time.Second,
			"OM_REDIS_DIAL_READ_TIMEOUT":        10 * time.Second,
			"OM_REDIS_DIAL_WRITE_TIMEOUT":       5 * time.Second,
			"OM_REDIS_POOL_IDLE_TIMEOUT":        time.Minute,
		}
		for key, want := range expectedDurations {
			assert.True(t, cfg.IsSet(key), "expected duration key %s to be registered", key)
			assert.IsType(t, time.Duration(0), cfg.Get(key), "expected raw default for %s to be time.Duration", key)
			assert.Equal(t, want, cfg.GetDuration(key), "unexpected default for %s", key)
		}
	})

	// Subtest 2: Verify environment variables override default values across
	// string, int, bool, and time.Duration configuration keys.
	t.Run("environment variables override defaults", func(t *testing.T) {
		t.Setenv("OM_LOGGING_FORMAT", "text")
		t.Setenv("OM_REDIS_POOL_MAX_IDLE", "100")
		t.Setenv("OM_CACHE_EXPIRATION_INTERVAL_MS", "250")
		t.Setenv("OM_CACHE_PACK_TICKET_STATE_UPDATES", "true")
		t.Setenv("OM_REDIS_POOL_IDLE_TIMEOUT", "45s")

		cfg := Read()

		assert.Equal(t, "text", cfg.GetString("OM_LOGGING_FORMAT"))
		assert.Equal(t, 100, cfg.GetInt("OM_REDIS_POOL_MAX_IDLE"))
		assert.Equal(t, 250, cfg.GetInt("OM_CACHE_EXPIRATION_INTERVAL_MS"))
		assert.True(t, cfg.GetBool("OM_CACHE_PACK_TICKET_STATE_UPDATES"))
		assert.Equal(t, 45*time.Second, cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT"))
	})

	// Subtest 3: Verify that zero or negative environment values for integer
	// keys that must be strictly positive (such as channel buffer sizes, ticker
	// intervals, and per-cycle bounds) are reset to their safe defaults.
	t.Run("non-positive integer keys reset to default", func(t *testing.T) {
		expectedDefaults := map[string]int{
			"OM_CACHE_EXPIRATION_INTERVAL_MS":           1000,
			"OM_CACHE_IN_MAX_APPLY_DURATION_MS":         500,
			"OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE": 5000,
			"OM_CACHE_IN_QUEUE_BUFFER_SIZE":             20000,
			"OM_CACHE_OUT_QUEUE_BUFFER_SIZE":            500,
			"OM_CACHE_OUT_MAX_PACKED_KEYS":              500,
			"OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS":   60000,
			"OM_CACHE_IN_MAX_UPDATES_PER_POLL":          10000,
			"OM_MAX_STATE_UPDATES_PER_CALL":             500,
		}

		for _, invalidVal := range []string{"0", "-5"} {
			for key, def := range expectedDefaults {
				t.Run(fmt.Sprintf("%s=%s", key, invalidVal), func(t *testing.T) {
					t.Setenv(key, invalidVal)
					cfg := Read()
					assert.Equal(t, def, cfg.GetInt(key), "key %s with env=%s should reset to %d", key, invalidVal, def)
				})
			}
		}
	})
}

// TestLogConfig verifies that LogConfig iterates and logs all configuration
// settings without panicking when debug logging is enabled.
func TestLogConfig(t *testing.T) {
	prevLevel := logrus.GetLevel()
	logrus.SetLevel(logrus.DebugLevel)
	defer logrus.SetLevel(prevLevel)

	cfg := Read()
	require.NotPanics(t, func() {
		LogConfig(cfg)
	})
}
