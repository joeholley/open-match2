// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logging

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewFormatter verifies that newFormatter returns a Cloud Logging-compatible
// JSONFormatter (with timestamp, severity, and message field mappings and
// RFC3339Nano timestamps) when requested (case-insensitively), and falls back
// to TextFormatter otherwise.
func TestNewFormatter(t *testing.T) {
	expectedJSONFormatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}

	testCases := []struct {
		in       string
		expected logrus.Formatter
	}{
		{"", &logrus.TextFormatter{}},
		{"json", expectedJSONFormatter},
		{"JSON", expectedJSONFormatter},
		{"text", &logrus.TextFormatter{}},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("newFormatter(%q) => %s", tc.in, reflect.TypeOf(tc.expected)), func(t *testing.T) {
			actual := newFormatter(tc.in)
			require.Equal(t, tc.expected, actual)
		})
	}
}

// TestConfigureLogging verifies that ConfigureLogging configures the global
// Logrus formatter and minimum log level from OM_LOGGING_FORMAT and
// OM_LOGGING_LEVEL, and defaults to InfoLevel when OM_LOGGING_LEVEL is invalid.
func TestConfigureLogging(t *testing.T) {
	origFormatter := logrus.StandardLogger().Formatter
	origLevel := logrus.GetLevel()
	origOut := logrus.StandardLogger().Out
	t.Cleanup(func() {
		logrus.SetFormatter(origFormatter)
		logrus.SetLevel(origLevel)
		logrus.SetOutput(origOut)
	})

	testCases := []struct {
		name              string
		format            string
		level             string
		wantFormatterType reflect.Type
		wantLevel         logrus.Level
	}{
		{
			name:              "json format and info level",
			format:            "json",
			level:             "info",
			wantFormatterType: reflect.TypeOf(&logrus.JSONFormatter{}),
			wantLevel:         logrus.InfoLevel,
		},
		{
			name:              "text format and debug level dumps config",
			format:            "text",
			level:             "debug",
			wantFormatterType: reflect.TypeOf(&logrus.TextFormatter{}),
			wantLevel:         logrus.DebugLevel,
		},
		{
			name:              "trace level enables trace logging",
			format:            "text",
			level:             "trace",
			wantFormatterType: reflect.TypeOf(&logrus.TextFormatter{}),
			wantLevel:         logrus.TraceLevel,
		},
		{
			name:              "invalid log level defaults to info",
			format:            "json",
			level:             "not-a-valid-level",
			wantFormatterType: reflect.TypeOf(&logrus.JSONFormatter{}),
			wantLevel:         logrus.InfoLevel,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := viper.New()
			cfg.Set("OM_LOGGING_FORMAT", tc.format)
			cfg.Set("OM_LOGGING_LEVEL", tc.level)

			ConfigureLogging(cfg)

			assert.Equal(t, tc.wantFormatterType, reflect.TypeOf(logrus.StandardLogger().Formatter))
			assert.Equal(t, tc.wantLevel, logrus.GetLevel())
		})
	}
}
