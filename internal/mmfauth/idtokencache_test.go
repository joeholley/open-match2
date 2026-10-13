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

package mmfauth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sliceTokenSource is a thread-safe test oauth2.TokenSource that returns
// preconfigured tokens in order (clamping at the last token) or a fixed error,
// while counting the number of Token() invocations.
type sliceTokenSource struct {
	mu     sync.Mutex
	tokens []*oauth2.Token
	err    error
	idx    int
	calls  int
}

func (s *sliceTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if len(s.tokens) == 0 {
		return nil, errors.New("no tokens configured")
	}
	i := s.idx
	if i >= len(s.tokens) {
		i = len(s.tokens) - 1
	} else {
		s.idx++
	}
	return s.tokens[i], nil
}

func (s *sliceTokenSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestIDTokenCache_CachesWhileValid_Concurrent verifies that concurrent Token()
// calls for multiple audiences create at most one TokenSource per audience and
// serve the cached valid token without re-invoking the underlying TokenSource.
func TestIDTokenCache_CachesWhileValid_Concurrent(t *testing.T) {
	t.Parallel()

	var factoryCalls sync.Map // audience -> *atomic.Int32
	var sources sync.Map      // audience -> *sliceTokenSource
	cache := NewIDTokenCache(func(_ context.Context, audience string) (oauth2.TokenSource, error) {
		val, _ := factoryCalls.LoadOrStore(audience, &atomic.Int32{})
		n := val.(*atomic.Int32).Add(1)
		src := &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					AccessToken: fmt.Sprintf("tok-%s-%d", audience, n),
					Expiry:      time.Now().Add(time.Hour),
				},
			},
		}
		sources.Store(audience, src)
		return src, nil
	})

	audiences := []string{"https://mmf-fifo.a.run.app", "https://mmf-debug.a.run.app"}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		aud := audiences[i%len(audiences)]
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			tok, err := cache.Token(a)
			if assert.NoError(t, err) {
				assert.Equal(t, fmt.Sprintf("tok-%s-1", a), tok.AccessToken)
			}
		}(aud)
	}
	wg.Wait()

	for _, aud := range audiences {
		val, ok := factoryCalls.Load(aud)
		require.True(t, ok)
		assert.Equal(t, int32(1), val.(*atomic.Int32).Load(), "expected factory to be called once for %s", aud)

		srcVal, ok := sources.Load(aud)
		require.True(t, ok)
		assert.Equal(t, 1, srcVal.(*sliceTokenSource).callCount(), "expected underlying TokenSource.Token() to be called only once while token is valid for %s", aud)
	}
}

// TestIDTokenCache_ExpiredTokenRefreshesFromSource verifies that when a cached
// token expires, Token() first attempts to refresh from the existing TokenSource,
// and falls back to recreating a new TokenSource if the existing source fails.
func TestIDTokenCache_ExpiredTokenRefreshesFromSource(t *testing.T) {
	t.Parallel()

	t.Run("RefreshesFromExistingSourceWhenValid", func(t *testing.T) {
		t.Parallel()

		var factoryCalls atomic.Int32
		src := &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					// Expires within oauth2's 10s expiryDelta, so Valid() is false.
					AccessToken: "expired-tok",
					Expiry:      time.Now().Add(-time.Minute),
				},
				{
					AccessToken: "fresh-from-same-source",
					Expiry:      time.Now().Add(time.Hour),
				},
			},
		}

		cache := NewIDTokenCache(func(_ context.Context, _ string) (oauth2.TokenSource, error) {
			factoryCalls.Add(1)
			return src, nil
		})

		// First call gets "expired-tok" from mintFreshLocked.
		tok1, err := cache.Token("https://mmf.a.run.app")
		require.NoError(t, err)
		assert.Equal(t, "expired-tok", tok1.AccessToken)
		assert.Equal(t, int32(1), factoryCalls.Load())

		// Second call sees tok1.Valid() == false and pulls the next token from the existing source.
		tok2, err := cache.Token("https://mmf.a.run.app")
		require.NoError(t, err)
		assert.Equal(t, "fresh-from-same-source", tok2.AccessToken)
		assert.Equal(t, int32(1), factoryCalls.Load(), "should reuse existing TokenSource when it returns a valid token")

		// Third call reuses the valid tok2 without calling src.Token() again.
		tok3, err := cache.Token("https://mmf.a.run.app")
		require.NoError(t, err)
		assert.Equal(t, "fresh-from-same-source", tok3.AccessToken)
		assert.Equal(t, 2, src.callCount())
	})

	t.Run("RecreatesSourceWhenExistingSourceReturnsExpiredOrError", func(t *testing.T) {
		t.Parallel()

		var factoryCalls atomic.Int32
		firstSrc := &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					// Still expired on subsequent calls, forcing fallback to mintFreshLocked.
					AccessToken: "stuck-expired-tok",
					Expiry:      time.Now().Add(-time.Minute),
				},
			},
		}
		secondSrc := &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					AccessToken: "fresh-from-new-source",
					Expiry:      time.Now().Add(time.Hour),
				},
			},
		}

		cache := NewIDTokenCache(func(_ context.Context, _ string) (oauth2.TokenSource, error) {
			if factoryCalls.Add(1) == 1 {
				return firstSrc, nil
			}
			return secondSrc, nil
		})

		tok1, err := cache.Token("https://mmf.a.run.app")
		require.NoError(t, err)
		assert.Equal(t, "stuck-expired-tok", tok1.AccessToken)
		assert.Equal(t, int32(1), factoryCalls.Load())

		// Second call sees tok1 is expired, tries firstSrc.Token() which is also expired,
		// deletes the stale cache entry, and mints a fresh TokenSource (secondSrc).
		tok2, err := cache.Token("https://mmf.a.run.app")
		require.NoError(t, err)
		assert.Equal(t, "fresh-from-new-source", tok2.AccessToken)
		assert.Equal(t, int32(2), factoryCalls.Load(), "should recreate TokenSource when existing source fails to return a valid token")
		assert.Equal(t, 2, firstSrc.callCount())
		assert.Equal(t, 1, secondSrc.callCount())
	})
}

// TestIDTokenCache_RefreshOnRejection_RecreatesSourceAndDeduplicatesConcurrentRefreshes
// verifies that when multiple concurrent callers report the same rejected token
// via Refresh(audience, rejectedAccessToken), only one new TokenSource is minted
// while the remaining callers receive the newly refreshed token without redundant minting.
func TestIDTokenCache_RefreshOnRejection_RecreatesSourceAndDeduplicatesConcurrentRefreshes(t *testing.T) {
	t.Parallel()

	var factoryCalls atomic.Int32
	cache := NewIDTokenCache(func(_ context.Context, audience string) (oauth2.TokenSource, error) {
		n := factoryCalls.Add(1)
		return &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					AccessToken: fmt.Sprintf("token-v%d", n),
					Expiry:      time.Now().Add(time.Hour),
				},
			},
		}, nil
	})

	const aud = "https://mmf-fifo.a.run.app"
	initial, err := cache.Token(aud)
	require.NoError(t, err)
	require.Equal(t, "token-v1", initial.AccessToken)
	require.True(t, initial.Valid())

	// Simulate 16 concurrent goroutines all observing "token-v1" rejected by the API.
	var wg sync.WaitGroup
	results := make([]string, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			refreshed, err := cache.Refresh(aud, "token-v1")
			if assert.NoError(t, err) {
				results[idx] = refreshed.AccessToken
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int32(2), factoryCalls.Load(), "concurrent refreshes for the same rejected token must coalesce into 1 new TokenSource")
	for _, got := range results {
		assert.Equal(t, "token-v2", got)
	}

	// Subsequent Token(aud) calls return "token-v2".
	current, err := cache.Token(aud)
	require.NoError(t, err)
	assert.Equal(t, "token-v2", current.AccessToken)
}

// TestIDTokenCache_DoesNotCacheErrors verifies that transient errors during
// TokenSource creation or initial Token() minting are not cached, allowing
// subsequent calls for the same audience to retry and recover.
func TestIDTokenCache_DoesNotCacheErrors(t *testing.T) {
	t.Parallel()

	var attempt atomic.Int32
	cache := NewIDTokenCache(func(_ context.Context, _ string) (oauth2.TokenSource, error) {
		n := attempt.Add(1)
		if n == 1 {
			return nil, errors.New("metadata server unavailable")
		}
		if n == 2 {
			return &sliceTokenSource{err: errors.New("token mint failed")}, nil
		}
		return &sliceTokenSource{
			tokens: []*oauth2.Token{
				{
					AccessToken: "recovered-token",
					Expiry:      time.Now().Add(time.Hour),
				},
			},
		}, nil
	})

	_, err := cache.Token("https://mmf.a.run.app")
	require.Error(t, err)

	_, err = cache.Token("https://mmf.a.run.app")
	require.Error(t, err)

	tok, err := cache.Token("https://mmf.a.run.app")
	require.NoError(t, err)
	assert.Equal(t, "recovered-token", tok.AccessToken)
}

// TestIsAuthRejection verifies that IsAuthRejection returns true only for gRPC
// Unauthenticated and PermissionDenied status errors.
func TestIsAuthRejection(t *testing.T) {
	t.Parallel()

	assert.False(t, IsAuthRejection(nil))
	assert.False(t, IsAuthRejection(status.Error(codes.Unavailable, "connection refused")))
	assert.False(t, IsAuthRejection(status.Error(codes.Internal, "internal error")))
	assert.True(t, IsAuthRejection(status.Error(codes.Unauthenticated, "invalid token")))
	assert.True(t, IsAuthRejection(status.Error(codes.PermissionDenied, "caller does not have permission")))
}
