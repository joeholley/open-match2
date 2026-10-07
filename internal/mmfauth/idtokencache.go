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
	"sync"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TokenSourceFactory creates an oauth2.TokenSource that mints Google ID tokens
// for the given audience (production: idtoken.NewTokenSource).
type TokenSourceFactory func(ctx context.Context, audience string) (oauth2.TokenSource, error)

type cachedEntry struct {
	source oauth2.TokenSource
	token  *oauth2.Token
}

// IDTokenCache caches one TokenSource and its current valid Token per audience.
// Safe for concurrent use across goroutines.
type IDTokenCache struct {
	mu        sync.Mutex
	newSource TokenSourceFactory
	entries   map[string]*cachedEntry
}

// NewIDTokenCache creates a concurrency-safe IDTokenCache backed by the given
// TokenSourceFactory.
func NewIDTokenCache(f TokenSourceFactory) *IDTokenCache {
	return &IDTokenCache{
		newSource: f,
		entries:   make(map[string]*cachedEntry),
	}
}

// Token returns a valid ID token for audience, reusing the cached token while
// token.Valid() holds and minting a new token when missing or expired.
func (c *IDTokenCache) Token(audience string) (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[audience]; ok {
		if entry.token != nil && entry.token.Valid() {
			return entry.token, nil
		}
		tok, err := entry.source.Token()
		if err == nil && tok != nil && tok.Valid() {
			entry.token = tok
			return tok, nil
		}
		// Underlying source failed or returned an invalid token; recreate below.
		delete(c.entries, audience)
	}

	return c.mintFreshLocked(audience)
}

// Refresh invalidates the cached token for audience if its AccessToken matches
// rejectedAccessToken (or if no valid token is cached) and mints a fresh token
// from a newly created TokenSource. If another goroutine has already refreshed
// the cache to a different valid token, that newer token is returned directly.
func (c *IDTokenCache) Refresh(audience string, rejectedAccessToken string) (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[audience]; ok {
		if entry.token != nil && entry.token.AccessToken != rejectedAccessToken && entry.token.Valid() {
			return entry.token, nil
		}
		delete(c.entries, audience)
	}

	return c.mintFreshLocked(audience)
}

func (c *IDTokenCache) mintFreshLocked(audience string) (*oauth2.Token, error) {
	src, err := c.newSource(context.Background(), audience)
	if err != nil {
		return nil, err
	}
	tok, err := src.Token()
	if err != nil {
		return nil, err
	}
	c.entries[audience] = &cachedEntry{
		source: src,
		token:  tok,
	}
	return tok, nil
}

// IsAuthRejection reports whether err represents an authentication or
// authorization rejection from a gRPC MMF endpoint (e.g. Cloud Run IAM / GFE
// returning Unauthenticated or PermissionDenied).
func IsAuthRejection(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	default:
		return false
	}
}
