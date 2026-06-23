// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestKeyedLimiter_Basic(t *testing.T) {
	kl := NewKeyedLimiter[string](rate.Limit(10), 2, 100*time.Millisecond)
	defer func() {
		_ = kl.Close()
	}()

	// "key1" allow check
	ok, err := kl.Allow("key-1")
	if err != nil || !ok {
		t.Errorf("expected Allow to succeed, got ok=%v, err=%v", ok, err)
	}

	ok, err = kl.Allow("key-1")
	if err != nil || !ok {
		t.Errorf("expected second Allow to succeed, got ok=%v, err=%v", ok, err)
	}

	// Exceed burst (limit is 2)
	ok, err = kl.Allow("key-1")
	if err != nil || ok {
		t.Errorf("expected Allow to fail, got ok=%v, err=%v", ok, err)
	}

	// "key-2" should be unaffected
	ok, err = kl.Allow("key-2")
	if err != nil || !ok {
		t.Errorf("expected key-2 Allow to succeed, got ok=%v, err=%v", ok, err)
	}
}

func TestKeyedLimiter_Wait(t *testing.T) {
	kl := NewKeyedLimiter[string](rate.Limit(100), 1, 100*time.Millisecond)
	defer func() {
		_ = kl.Close()
	}()

	ctx := context.Background()

	// Wait 1
	err := kl.Wait(ctx, "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait 2 with context cancellation
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel() // pre-canceled

	err = kl.Wait(ctxCancel, "key-1")
	if err == nil {
		t.Error("expected Wait to fail on canceled context, got nil")
	}
}

func TestKeyedLimiter_Sweep(t *testing.T) {
	// Use small TTL for test
	kl := NewKeyedLimiter[string](rate.Limit(100), 1, 10*time.Millisecond)
	defer func() {
		_ = kl.Close()
	}()

	_, _ = kl.Allow("key-1")

	kl.mu.Lock()
	n := len(kl.limiters)
	kl.mu.Unlock()

	if n != 1 {
		t.Errorf("expected 1 entry in map, got %d", n)
	}

	// Wait for TTL (10ms) and sweep ticker to run
	time.Sleep(30 * time.Millisecond)

	kl.mu.Lock()
	n = len(kl.limiters)
	kl.mu.Unlock()

	if n != 0 {
		t.Errorf("expected map to be swept and empty, got %d", n)
	}
}

func TestKeyedLimiter_SweepTinyTTL(t *testing.T) {
	// TTL is 1 microsecond, sweepInterval will be 500ns, which is < 1ms
	kl := NewKeyedLimiter[string](rate.Limit(100), 1, 1*time.Microsecond)
	defer func() {
		_ = kl.Close()
	}()
}

func TestKeyedLimiter_Close(t *testing.T) {
	kl := NewKeyedLimiter[string](rate.Limit(100), 1, 100*time.Millisecond)

	err := kl.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second Close should be safe
	err = kl.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Allow after close
	_, err = kl.Allow("key-1")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}

	// Wait after close
	err = kl.Wait(context.Background(), "key-1")
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}
