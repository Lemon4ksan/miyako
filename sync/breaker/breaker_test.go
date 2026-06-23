// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package breaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_BasicFlow(t *testing.T) {
	ch := make(chan State, 10)

	cb := New[string](Config{
		FailureThreshold: 0.5,
		Cooldown:         50 * time.Millisecond,
		MinRequests:      4,
		Window:           1 * time.Second,
		OnStateChange: func(from, to State) {
			ch <- to
		},
	})

	ctx := context.Background()

	// 1. All success, state should stay Closed
	for range 4 {
		_, err := cb.Do(ctx, func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if cb.State() != StateClosed {
		t.Errorf("expected Closed state, got %v", cb.State())
	}

	// 2. 50% failures (4 success, 4 failures).
	// Total 8 requests in window: 4 fail -> ratio 0.5 >= threshold 0.5 -> opens!
	// Let's execute 4 failures
	for range 4 {
		_, _ = cb.Do(ctx, func(ctx context.Context) (string, error) {
			return "", errors.New("fail")
		})
	}

	// Wait for callback to transition to Open
	select {
	case state := <-ch:
		if state != StateOpen {
			t.Errorf("expected callback StateOpen, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateOpen transition")
	}

	if cb.State() != StateOpen {
		t.Errorf("expected Open state, got %v", cb.State())
	}

	// 3. In Open state, requests should fail immediately
	_, err := cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "should not run", nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}

	// 4. Wait for Cooldown to transition to Half-Open
	time.Sleep(60 * time.Millisecond)

	if cb.State() != StateHalfOpen {
		t.Errorf("expected Half-Open state after cooldown, got %v", cb.State())
	}

	// Wait for callback to transition to HalfOpen
	select {
	case state := <-ch:
		if state != StateHalfOpen {
			t.Errorf("expected callback StateHalfOpen, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateHalfOpen transition")
	}

	// 5. Successful request in Half-Open transitions to Closed
	_, err = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for callback to transition to Closed
	select {
	case state := <-ch:
		if state != StateClosed {
			t.Errorf("expected callback StateClosed, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateClosed transition")
	}

	if cb.State() != StateClosed {
		t.Errorf("expected Closed state, got %v", cb.State())
	}

	// 6. Transition to Open again from Closed
	// Execute 4 failed requests to trip it again
	for range 4 {
		_, _ = cb.Do(ctx, func(ctx context.Context) (string, error) {
			return "", errors.New("fail")
		})
	}

	// Wait for callback to transition to Open
	select {
	case state := <-ch:
		if state != StateOpen {
			t.Errorf("expected callback StateOpen, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateOpen transition")
	}

	if cb.State() != StateOpen {
		t.Errorf("expected Open state, got %v", cb.State())
	}

	// 7. Wait for Cooldown, and then execute a FAILED request in Half-Open -> transitions back to Open
	time.Sleep(60 * time.Millisecond)

	if cb.State() != StateHalfOpen {
		t.Errorf("expected Half-Open state, got %v", cb.State())
	}

	// Wait for callback to transition to HalfOpen
	select {
	case state := <-ch:
		if state != StateHalfOpen {
			t.Errorf("expected callback StateHalfOpen, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateHalfOpen transition")
	}

	_, err = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "", errors.New("fail in half-open")
	})
	if err == nil {
		t.Fatal("expected failure error, got nil")
	}

	// Wait for callback to transition to Open
	select {
	case state := <-ch:
		if state != StateOpen {
			t.Errorf("expected callback StateOpen, got %v", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for StateOpen transition")
	}

	if cb.State() != StateOpen {
		t.Errorf("expected Open state after half-open failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenConcurrency(t *testing.T) {
	cb := New[string](Config{
		FailureThreshold: 0.5,
		Cooldown:         10 * time.Millisecond,
		MinRequests:      1,
		Window:           1 * time.Second,
	})

	ctx := context.Background()

	// Make it Open
	_, _ = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "", errors.New("fail")
	})

	// Wait for cooldown
	time.Sleep(15 * time.Millisecond)

	startCh := make(chan struct{})
	blockCh := make(chan struct{})
	done := make(chan struct{})

	var err1, err2 error

	go func() {
		defer close(done)
		_, err1 = cb.Do(ctx, func(ctx context.Context) (string, error) {
			close(startCh)
			<-blockCh
			return "ok", nil
		})
	}()

	<-startCh

	_, err2 = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "ok", nil
	})

	close(blockCh)
	<-done

	if err1 != nil {
		t.Errorf("expected first request to succeed, got %v", err1)
	}

	if !errors.Is(err2, ErrCircuitOpen) {
		t.Errorf("expected concurrent request in Half-Open to fail with ErrCircuitOpen, got %v", err2)
	}
}

func TestCircuitBreaker_WindowPruning(t *testing.T) {
	cb := New[string](Config{
		FailureThreshold: 0.5,
		Cooldown:         1 * time.Second,
		MinRequests:      2,
		Window:           10 * time.Millisecond,
	})

	ctx := context.Background()

	// 1. One failure
	_, _ = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "", errors.New("fail")
	})

	// 2. Wait for it to expire out of window
	time.Sleep(20 * time.Millisecond)

	// 3. Second failure. If window pruning works, the first failure is ignored,
	// so total requests = 1, which is < MinRequests (2). The state stays Closed.
	_, _ = cb.Do(ctx, func(ctx context.Context) (string, error) {
		return "", errors.New("fail")
	})

	if cb.State() != StateClosed {
		t.Errorf("expected Closed state, got %v", cb.State())
	}
}

func TestCircuitBreaker_StateString(t *testing.T) {
	if StateClosed.String() != "Closed" {
		t.Errorf("expected Closed, got %q", StateClosed.String())
	}

	if StateOpen.String() != "Open" {
		t.Errorf("expected Open, got %q", StateOpen.String())
	}

	if StateHalfOpen.String() != "Half-Open" {
		t.Errorf("expected Half-Open, got %q", StateHalfOpen.String())
	}

	if State(99).String() != "Unknown" {
		t.Errorf("expected Unknown, got %q", State(99).String())
	}
}

func TestCircuitBreaker_Defaults(t *testing.T) {
	cb := New[int](Config{})

	if cb.cfg.FailureThreshold != 0.5 {
		t.Errorf("expected 0.5, got %f", cb.cfg.FailureThreshold)
	}

	if cb.cfg.Cooldown != 5*time.Second {
		t.Errorf("expected 5s, got %v", cb.cfg.Cooldown)
	}

	if cb.cfg.MinRequests != 5 {
		t.Errorf("expected 5, got %d", cb.cfg.MinRequests)
	}

	if cb.cfg.Window != 10*time.Second {
		t.Errorf("expected 10s, got %v", cb.cfg.Window)
	}
}
