// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewAdaptiveLimiter(t *testing.T) {
	l := NewAdaptiveLimiter(10.0)
	if l.limit != 10.0 {
		t.Errorf("expected limit to be 10.0, got %f", l.limit)
	}

	if l.minLimit != 1.0 {
		t.Errorf("expected minLimit to be 1.0, got %f", l.minLimit)
	}

	if l.maxLimit != 1000.0 {
		t.Errorf("expected maxLimit to be 1000.0, got %f", l.maxLimit)
	}

	if l.alpha != 2.0 {
		t.Errorf("expected alpha to be 2.0, got %f", l.alpha)
	}

	if l.beta != 5.0 {
		t.Errorf("expected beta to be 5.0, got %f", l.beta)
	}

	if l.lastReset.IsZero() {
		t.Error("expected lastReset to be initialized")
	}
}

func TestAdaptiveLimiter_AcquireRelease_Basic(t *testing.T) {
	l := NewAdaptiveLimiter(2.0)

	// Acquire 1
	err := l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if l.active != 1 {
		t.Errorf("expected active to be 1, got %d", l.active)
	}

	// Acquire 2
	err = l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if l.active != 2 {
		t.Errorf("expected active to be 2, got %d", l.active)
	}

	// Acquire 3 should block. Let's run with context timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = l.Acquire(ctx)
	if err == nil {
		t.Fatal("expected acquire to fail due to context deadline, but succeeded")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}

	// Release 1 with some RTT
	l.Release(10 * time.Millisecond)

	if l.active != 1 {
		t.Errorf("expected active to be 1, got %d", l.active)
	}

	// Acquire again should now succeed
	err = l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if l.active != 2 {
		t.Errorf("expected active to be 2, got %d", l.active)
	}
}

func TestAdaptiveLimiter_Acquire_CancelWait(t *testing.T) {
	l := NewAdaptiveLimiter(1.0)

	// Acquire first slot
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	var acquireErr error
	go func() {
		defer wg.Done()

		acquireErr = l.Acquire(ctx)
	}()

	// Wait a bit to ensure it is blocked
	time.Sleep(10 * time.Millisecond)

	l.mu.Lock()
	if len(l.waitChs) != 1 {
		t.Errorf("expected 1 waiter, got %d", len(l.waitChs))
	}

	l.mu.Unlock()

	cancel()
	wg.Wait()

	if !errors.Is(acquireErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", acquireErr)
	}

	l.mu.Lock()
	if len(l.waitChs) != 0 {
		t.Errorf("expected 0 waiters after cancel, got %d", len(l.waitChs))
	}

	l.mu.Unlock()
}

func TestAdaptiveLimiter_Acquire_CancelMultipleWaiters(t *testing.T) {
	l := NewAdaptiveLimiter(1.0)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	ctx3, cancel3 := context.WithCancel(context.Background())

	defer cancel1()
	defer cancel2()
	defer cancel3()

	var wg sync.WaitGroup
	wg.Add(3)

	var err1, err2, err3 error
	go func() {
		defer wg.Done()

		err1 = l.Acquire(ctx1)
	}()

	for {
		l.mu.Lock()
		n := len(l.waitChs)
		l.mu.Unlock()

		if n == 1 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	go func() {
		defer wg.Done()

		err2 = l.Acquire(ctx2)
	}()

	for {
		l.mu.Lock()
		n := len(l.waitChs)
		l.mu.Unlock()

		if n == 2 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	go func() {
		defer wg.Done()

		err3 = l.Acquire(ctx3)
	}()

	for {
		l.mu.Lock()
		n := len(l.waitChs)
		l.mu.Unlock()

		if n == 3 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	l.mu.Lock()
	ch2 := l.waitChs[1] // the channel for the second waiter
	l.mu.Unlock()

	// Cancel the second waiter
	cancel2()

	// Wait a bit for the second waiter to remove itself
	time.Sleep(20 * time.Millisecond)

	l.mu.Lock()
	if len(l.waitChs) != 2 {
		t.Errorf("expected 2 waiters, got %d", len(l.waitChs))
	}

	// Check that ch2 is not in waitChs
	for _, w := range l.waitChs {
		if w == ch2 {
			t.Error("expected second waiter channel to be removed from waitChs")
		}
	}

	l.mu.Unlock()

	// Clean up by cancelling 1 and 3
	cancel1()
	cancel3()
	wg.Wait()

	if !errors.Is(err2, context.Canceled) {
		t.Errorf("expected second waiter to be cancelled, got %v", err2)
	}

	if !errors.Is(err1, context.Canceled) {
		t.Errorf("expected first waiter to be cancelled, got %v", err1)
	}

	if !errors.Is(err3, context.Canceled) {
		t.Errorf("expected third waiter to be cancelled, got %v", err3)
	}
}

func TestAdaptiveLimiter_Release_Reset(t *testing.T) {
	l := NewAdaptiveLimiter(2.0)
	l.minRTT = 5 * time.Millisecond
	l.lastReset = time.Now().Add(-31 * time.Second)

	l.Release(10 * time.Millisecond)

	if l.minRTT != 10*time.Millisecond {
		t.Errorf("expected minRTT to be reset and updated to 10ms, got %v", l.minRTT)
	}
}

func TestAdaptiveLimiter_Release_MinRTT(t *testing.T) {
	l := NewAdaptiveLimiter(2.0)

	// Case 1: minRTT is 0
	l.Release(10 * time.Millisecond)

	if l.minRTT != 10*time.Millisecond {
		t.Errorf("expected minRTT to be 10ms, got %v", l.minRTT)
	}

	// Case 2: rtt < minRTT
	l.Release(5 * time.Millisecond)

	if l.minRTT != 5*time.Millisecond {
		t.Errorf("expected minRTT to be 5ms, got %v", l.minRTT)
	}

	// Case 3: rtt >= minRTT
	l.Release(8 * time.Millisecond)

	if l.minRTT != 5*time.Millisecond {
		t.Errorf("expected minRTT to stay 5ms, got %v", l.minRTT)
	}
}

func TestAdaptiveLimiter_Release_SmoothedRTT(t *testing.T) {
	l := NewAdaptiveLimiter(2.0)

	// Case 1: smoothedRTT is 0
	l.Release(10 * time.Millisecond)

	if l.smoothedRTT != 10*time.Millisecond {
		t.Errorf("expected smoothedRTT to be 10ms, got %v", l.smoothedRTT)
	}

	// Case 2: smoothedRTT is not 0
	l.Release(20 * time.Millisecond)
	// expected = 0.9 * 10ms + 0.1 * 20ms = 9ms + 2ms = 11ms
	if l.smoothedRTT != 11*time.Millisecond {
		t.Errorf("expected smoothedRTT to be 11ms, got %v", l.smoothedRTT)
	}
}

func TestAdaptiveLimiter_Release_LimitDec(t *testing.T) {
	// Decrease limit when queue > beta (5.0)
	// queue = limit * (1.0 - minRTT / smoothedRTT)
	// if limit = 10.0, minRTT = 5ms, smoothedRTT = 20ms,
	// queue = 10.0 * (1.0 - 5/20) = 10.0 * 0.75 = 7.5 > 5.0 (beta)
	// so limit should decrease.
	l := NewAdaptiveLimiter(10.0)
	l.minRTT = 5 * time.Millisecond
	l.smoothedRTT = 20 * time.Millisecond

	l.Release(20 * time.Millisecond) // smoothedRTT becomes 0.9*20 + 0.1*20 = 20ms, minRTT = 5ms, queue = 7.5

	if l.limit != 9.0 {
		t.Errorf("expected limit to decrease to 9.0, got %f", l.limit)
	}

	// Decrease down to minLimit (1.0)
	l.beta = 0.1
	l.minLimit = 1.0
	l.limit = 1.5
	l.minRTT = 1 * time.Millisecond
	l.smoothedRTT = 10 * time.Millisecond
	l.Release(10 * time.Millisecond)

	if l.limit != 1.0 {
		t.Errorf("expected limit to hit minLimit 1.0, got %f", l.limit)
	}
}

func TestAdaptiveLimiter_Release_LimitInc(t *testing.T) {
	// Increase limit when queue < alpha (2.0)
	// queue = limit * (1.0 - minRTT / smoothedRTT)
	// if limit = 10.0, minRTT = 10ms, smoothedRTT = 11ms
	// queue = 10.0 * (1 - 10/11) = 10.0 * 1/11 = 0.909 < 2.0 (alpha)
	// so limit should increase to 11.0.
	l := NewAdaptiveLimiter(10.0)
	l.minRTT = 10 * time.Millisecond
	l.smoothedRTT = 11 * time.Millisecond

	l.Release(11 * time.Millisecond)

	if l.limit != 11.0 {
		t.Errorf("expected limit to increase to 11.0, got %f", l.limit)
	}

	// Increase up to maxLimit (1000.0)
	l.limit = 1000.0
	l.maxLimit = 1000.0
	l.minRTT = 999 * time.Millisecond
	l.smoothedRTT = 1000 * time.Millisecond
	l.Release(1000 * time.Millisecond)

	if l.limit != 1000.0 {
		t.Errorf("expected limit to stay at maxLimit 1000.0, got %f", l.limit)
	}
}

func TestAdaptiveLimiter_Release_LimitStay(t *testing.T) {
	// Limit stays same when alpha <= queue <= beta
	// limit = 10.0, alpha = 2.0, beta = 5.0
	// We want queue to be around 3.0.
	// queue = 10.0 * (1 - minRTT/smoothedRTT) = 3.0
	// 1 - minRTT/smoothedRTT = 0.3 => minRTT/smoothedRTT = 0.7
	// Let minRTT = 7ms, smoothedRTT = 10ms.
	l := NewAdaptiveLimiter(10.0)
	l.minRTT = 7 * time.Millisecond
	l.smoothedRTT = 10 * time.Millisecond

	l.Release(10 * time.Millisecond)

	if l.limit != 10.0 {
		t.Errorf("expected limit to stay 10.0, got %f", l.limit)
	}
}

func TestAdaptiveLimiter_Release_NotifyMultiple(t *testing.T) {
	l := NewAdaptiveLimiter(3.0)
	l.active = 3

	// Add two waiters
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	l.waitChs = append(l.waitChs, ch1, ch2)

	// We want limit to increase from 3.0 to 4.0 so we have slots = 4 - 2 = 2.
	// We need queue < alpha (2.0)
	// queue = 3.0 * (1 - minRTT/smoothedRTT)
	// Let minRTT = 9ms, smoothedRTT = 10ms.
	l.minRTT = 9 * time.Millisecond
	l.smoothedRTT = 10 * time.Millisecond

	l.Release(10 * time.Millisecond) // decrements active to 2. limit increases to 4. slots = 4 - 2 = 2.
	// Both ch1 and ch2 should be closed.
	select {
	case <-ch1:
	default:
		t.Error("expected ch1 to be closed")
	}

	select {
	case <-ch2:
	default:
		t.Error("expected ch2 to be closed")
	}

	if l.active != 4 {
		t.Errorf("expected active to be 4 after waking two waiters, got %d", l.active)
	}
}

func TestAdaptiveLimiter_Limit(t *testing.T) {
	l := NewAdaptiveLimiter(5.0)
	if l.Limit() != 5.0 {
		t.Errorf("expected Limit() to be 5.0, got %f", l.Limit())
	}
}

func TestAdaptiveLimiter_Acquire_BlockAndRelease(t *testing.T) {
	l := NewAdaptiveLimiter(1.0)

	// Acquire the first slot
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var blockedErr error
	go func() {
		defer wg.Done()

		blockedErr = l.Acquire(context.Background())
	}()

	// Wait to ensure the goroutine is blocked
	for {
		l.mu.Lock()
		n := len(l.waitChs)
		l.mu.Unlock()

		if n == 1 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	// Release the slot, which should wake up the blocked goroutine
	l.Release(10 * time.Millisecond)

	wg.Wait()

	if blockedErr != nil {
		t.Errorf("expected blocked acquire to succeed, got %v", blockedErr)
	}

	if l.active != 1 {
		t.Errorf("expected active to be 1, got %d", l.active)
	}
}
