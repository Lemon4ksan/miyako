// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package batto

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestGroup_Do_SuppressedExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := &Group[string, string]{}
		ctx := t.Context()

		var (
			runCount atomic.Int32
			mu       sync.Mutex
			results  []string
		)

		fn := func(workerCtx context.Context) (string, error) {
			runCount.Add(1)
			time.Sleep(100 * time.Millisecond)
			return "shared-value", nil
		}

		var wg sync.WaitGroup

		const waiters = 5

		for range waiters {
			wg.Go(func() {
				val, err := g.Do(ctx, "key-1", fn)
				if err != nil {
					t.Errorf("Do() failed: %v", err)
					return
				}

				mu.Lock()

				results = append(results, val)
				mu.Unlock()
			})
		}

		synctest.Wait()

		// Advance virtual time to complete the worker execution
		time.Sleep(150 * time.Millisecond)
		wg.Wait()

		gotCount := runCount.Load()
		if gotCount != 1 {
			t.Errorf("worker execution count = %d, want 1", gotCount)
		}

		mu.Lock()
		defer mu.Unlock()

		if len(results) != waiters {
			t.Errorf("results count = %d, want %d", len(results), waiters)
		}

		for i, res := range results {
			if res != "shared-value" {
				t.Errorf("results[%d] = %q, want %q", i, res, "shared-value")
			}
		}
	})
}

func TestGroup_Do_PartialContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := &Group[string, string]{}

		ctxA, cancelA := context.WithCancel(t.Context())
		t.Cleanup(cancelA)
		ctxB := t.Context()

		var (
			runCount atomic.Int32
			mu       sync.Mutex
			resultB  string
			errB     error
			errA     error
		)

		fn := func(workerCtx context.Context) (string, error) {
			runCount.Add(1)
			time.Sleep(100 * time.Millisecond)
			return "success", nil
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine A: Will cancel while waiting
		go func() {
			defer wg.Done()

			_, err := g.Do(ctxA, "key-2", fn)

			mu.Lock()
			errA = err
			mu.Unlock()
		}()

		// Goroutine B: Will wait and successfully complete
		go func() {
			defer wg.Done()

			r, err := g.Do(ctxB, "key-2", fn)

			mu.Lock()
			resultB = r
			errB = err
			mu.Unlock()
		}()

		// Let both goroutines enter Do and block
		synctest.Wait()

		// Cancel context for goroutine A
		cancelA()

		// Allow cancellation propagation to complete inside wait block
		synctest.Wait()

		mu.Lock()
		eA := errA
		mu.Unlock()

		if eA == nil || !errors.Is(eA, context.Canceled) {
			t.Errorf("caller A error = %v, want context.Canceled", eA)
		}

		// Check that B is still waiting on the execution (under mutex protection)
		mu.Lock()
		resB := resultB
		eB := errB
		mu.Unlock()

		if resB != "" || eB != nil {
			t.Errorf("caller B completed prematurely: val = %q, err = %v", resB, eB)
		}

		// Advance virtual time past worker execution threshold
		time.Sleep(150 * time.Millisecond)
		wg.Wait()

		gotCount := runCount.Load()
		if gotCount != 1 {
			t.Errorf("worker execution count = %d, want 1", gotCount)
		}

		mu.Lock()
		resBFinal := resultB
		eBFinal := errB
		mu.Unlock()

		if eBFinal != nil {
			t.Errorf("caller B error = %v, want nil", eBFinal)
		}

		if resBFinal != "success" {
			t.Errorf("caller B result = %q, want %q", resBFinal, "success")
		}
	})
}

func TestGroup_Do_WorkerContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := &Group[string, string]{}

		ctxA, cancelA := context.WithCancel(t.Context())
		t.Cleanup(cancelA)
		ctxB, cancelB := context.WithCancel(t.Context())
		t.Cleanup(cancelB)

		var workerCancelled atomic.Bool

		fn := func(workerCtx context.Context) (string, error) {
			select {
			case <-workerCtx.Done():
				workerCancelled.Store(true)
				return "", workerCtx.Err()
			case <-time.After(100 * time.Millisecond):
				return "completed", nil
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			_, _ = g.Do(ctxA, "key-3", fn)
		}()

		go func() {
			defer wg.Done()

			_, _ = g.Do(ctxB, "key-3", fn)
		}()

		// Let them start and block inside worker selection
		synctest.Wait()

		// Cancel all waiting contexts
		cancelA()
		cancelB()

		// Propagate cancellation down to the active worker context
		synctest.Wait()

		if !workerCancelled.Load() {
			t.Error("worker context was not cancelled after all waiters cancelled their contexts")
		}

		wg.Wait()
	})
}

func TestGroup_Do_PanicIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := &Group[string, string]{}
		ctx := t.Context()

		fn := func(workerCtx context.Context) (string, error) {
			time.Sleep(50 * time.Millisecond)
			panic("simulated-panic")
		}

		var wg sync.WaitGroup
		wg.Add(2)

		var (
			recoveredPanic any
			secondErr      error
		)

		// Goroutine 1: Initiator (will receive and propagate panic)

		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					recoveredPanic = r
				}
			}()

			_, _ = g.Do(ctx, "key-4", fn)
		}()

		// Ensure the initiator has successfully registered first
		synctest.Wait()

		// Goroutine 2: Secondary waiter (will receive ErrWorkerPanicked)
		go func() {
			defer wg.Done()

			_, secondErr = g.Do(ctx, "key-4", fn)
		}()

		// Advance virtual time past the panic point
		time.Sleep(100 * time.Millisecond)
		wg.Wait()

		if recoveredPanic != "simulated-panic" {
			t.Errorf("recovered panic = %v, want %q", recoveredPanic, "simulated-panic")
		}

		if secondErr == nil || !errors.Is(secondErr, ErrWorkerPanicked) {
			t.Errorf("second caller error = %v, want ErrWorkerPanicked", secondErr)
		}
	})
}

func TestGroup_Do_ExpiredContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := &Group[string, string]{}

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		var runCount atomic.Int32

		fn := func(workerCtx context.Context) (string, error) {
			runCount.Add(1)
			return "data", nil
		}

		_, err := g.Do(ctx, "key-5", fn)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Errorf("execution error = %v, want context.Canceled", err)
		}

		gotCount := runCount.Load()
		if gotCount != 0 {
			t.Errorf("worker execution count = %d, want 0", gotCount)
		}
	})
}

func TestGroup_Do_ExtraCoverage(t *testing.T) {
	// 1. Cover Do's c.done and c.panicked branch
	g1 := &Group[string, int]{
		m: make(map[string]*call[int]),
	}
	g1.m["panicked"] = &call[int]{
		done:     true,
		panicked: true,
	}

	_, err := g1.Do(context.Background(), "panicked", func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if !errors.Is(err, ErrWorkerPanicked) {
		t.Errorf("expected ErrWorkerPanicked, got %v", err)
	}

	// 2. Cover wait()'s case <-ctx.Done(): where c.done is true (PanicVal is nil)
	g2 := &Group[string, string]{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2() // cancel ctx to force <-ctx.Done() branch in wait()

	ch2 := make(chan Result[string], 1)
	c2 := &call[string]{
		done: true,
	}
	// Send result asynchronously so select chooses ctx.Done first
	go func() {
		time.Sleep(10 * time.Millisecond)

		ch2 <- Result[string]{Val: "success", Err: nil}
	}()

	val2, err2 := g2.wait(ctx2, "key", c2, ch2)
	if val2 != "success" || err2 != nil {
		t.Errorf("expected success, got val %q, err %v", val2, err2)
	}

	// 3. Cover wait()'s case <-ctx.Done(): where c.done is true (PanicVal is non-nil)
	g3 := &Group[string, string]{}
	ctx3, cancel3 := context.WithCancel(context.Background())
	cancel3() // cancel ctx to force <-ctx.Done() branch in wait()

	ch3 := make(chan Result[string], 1)
	c3 := &call[string]{
		done: true,
	}
	// Send result asynchronously so select chooses ctx.Done first
	go func() {
		time.Sleep(10 * time.Millisecond)

		ch3 <- Result[string]{PanicVal: "some-panic"}
	}()

	defer func() {
		r := recover()
		if r != "some-panic" {
			t.Errorf("expected panic 'some-panic', got %v", r)
		}
	}()

	_, _ = g3.wait(ctx3, "key", c3, ch3)
}
