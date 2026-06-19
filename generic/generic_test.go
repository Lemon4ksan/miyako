// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package generic

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- collections.go ---

func TestMap(t *testing.T) {
	if Map[int, int](nil, func(x int) int { return x }) != nil {
		t.Fatal("expected nil for nil slice")
	}

	in := []int{1, 2, 3}
	out := Map(in, func(x int) int { return x * 2 })

	expected := []int{2, 4, 6}
	for i, v := range out {
		if v != expected[i] {
			t.Errorf("expected %d at index %d, got %d", expected[i], i, v)
		}
	}
}

func TestSet(t *testing.T) {
	s := NewSet(1, 2, 2, 3)
	if len(s) != 3 {
		t.Errorf("expected set size 3, got %d", len(s))
	}

	if !s.Has(1) || !s.Has(2) || !s.Has(3) || s.Has(4) {
		t.Error("unexpected set contents")
	}

	s.Add(4)

	if !s.Has(4) {
		t.Error("expected set to have 4 after Add")
	}

	other := NewSet(3, 4, 5)

	intersection := s.Intersect(other)
	if len(intersection) != 2 || !intersection.Has(3) || !intersection.Has(4) {
		t.Errorf("unexpected intersection: %v", intersection.ToSlice())
	}

	slice := s.ToSlice()
	if len(slice) != 4 {
		t.Errorf("expected slice length 4, got %d", len(slice))
	}
}

func TestCache(t *testing.T) {
	cache := NewCache[string, int]()

	// Miss
	if _, ok := cache.Get("a"); ok {
		t.Fatal("expected cache miss")
	}

	// Hit
	cache.Set("a", 1, 100*time.Millisecond)

	val, ok := cache.Get("a")
	if !ok || val != 1 {
		t.Fatalf("expected cache hit with 1, got %v, %t", val, ok)
	}

	// Expired
	cache.Set("b", 2, 1*time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected cache expiration")
	}
}

// --- concurrency.go ---

func TestParallelMap(t *testing.T) {
	// Empty slice
	if ParallelMap[int, int](context.Background(), nil, 2, func(ctx context.Context, x int) int { return x }) != nil {
		t.Fatal("expected nil for empty slice")
	}

	// Negative limit defaults to 1
	in := []int{1, 2, 3}

	out := ParallelMap(context.Background(), in, -1, func(ctx context.Context, x int) int {
		return x * 10
	})
	if len(out) != 3 || out[0] != 10 || out[1] != 20 || out[2] != 30 {
		t.Fatalf("expected ParallelMap with limit -1 to work, got %v", out)
	}

	// Normal ParallelMap
	out2 := ParallelMap(context.Background(), in, 2, func(ctx context.Context, x int) int {
		return x * 10
	})
	if len(out2) != 3 || out2[0] != 10 || out2[1] != 20 || out2[2] != 30 {
		t.Fatalf("expected ParallelMap to work, got %v", out2)
	}

	// Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out3 := ParallelMap(ctx, in, 2, func(ctx context.Context, x int) int {
		return x
	})
	if out3 != nil {
		t.Fatal("expected nil result on cancelled context")
	}
}

func TestParallelForEach(t *testing.T) {
	// Empty slice
	if err := ParallelForEach[int](
		context.Background(),
		nil,
		2,
		func(ctx context.Context, x int) error { return nil },
	); err != nil {
		t.Fatalf("unexpected error on empty: %v", err)
	}

	// Negative limit defaults to 1
	in := []int{1, 2, 3}

	var mu sync.Mutex

	sum := 0

	err := ParallelForEach(context.Background(), in, -1, func(ctx context.Context, x int) error {
		mu.Lock()
		sum += x
		mu.Unlock()

		return nil
	})
	if err != nil || sum != 6 {
		t.Fatalf("expected sum 6 and no error, got %d, %v", sum, err)
	}

	// Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = ParallelForEach(ctx, in, 2, func(ctx context.Context, x int) error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Return error
	errDummy := errors.New("dummy")

	err = ParallelForEach(context.Background(), in, 2, func(ctx context.Context, x int) error {
		if x == 2 {
			return errDummy
		}

		return nil
	})
	if !errors.Is(err, errDummy) {
		t.Fatalf("expected %v, got %v", errDummy, err)
	}
}

func TestFuture(t *testing.T) {
	expectedErr := errors.New("future err")
	f := NewFuture(func() (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 42, expectedErr
	})

	// Cancelled context before completion
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.Get(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Get successfully
	val, err := f.Get(context.Background())
	if val != 42 || !errors.Is(err, expectedErr) {
		t.Fatalf("expected (42, future err), got (%d, %v)", val, err)
	}
}

func TestSingleFlight(t *testing.T) {
	sf := NewSingleFlight[int]()

	var (
		wg    sync.WaitGroup
		calls int32
	)

	numGoroutines := 10

	wg.Add(numGoroutines)

	results := make([]int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()

			val, _ := sf.Do("key", func() (int, error) {
				time.Sleep(10 * time.Millisecond)

				results[idx] = idx // to check who executed it (or that it block-shares)

				return int(atomic.AddInt32(&calls, 1)), nil
			})
			results[idx] = val
		}(i)
	}

	wg.Wait()

	if calls != 1 {
		t.Errorf("expected function to be called exactly once, got %d", calls)
	}

	for i, r := range results {
		if r != 1 {
			t.Errorf("expected result 1 for goroutine %d, got %d", i, r)
		}
	}
}

func TestRetry(t *testing.T) {
	errDummy := errors.New("dummy")

	// Attempts <= 0 defaults to 1
	var calls int

	err := Retry(context.Background(), RetryConfig{Attempts: -1, Delay: 0}, func(ctx context.Context) error {
		calls++
		return errDummy
	})
	if calls != 1 || !errors.Is(err, errDummy) {
		t.Fatalf("expected 1 call and errDummy, got %d and %v", calls, err)
	}

	// Happy path (succeeds on 2nd attempt)
	calls = 0

	err = Retry(
		context.Background(),
		RetryConfig{Attempts: 3, Delay: 1 * time.Millisecond},
		func(ctx context.Context) error {
			calls++
			if calls == 2 {
				return nil
			}

			return errDummy
		},
	)
	if err != nil || calls != 2 {
		t.Fatalf("expected success on 2nd attempt, got error %v and calls %d", err, calls)
	}

	// Context cancellation in loop
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = Retry(ctx, RetryConfig{Attempts: 3, Delay: 0}, func(ctx context.Context) error {
		return errDummy
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Context cancellation during delay
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel2()

	err = Retry(ctx2, RetryConfig{Attempts: 3, Delay: 100 * time.Millisecond}, func(ctx context.Context) error {
		return errDummy
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestBackoff(t *testing.T) {
	// Factor <= 0 defaults to 2
	b := NewBackoff(10*time.Millisecond, 100*time.Millisecond, 0.0, 0.0)
	d1 := b.Next()

	d2 := b.Next()
	if d1 != 10*time.Millisecond || d2 != 20*time.Millisecond {
		t.Errorf("expected 10ms then 20ms, got %v, %v", d1, d2)
	}

	// With Jitter (and testing negative bounds, bounds clamp to Max)
	b2 := NewBackoff(10*time.Millisecond, 50*time.Millisecond, 2.0, 0.5)
	for i := 0; i < 20; i++ {
		d := b2.Next()
		if d > 50*time.Millisecond {
			t.Errorf("expected backoff capped to 50ms, got %v", d)
		}
	}

	// Trigger ms < 0 with high jitter
	b3 := NewBackoff(10*time.Millisecond, 50*time.Millisecond, 2.0, 5.0)
	for i := 0; i < 50; i++ {
		_ = b3.Next()
	}

	// Reset
	b2.Reset()

	dFirst := b2.Next()
	if dFirst > 15*time.Millisecond || dFirst < 5*time.Millisecond {
		t.Errorf("expected backoff reset to first range around 10ms, got %v", dFirst)
	}
}

func TestDataLoader(t *testing.T) {
	var callCount int32

	batchFn := func(ctx context.Context, keys []string) (map[string]int, error) {
		atomic.AddInt32(&callCount, 1)

		res := make(map[string]int)
		for _, k := range keys {
			if k == "error-trigger" {
				return nil, errors.New("batch error")
			}

			if k != "missing-key" {
				res[k] = len(k)
			}
		}

		return res, nil
	}

	dl := NewDataLoader[string, int](5*time.Millisecond, batchFn)

	var wg sync.WaitGroup
	wg.Add(3)

	var (
		val1, val2       int
		err1, err2, err3 error
	)

	// Normal concurrent loads should be batched

	go func() {
		defer wg.Done()

		val1, err1 = dl.Load(context.Background(), "alice")
	}()

	go func() {
		defer wg.Done()

		val2, err2 = dl.Load(context.Background(), "bob")
	}()

	// Missing key from batch results
	go func() {
		defer wg.Done()

		_, err3 = dl.Load(context.Background(), "missing-key")
	}()

	wg.Wait()

	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("expected exactly 1 batch execution, got %d", callCount)
	}

	if err1 != nil || val1 != 5 {
		t.Errorf("expected alice = 5, got val %d, err %v", val1, err1)
	}

	if err2 != nil || val2 != 3 {
		t.Errorf("expected bob = 3, got val %d, err %v", val2, err2)
	}

	if err3 == nil || err3.Error() != "aoni dataloader: key not found in batch results" {
		t.Errorf("expected key not found error, got %v", err3)
	}

	// Trigger error from batch function
	dl2 := NewDataLoader[string, int](5*time.Millisecond, batchFn)

	wg.Add(2)

	var errA, errB error
	go func() {
		defer wg.Done()

		_, errA = dl2.Load(context.Background(), "error-trigger")
	}()
	go func() {
		defer wg.Done()

		_, errB = dl2.Load(context.Background(), "other")
	}()

	wg.Wait()

	if errA == nil || errA.Error() != "batch error" || errB == nil || errB.Error() != "batch error" {
		t.Errorf("expected batch error, got errA=%v, errB=%v", errA, errB)
	}

	// Cancelled context on Load
	dl3 := NewDataLoader[string, int](100*time.Millisecond, batchFn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, errCancel := dl3.Load(ctx, "cancel-me")
	if !errors.Is(errCancel, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", errCancel)
	}

	// Test executeBatch on empty queue
	dl4 := NewDataLoader[string, int](5*time.Millisecond, batchFn)
	dl4.executeBatch() // should return immediately without panic/executing batchFn
}

// --- slices.go ---

func TestIndexBy(t *testing.T) {
	if IndexBy[string, int](nil, func(x int) string { return "" }) != nil {
		t.Fatal("expected nil for nil slice")
	}

	in := []int{1, 2, 3}

	res := IndexBy(in, func(x int) int { return x * 10 })
	if len(res) != 3 || res[10] != 1 || res[20] != 2 || res[30] != 3 {
		t.Fatalf("unexpected IndexBy result: %v", res)
	}
}

func TestUnique(t *testing.T) {
	if len(Unique[int](nil)) != 0 {
		t.Fatal("expected empty slice")
	}

	in := []int{1, 2, 2, 3, 1, 4}
	res := Unique(in)
	expected := []int{1, 2, 3, 4}

	if len(res) != 4 {
		t.Fatalf("expected length 4, got %d", len(res))
	}

	for i, v := range res {
		if v != expected[i] {
			t.Errorf("expected %d at %d, got %d", expected[i], i, v)
		}
	}
}

func TestGroupBy(t *testing.T) {
	if GroupBy[string, int](nil, func(x int) string { return "" }) != nil {
		t.Fatal("expected nil for nil slice")
	}

	in := []int{1, 2, 3, 4, 5}
	res := GroupBy(in, func(x int) string {
		if x%2 == 0 {
			return "even"
		}

		return "odd"
	})

	if len(res["even"]) != 2 || len(res["odd"]) != 3 {
		t.Fatalf("unexpected GroupBy: %v", res)
	}
}

func TestAnyAllFind(t *testing.T) {
	in := []int{1, 2, 3, 4}

	if !Any(in, func(x int) bool { return x == 3 }) {
		t.Error("expected Any to return true")
	}

	if Any(in, func(x int) bool { return x == 5 }) {
		t.Error("expected Any to return false")
	}

	if !All(in, func(x int) bool { return x > 0 }) {
		t.Error("expected All to return true")
	}

	if All(in, func(x int) bool { return x < 4 }) {
		t.Error("expected All to return false")
	}

	val, found := Find(in, func(x int) bool { return x%3 == 0 })
	if !found || val != 3 {
		t.Errorf("expected (3, true), got (%d, %t)", val, found)
	}

	val2, found2 := Find(in, func(x int) bool { return x == 10 })
	if found2 || val2 != 0 {
		t.Errorf("expected (0, false), got (%d, %t)", val2, found2)
	}
}

func TestFilterInPlace(t *testing.T) {
	in := []int{1, 2, 3, 4, 5}
	res := FilterInPlace(in, func(x int) bool {
		return x%2 == 0
	})

	if len(res) != 2 || res[0] != 2 || res[1] != 4 {
		t.Fatalf("unexpected FilterInPlace output: %v", res)
	}

	// Verify clear() zeroed out the tail of the backing array
	backing := in[:5]
	if backing[2] != 0 || backing[3] != 0 || backing[4] != 0 {
		t.Errorf("expected backing array tail to be cleared, got %v", backing)
	}
}

// --- types.go ---

func TestApplyOptions(t *testing.T) {
	type config struct {
		name string
		val  int
	}

	var cfg config

	optName := func(c *config) { c.name = "opt" }
	optVal := func(c *config) { c.val = 42 }

	ApplyOptions(&cfg, optName, nil, optVal)

	if cfg.name != "opt" || cfg.val != 42 {
		t.Errorf("expected options to be applied, got %v", cfg)
	}
}

func TestPtr(t *testing.T) {
	v := 42

	p := Ptr(v)
	if p == nil || *p != 42 {
		t.Fatalf("expected pointer to 42, got %v", p)
	}
}

func TestPtrOrNil(t *testing.T) {
	p1 := PtrOrNil(0)
	if p1 != nil {
		t.Fatal("expected nil pointer for zero value")
	}

	p2 := PtrOrNil(42)
	if p2 == nil || *p2 != 42 {
		t.Fatalf("expected pointer to 42, got %v", p2)
	}
}

func TestZeroIsZero(t *testing.T) {
	z := Zero[int]()
	if z != 0 {
		t.Errorf("expected 0, got %d", z)
	}

	if !IsZero(0) {
		t.Error("expected 0 to be zero")
	}

	if IsZero(42) {
		t.Error("expected 42 to not be zero")
	}
}

func TestDeref(t *testing.T) {
	if Deref[int](nil) != 0 {
		t.Error("expected 0 for nil pointer")
	}

	val := 42
	if Deref(&val) != 42 {
		t.Error("expected 42")
	}
}

func TestDerefOr(t *testing.T) {
	if DerefOr[int](nil, 100) != 100 {
		t.Error("expected default value 100 for nil pointer")
	}

	val := 42
	if DerefOr(&val, 100) != 42 {
		t.Error("expected 42, not default 100")
	}
}

func TestCoalesce(t *testing.T) {
	if Coalesce[int](0, 0, 0) != 0 {
		t.Error("expected zero value")
	}

	if Coalesce(0, 42, 100) != 42 {
		t.Error("expected first non-zero value 42")
	}
}

func TestTernary(t *testing.T) {
	if Ternary(true, "yes", "no") != "yes" {
		t.Error("expected 'yes'")
	}

	if Ternary(false, "yes", "no") != "no" {
		t.Error("expected 'no'")
	}
}
