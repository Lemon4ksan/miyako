// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lazy

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLazy_Basic(t *testing.T) {
	var calls int32

	l := New(func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	})

	// First call
	val, err := l.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	// Second call
	val, err = l.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestLazy_Error(t *testing.T) {
	expectedErr := errors.New("init error")
	l := New(func() (string, error) {
		return "", expectedErr
	})

	val, err := l.Get()
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}

	if val != "" {
		t.Fatalf("expected empty string, got %q", val)
	}
}

func TestLazy_Reset(t *testing.T) {
	var calls int32

	l := New(func() (int, error) {
		c := atomic.AddInt32(&calls, 1)
		return int(c), nil
	})

	val, err := l.Get()
	if err != nil || val != 1 {
		t.Fatalf("expected (1, nil), got (%d, %v)", val, err)
	}

	l.Reset()

	val, err = l.Get()
	if err != nil || val != 2 {
		t.Fatalf("expected (2, nil), got (%d, %v)", val, err)
	}
}

func TestLazy_Concurrent(t *testing.T) {
	var calls int32

	l := New(func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 100, nil
	})

	var wg sync.WaitGroup

	numGoroutines := 50
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			val, err := l.Get()
			if err != nil || val != 100 {
				t.Errorf("expected (100, nil), got (%d, %v)", val, err)
			}
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected init to be called exactly once, got %d", calls)
	}
}
