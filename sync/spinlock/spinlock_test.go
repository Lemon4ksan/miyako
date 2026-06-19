// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spinlock

import (
	"sync"
	"testing"
	"time"
)

func TestSpinLock_Basic(t *testing.T) {
	var mu SpinLock

	// Initial lock
	mu.Lock()
	// TryLock should fail when locked
	if mu.TryLock() {
		t.Fatal("TryLock should fail when already locked")
	}

	mu.Unlock()

	// TryLock should succeed when unlocked
	if !mu.TryLock() {
		t.Fatal("TryLock should succeed when unlocked")
	}

	// TryLock should fail again
	if mu.TryLock() {
		t.Fatal("TryLock should fail when already locked")
	}

	mu.Unlock()
}

func TestSpinLock_Concurrent(t *testing.T) {
	var (
		mu SpinLock
		wg sync.WaitGroup
	)

	counter := 0
	numGoroutines := 100
	numIterations := 1000

	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()

			for range numIterations {
				mu.Lock()
				counter++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	expected := numGoroutines * numIterations
	if counter != expected {
		t.Errorf("expected counter to be %d, got %d", expected, counter)
	}
}

func TestSpinLock_TryLockConcurrent(t *testing.T) {
	var (
		mu SpinLock
		wg sync.WaitGroup
	)

	counter := 0
	numGoroutines := 10
	numIterations := 100

	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()

			for j := 0; j < numIterations; {
				if mu.TryLock() {
					counter++
					mu.Unlock()

					j++
				} else {
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	wg.Wait()

	expected := numGoroutines * numIterations
	if counter != expected {
		t.Errorf("expected counter to be %d, got %d", expected, counter)
	}
}
