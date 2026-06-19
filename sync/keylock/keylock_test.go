// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package keylock

import (
	"sync"
	"testing"
	"time"
)

func TestKeyMutex_Basic(t *testing.T) {
	km := New[string]()

	// Lock key "A"
	km.Lock("A")
	if !km.IsLocked("A") {
		t.Fatal("expected A to be locked")
	}

	if km.IsLocked("B") {
		t.Fatal("expected B to not be locked")
	}

	keys := km.Keys()
	if len(keys) != 1 || keys[0] != "A" {
		t.Fatalf("expected keys [\"A\"], got %v", keys)
	}

	km.Unlock("A")

	if km.IsLocked("A") {
		t.Fatal("expected A to be unlocked")
	}
}

func TestKeyMutex_TryLock(t *testing.T) {
	km := New[string]()

	if !km.TryLock("A") {
		t.Fatal("expected TryLock to succeed on unlocked key")
	}

	if km.TryLock("A") {
		t.Fatal("expected TryLock to fail on already locked key")
	}

	km.Unlock("A")

	if !km.TryLock("A") {
		t.Fatal("expected TryLock to succeed after unlock")
	}

	km.Unlock("A")
}

func TestKeyMutex_ForceUnlock(t *testing.T) {
	km := New[string]()

	// ForceUnlock of non-existent key should not panic or fail
	km.ForceUnlock("nonexistent")

	km.Lock("A")
	km.ForceUnlock("A")

	if km.IsLocked("A") {
		t.Fatal("expected A to be unlocked after ForceUnlock")
	}

	// Try locking A again
	if !km.TryLock("A") {
		t.Fatal("expected to lock A after ForceUnlock")
	}

	km.Unlock("A")
}

func TestKeyMutex_PanicUnlockNonExistent(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when unlocking non-existent key")
		}
	}()

	km := New[string]()
	km.Unlock("A")
}

func TestKeyMutex_ZeroValue(t *testing.T) {
	// Test that we can use zero-value KeyMutex safely without panic on Lock and TryLock
	var km KeyMutex[string]

	if km.IsLocked("A") {
		t.Fatal("expected A to not be locked")
	}

	if len(km.Keys()) != 0 {
		t.Fatal("expected empty keys")
	}

	// TryLock on zero-value
	var km2 KeyMutex[string]
	if !km2.TryLock("B") {
		t.Fatal("expected TryLock on zero-value KeyMutex to succeed")
	}

	km2.Unlock("B")

	km.Lock("A")
	if !km.IsLocked("A") {
		t.Fatal("expected A to be locked")
	}

	km.Unlock("A")
}

func TestKeyMutex_ConcurrentDifferentKeys(t *testing.T) {
	km := New[int]()

	var wg sync.WaitGroup
	wg.Add(10)

	// Locks on different keys should run in parallel without blocking each other
	for i := 0; i < 10; i++ {
		go func(key int) {
			defer wg.Done()

			km.Lock(key)
			time.Sleep(10 * time.Millisecond)
			km.Unlock(key)
		}(i)
	}

	wg.Wait()
}

func TestKeyMutex_ConcurrentSameKey(t *testing.T) {
	km := New[string]()

	var wg sync.WaitGroup

	counter := 0
	numGoroutines := 20
	numIterations := 100

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < numIterations; j++ {
				km.Lock("shared-key")
				counter++
				km.Unlock("shared-key")
			}
		}()
	}

	wg.Wait()

	expected := numGoroutines * numIterations
	if counter != expected {
		t.Errorf("expected counter to be %d, got %d", expected, counter)
	}
}

func TestKeyMutex_TryLockConcurrent(t *testing.T) {
	km := New[string]()

	var wg sync.WaitGroup

	counter := 0
	numGoroutines := 10
	numIterations := 50

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < numIterations; {
				if km.TryLock("shared-try") {
					counter++
					km.Unlock("shared-try")

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
