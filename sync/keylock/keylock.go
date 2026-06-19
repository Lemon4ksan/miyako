// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package keylock

import (
	"sync"
)

type refCounter struct {
	mu    sync.Mutex
	count int
}

// KeyMutex implements a thread-safe, striped/key-based locking mechanism.
// It allows goroutine A to lock key "X" while goroutine B works concurrently with key "Y".
//
// Using an internal reference counter, KeyMutex automatically removes unused mutexes
// from memory to prevent memory leaks.
type KeyMutex[K comparable] struct {
	mu    sync.Mutex
	locks map[K]*refCounter
}

// New creates and returns a new [KeyMutex] instance for keys of type K.
func New[K comparable]() *KeyMutex[K] {
	return &KeyMutex[K]{
		locks: make(map[K]*refCounter),
	}
}

// Keys returns a slice of all currently locked keys.
func (km *KeyMutex[K]) Keys() []K {
	km.mu.Lock()
	defer km.mu.Unlock()

	keys := make([]K, 0, len(km.locks))
	for k := range km.locks {
		keys = append(keys, k)
	}

	return keys
}

// IsLocked returns true if the key is currently locked by any goroutine.
func (km *KeyMutex[K]) IsLocked(key K) bool {
	km.mu.Lock()
	defer km.mu.Unlock()

	_, exists := km.locks[key]

	return exists
}

// ForceUnlock forcibly unlocks the specified key, even if it is not currently locked.
func (km *KeyMutex[K]) ForceUnlock(key K) {
	km.mu.Lock()

	ref, exists := km.locks[key]
	if !exists {
		km.mu.Unlock()
		return
	}

	delete(km.locks, key)
	km.mu.Unlock()

	defer func() {
		_ = recover()
	}()

	ref.mu.Unlock()
}

// Lock locks the specified key.
// If the key is already locked by another goroutine, the calling goroutine
// blocks until the key is unlocked.
func (km *KeyMutex[K]) Lock(key K) {
	km.mu.Lock()
	if km.locks == nil {
		km.locks = make(map[K]*refCounter)
	}

	ref, exists := km.locks[key]
	if !exists {
		ref = &refCounter{}
		km.locks[key] = ref
	}

	ref.count++
	km.mu.Unlock()

	ref.mu.Lock()
}

// Unlock unlocks the specified key.
// It panics if the key is not currently locked or does not exist.
func (km *KeyMutex[K]) Unlock(key K) {
	km.mu.Lock()

	ref, exists := km.locks[key]
	if !exists {
		km.mu.Unlock()
		panic("miyabi keylock: unlock of unlocked key")
	}

	ref.count--
	if ref.count == 0 {
		delete(km.locks, key)
	}

	km.mu.Unlock()

	ref.mu.Unlock()
}

// TryLock attempts to lock the specified key without blocking the calling goroutine.
// It returns true if the lock was successfully acquired, and false otherwise.
func (km *KeyMutex[K]) TryLock(key K) bool {
	km.mu.Lock()
	if km.locks == nil {
		km.locks = make(map[K]*refCounter)
	}

	ref, exists := km.locks[key]
	if !exists {
		ref = &refCounter{}
		km.locks[key] = ref
	}

	ref.count++
	km.mu.Unlock()

	if ref.mu.TryLock() {
		return true
	}

	km.mu.Lock()

	ref.count--
	if ref.count == 0 {
		delete(km.locks, key)
	}

	km.mu.Unlock()

	return false
}
