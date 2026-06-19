// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package generic

import (
	"sync"
	"time"
)

// Map applies a function to each element of a slice and returns a new slice with the results.
func Map[F, T any](slice []F, fn func(F) T) []T {
	if slice == nil {
		return nil
	}

	res := make([]T, len(slice))
	for i, v := range slice {
		res[i] = fn(v)
	}

	return res
}

// Set represents an unordered collection of unique elements.
type Set[T comparable] map[T]struct{}

// NewSet creates a new set from the given items.
func NewSet[T comparable](items ...T) Set[T] {
	s := make(Set[T], len(items))
	for _, item := range items {
		s[item] = struct{}{}
	}

	return s
}

// Add adds an item to the set.
func (s Set[T]) Add(item T) { s[item] = struct{}{} }

// Has returns true if the set contains the given item.
func (s Set[T]) Has(item T) bool {
	_, ok := s[item]
	return ok
}

// Intersect returns the intersection of two sets.
func (s Set[T]) Intersect(other Set[T]) Set[T] {
	res := make(Set[T])
	for k := range s {
		if other.Has(k) {
			res.Add(k)
		}
	}

	return res
}

// ToSlice converts the set back to a flat slice.
func (s Set[T]) ToSlice() []T {
	res := make([]T, 0, len(s))
	for k := range s {
		res = append(res, k)
	}

	return res
}

type cacheItem[V any] struct {
	value     V
	expiresAt time.Time
}

// Cache implements a thread-safe in-memory cache with TTL.
type Cache[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]cacheItem[V]
}

// NewCache creates a new cache instance.
func NewCache[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{
		data: make(map[K]cacheItem[V]),
	}
}

// Set sets a value in the cache with the given key and TTL.
func (c *Cache[K, V]) Set(key K, val V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = cacheItem[V]{
		value:     val,
		expiresAt: time.Now().Add(ttl),
	}
}

// Get retrieves a value from the cache by key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data[key]
	if !ok {
		var zero V
		return zero, false
	}

	if time.Now().After(item.expiresAt) {
		var zero V
		return zero, false
	}

	return item.value, true
}
