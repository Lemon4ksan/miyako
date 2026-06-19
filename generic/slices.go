// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package generic

import (
	"slices"
)

// IndexBy returns a map where the keys are the results of applying fn to each element of slice.
func IndexBy[K comparable, V any](slice []V, fn func(V) K) map[K]V {
	if slice == nil {
		return nil
	}

	res := make(map[K]V, len(slice))
	for _, v := range slice {
		res[fn(v)] = v
	}

	return res
}

// Unique returns a new slice containing only the unique elements from the original slice.
func Unique[T comparable](slice []T) []T {
	if len(slice) == 0 {
		return slice
	}

	seen := make(map[T]struct{}, len(slice))

	res := make([]T, 0, len(slice))
	for _, v := range slice {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			res = append(res, v)
		}
	}

	return res
}

// GroupBy groups elements of a slice into a map of slices based on a key extracted by fn.
func GroupBy[K comparable, V any](slice []V, fn func(V) K) map[K][]V {
	if slice == nil {
		return nil
	}

	res := make(map[K][]V)
	for _, v := range slice {
		key := fn(v)
		res[key] = append(res[key], v)
	}

	return res
}

// Any reports whether at least one element of the slice satisfies the predicate fn.
func Any[T any](slice []T, fn func(T) bool) bool {
	return slices.ContainsFunc(slice, fn)
}

// All reports whether all elements of the slice satisfy the predicate fn.
func All[T any](slice []T, fn func(T) bool) bool {
	for _, v := range slice {
		if !fn(v) {
			return false
		}
	}

	return true
}

// Find searches for the first element in the slice that satisfies the predicate fn.
func Find[T any](slice []T, fn func(T) bool) (T, bool) {
	for _, v := range slice {
		if fn(v) {
			return v, true
		}
	}

	var zero T

	return zero, false
}

// FilterInPlace фильтрует слайс на месте БЕЗ выделения новой памяти (Zero Allocations).
// Он зачищает хвост слайса встроенной функцией clear(), позволяя сборщику мусора очистить память.
func FilterInPlace[T any](slice []T, fn func(T) bool) []T {
	n := 0
	for _, v := range slice {
		if fn(v) {
			slice[n] = v
			n++
		}
	}

	clear(slice[n:])

	return slice[:n]
}
