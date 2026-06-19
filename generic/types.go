// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package generic

// Option represents a functional option that configures a generic target.
type Option[T any] func(T)

// ApplyOptions sequentially applies a list of functional options to the target object.
func ApplyOptions[T any](target *T, opts ...Option[*T]) {
	for _, opt := range opts {
		if opt != nil {
			opt(target)
		}
	}
}

// Ptr returns a pointer to the given value.
//
//go:fix inline
func Ptr[T any](v T) *T {
	return &v
}

// PtrOrNil returns a pointer to the given value, or nil if the value is the zero value of its type.
func PtrOrNil[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}

	return &v
}

// Zero returns the zero value of the given type.
func Zero[T any]() T {
	var zero T
	return zero
}

// IsZero returns true if the value is the zero value of its type.
func IsZero[T comparable](v T) bool {
	var zero T
	return v == zero
}

// Deref safely dereferences a pointer, returning the zero value if nil.
func Deref[T any](ptr *T) T {
	if ptr == nil {
		var zero T
		return zero
	}

	return *ptr
}

// DerefOr safely dereferences a pointer, returning the default value if nil.
func DerefOr[T any](ptr *T, def T) T {
	if ptr == nil {
		return def
	}

	return *ptr
}

// Coalesce returns the first non-zero value from the list, or the zero value if all are zero.
func Coalesce[T comparable](vals ...T) T {
	var zero T
	for _, v := range vals {
		if v != zero {
			return v
		}
	}

	return zero
}

// Ternary emulates a ternary operator for generic types.
func Ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}

	return b
}
