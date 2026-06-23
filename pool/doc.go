// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pool provides a dynamic, auto-scaling worker pool.
//
// The worker pool manages worker goroutines dynamically.
// It scales up workers under load (up to a configured maximum)
// and scales them down (to a configured minimum) after a period of idleness,
// preventing resources from leaking when the system is quiet.
package pool
