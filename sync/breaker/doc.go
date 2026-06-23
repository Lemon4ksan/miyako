// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package breaker provides a generics-first, thread-safe Circuit Breaker.
//
// The circuit breaker prevents cascading failures
// in distributed systems by dynamically tracking failures of wrapped operations.
//
// # States and Transitions
//
//   - Closed: Requests are executed normally. If the failure rate exceeds the
//     configured threshold over a sliding window, the breaker opens.
//   - Open: Requests fail fast immediately, returning [ErrCircuitOpen].
//     After a cooldown period, the breaker transitions to Half-Open.
//   - Half-Open: A single trial request is permitted. If it succeeds, the breaker
//     closes. If it fails, the breaker transitions back to Open, restarting the cooldown.
package breaker
