// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package batto

import (
	"context"
	"errors"
	"sync"

	"github.com/lemon4ksan/miyako/generic"
)

// ErrWorkerPanicked is returned to secondary waiting callers when the active worker function panics.
var ErrWorkerPanicked = errors.New("worker panicked during execution")

// Result represents the execution outcome of a [CallFn] worker function.
type Result[V any] struct {
	// Val is the value returned by a successful worker execution.
	Val V
	// Err is the error returned by the worker execution.
	Err error
	// PanicVal contains the recovered panic value if the worker execution panicked.
	PanicVal any
}

type call[V any] struct {
	mu        sync.Mutex
	cancel    context.CancelFunc
	waiters   map[chan<- Result[V]]context.Context
	initiator chan<- Result[V]
	done      bool
	val       V
	err       error
	panicked  bool
}

// Group manages duplicate call suppression for parameterized keys and values.
// The zero value of Group is ready to use and safe for concurrent execution.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// CallFn represents a context-aware generic function executed by a [Group].
type CallFn[V any] func(ctx context.Context) (V, error)

// Do executes and suppresses duplicate concurrent calls for a given key.
//
// If an execution for the key is already in progress, Do blocks subsequent callers
// until the active execution completes, returning the shared result.
//
// If the context passed to Do is cancelled while waiting, the caller returns immediately
// with the context error. If all concurrent waiting contexts are cancelled, the active
// worker's context is cancelled immediately.
//
// If the worker function panics, the panic is re-raised (propagated) on the goroutine
// of the initiating caller, while secondary waiting callers receive [ErrWorkerPanicked].
//
// It returns an error if the passed context is already expired.
func (g *Group[K, V]) Do(ctx context.Context, key K, fn CallFn[V]) (V, error) {
	if err := ctx.Err(); err != nil {
		return generic.Zero[V](), err
	}

	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}

	c, ok := g.m[key]
	if !ok {
		workerCtx, cancel := context.WithCancel(context.Background()) //nolint:gosec

		ch := make(chan Result[V], 1)
		c = &call[V]{
			cancel:    cancel,
			waiters:   make(map[chan<- Result[V]]context.Context),
			initiator: ch,
		}
		c.waiters[ch] = ctx
		g.m[key] = c
		g.mu.Unlock()

		go g.run(key, c, fn, workerCtx)

		return g.wait(ctx, key, c, ch)
	}

	c.mu.Lock()
	if c.done {
		c.mu.Unlock()
		g.mu.Unlock()

		if c.panicked {
			return generic.Zero[V](), ErrWorkerPanicked
		}
	}

	ch := make(chan Result[V], 1)
	c.waiters[ch] = ctx
	c.mu.Unlock()
	g.mu.Unlock()

	return g.wait(ctx, key, c, ch)
}

func (g *Group[K, V]) wait(ctx context.Context, key K, c *call[V], ch chan Result[V]) (V, error) {
	select {
	case res := <-ch:
		if res.PanicVal != nil {
			panic(res.PanicVal)
		}

		return res.Val, res.Err

	case <-ctx.Done():
		c.mu.Lock()

		if c.done {
			c.mu.Unlock()

			res := <-ch
			if res.PanicVal != nil {
				panic(res.PanicVal)
			}

			return res.Val, res.Err
		}

		g.mu.Lock()

		delete(c.waiters, ch)

		if len(c.waiters) == 0 {
			c.cancel()
			delete(g.m, key)
		}

		c.mu.Unlock()
		g.mu.Unlock()

		return generic.Zero[V](), ctx.Err()
	}
}

func (g *Group[K, V]) run(key K, c *call[V], fn CallFn[V], workerCtx context.Context) {
	var (
		val V
		err error
	)

	defer func() {
		if r := recover(); r != nil {
			g.mu.Lock()
			c.mu.Lock()

			c.done = true
			c.panicked = true

			delete(g.m, key)

			for ch := range c.waiters {
				if ch == c.initiator {
					ch <- Result[V]{PanicVal: r}
				} else {
					ch <- Result[V]{Err: ErrWorkerPanicked}
				}
			}

			c.mu.Unlock()
			g.mu.Unlock()
		}
	}()

	val, err = fn(workerCtx)

	g.mu.Lock()
	c.mu.Lock()

	c.done = true
	c.val = val
	c.err = err

	delete(g.m, key)

	for ch := range c.waiters {
		ch <- Result[V]{Val: val, Err: err}
	}

	c.mu.Unlock()
	g.mu.Unlock()
}
