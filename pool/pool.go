// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrPoolClosed is returned when submitting tasks to a closed pool.
var ErrPoolClosed = errors.New("worker pool is closed")

// ErrQueueFull is returned when the task queue capacity is exceeded.
var ErrQueueFull = errors.New("worker pool queue is full")

// Future represents the deferred result of a task submitted to the pool.
type Future[T any] struct {
	ch  chan struct{}
	val T
	err error
}

// Get blocks until the task completes, or until the context expires.
func (f *Future[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-f.ch:
		return f.val, f.err
	}
}

type task[T any] struct {
	ctx  context.Context
	work func(context.Context) (T, error)
	fut  *Future[T]
}

// Config defines the scale and timing configuration for the Pool.
type Config struct {
	MinWorkers  int
	MaxWorkers  int
	IdleTimeout time.Duration
	QueueLimit  int
}

func (c *Config) resolveDefaults() {
	if c.MinWorkers <= 0 {
		c.MinWorkers = 1
	}

	if c.MaxWorkers < c.MinWorkers {
		c.MaxWorkers = c.MinWorkers
	}

	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 5 * time.Second
	}

	if c.QueueLimit <= 0 {
		c.QueueLimit = 100
	}
}

// Pool manages a set of worker goroutines that scale up and down dynamically.
type Pool[T any] struct {
	mu             sync.Mutex
	cfg            Config
	tasks          chan task[T]
	currentWorkers int
	busyWorkers    int
	closed         bool
	wg             sync.WaitGroup
}

// NewPool creates and starts a new worker Pool.
func NewPool[T any](cfg Config) *Pool[T] {
	cfg.resolveDefaults()

	p := &Pool[T]{
		cfg:   cfg,
		tasks: make(chan task[T], cfg.QueueLimit),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for range cfg.MinWorkers {
		p.spawnWorker()
	}

	return p
}

func (p *Pool[T]) spawnWorker() {
	p.currentWorkers++

	p.wg.Go(func() {
		timer := time.NewTimer(p.cfg.IdleTimeout)
		defer timer.Stop()

		for {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(p.cfg.IdleTimeout)

			select {
			case t, ok := <-p.tasks:
				if !ok {
					p.mu.Lock()
					p.currentWorkers--
					p.mu.Unlock()

					return
				}

				p.mu.Lock()
				p.busyWorkers++
				p.mu.Unlock()

				p.runTaskSafely(t)

				p.mu.Lock()
				p.busyWorkers--
				p.mu.Unlock()

			case <-timer.C:
				p.mu.Lock()
				if p.currentWorkers > p.cfg.MinWorkers {
					p.currentWorkers--
					p.mu.Unlock()

					return
				}

				p.mu.Unlock()
			}
		}
	})
}

func (p *Pool[T]) runTaskSafely(t task[T]) {
	defer func() {
		if r := recover(); r != nil {
			t.fut.err = fmt.Errorf("task panicked: %v", r)
			close(t.fut.ch)
		}
	}()

	t.fut.val, t.fut.err = t.work(t.ctx)
	close(t.fut.ch)
}

// Submit enqueues a task to be processed by a worker.
// It returns a Future to wait for the results, or an error if the pool is closed or queue is full.
func (p *Pool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) (*Future[T], error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return nil, ErrPoolClosed
	}

	if len(p.tasks) >= p.cfg.QueueLimit {
		p.mu.Unlock()

		return nil, ErrQueueFull
	}

	fut := &Future[T]{ch: make(chan struct{})}
	t := task[T]{
		ctx:  ctx,
		work: fn,
		fut:  fut,
	}

	p.tasks <- t

	if p.currentWorkers < p.cfg.MaxWorkers && (p.busyWorkers == p.currentWorkers || len(p.tasks) > 0) {
		p.spawnWorker()
	}

	p.mu.Unlock()

	return fut, nil
}

// Close gracefully shuts down the pool, waiting for all enqueued tasks to complete.
func (p *Pool[T]) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return nil
	}

	p.closed = true

	close(p.tasks)
	p.mu.Unlock()

	p.wg.Wait()

	return nil
}
