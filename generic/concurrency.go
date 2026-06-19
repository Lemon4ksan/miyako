// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package generic

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// ParallelMap applies fn to each element of the slice concurrently, limiting active goroutines.
func ParallelMap[F, T any](ctx context.Context, slice []F, limit int, fn func(context.Context, F) T) []T {
	if len(slice) == 0 {
		return nil
	}

	if limit <= 0 {
		limit = 1
	}

	res := make([]T, len(slice))
	sem := make(chan struct{}, limit)

	var wg sync.WaitGroup

	for i, v := range slice {
		select {
		case <-ctx.Done():
			return nil
		case sem <- struct{}{}:
		}

		wg.Add(1)

		go func(idx int, val F) {
			defer wg.Done()
			defer func() { <-sem }()

			res[idx] = fn(ctx, val)
		}(i, v)
	}

	wg.Wait()

	return res
}

// ParallelForEach executes the side-effect function fn on the slice elements in parallel,
// limiting the number of goroutines. Returns the first error encountered, if any.
func ParallelForEach[T any](ctx context.Context, slice []T, limit int, fn func(context.Context, T) error) error {
	if len(slice) == 0 {
		return nil
	}

	if limit <= 0 {
		limit = 1
	}

	sem := make(chan struct{}, limit)
	errs := make(chan error, len(slice))

	var wg sync.WaitGroup

	for _, v := range slice {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)

		go func(val T) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := fn(ctx, val); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}(v)
	}

	wg.Wait()
	close(errs)

	return <-errs
}

// Future represents an asynchronous deferred evaluation (a Go-style Promise).
type Future[T any] struct {
	val T
	err error
	ch  chan struct{}
}

// NewFuture immediately starts executing fn in a background goroutine.
func NewFuture[T any](fn func() (T, error)) *Future[T] {
	f := &Future[T]{ch: make(chan struct{})}
	go func() {
		f.val, f.err = fn()
		close(f.ch)
	}()

	return f
}

// Get blocks the calling goroutine until the computation completes,
// or until the context expires.
func (f *Future[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-f.ch:
		return f.val, f.err
	}
}

type sfCall[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// SingleFlight prevents duplicate concurrent requests to the same key (De-duplicator).
// This is a lightweight, generic version without reflection.
type SingleFlight[T any] struct {
	mu sync.Mutex
	m  map[string]*sfCall[T]
}

// NewSingleFlight creates a new SingleFlight instance.
func NewSingleFlight[T any]() *SingleFlight[T] {
	return &SingleFlight[T]{m: make(map[string]*sfCall[T])}
}

// Do executes the function fn for the given key, returning the result and any error.
func (g *SingleFlight[T]) Do(key string, fn func() (T, error)) (T, error) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(sfCall[T])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}

// RetryConfig defines the parameters for generic execution retries.
type RetryConfig struct {
	Attempts int
	Delay    time.Duration
}

// Retry executes the function fn up to config.Attempts times if it returns an error.
func Retry(ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) error) error {
	if cfg.Attempts <= 0 {
		cfg.Attempts = 1
	}

	var err error
	for i := 0; i < cfg.Attempts; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = fn(ctx)
		if err == nil {
			return nil
		}

		if i+1 < cfg.Attempts && cfg.Delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.Delay):
			}
		}
	}

	return err
}

// Backoff implements a thread-safe exponential backoff with jitter (AWS algorithm).
type Backoff struct {
	mu       sync.Mutex
	min      time.Duration
	max      time.Duration
	factor   float64
	jitter   float64
	attempts int
}

// NewBackoff creates a new instance of Backoff with the given parameters.
func NewBackoff(min, max time.Duration, factor, jitter float64) *Backoff {
	if factor <= 0 {
		factor = 2
	}

	return &Backoff{
		min:    min,
		max:    max,
		factor: factor,
		jitter: jitter,
	}
}

// Next returns the delay for the next attempt,
// incrementing the internal attempt counter. Safe for concurrent calls.
func (b *Backoff) Next() time.Duration {
	b.mu.Lock()
	attempts := b.attempts
	b.attempts++
	b.mu.Unlock()

	ms := float64(b.min.Milliseconds()) * math.Pow(b.factor, float64(attempts))
	if b.jitter > 0 {
		deviation := math.Floor(rand.Float64() * b.jitter * ms) //nolint:gosec
		if rand.IntN(2) == 0 {                                  //nolint:gosec
			ms -= deviation
		} else {
			ms += deviation
		}
	}

	if ms > float64(b.max.Milliseconds()) {
		ms = float64(b.max.Milliseconds())
	}

	if ms < 0 {
		ms = 0
	}

	return time.Duration(ms) * time.Millisecond
}

// Reset resets the attempt counter of the backoff.
func (b *Backoff) Reset() {
	b.mu.Lock()
	b.attempts = 0
	b.mu.Unlock()
}

type loaderResult[V any] struct {
	val V
	err error
}

// DataLoader groups individual requests by key K into a single batch call,
// returning results of type V. It is fully thread-safe.
type DataLoader[K comparable, V any] struct {
	mu      sync.Mutex
	delay   time.Duration
	batchFn func(context.Context, []K) (map[K]V, error)
	pending map[K][]chan loaderResult[V]
	timer   *time.Timer
}

// NewDataLoader creates a new DataLoader with the given delay and batch function.
func NewDataLoader[K comparable, V any](
	delay time.Duration,
	batchFn func(context.Context, []K) (map[K]V, error),
) *DataLoader[K, V] {
	return &DataLoader[K, V]{
		delay:   delay,
		batchFn: batchFn,
		pending: make(map[K][]chan loaderResult[V]),
	}
}

// Load loads the value for the given key K. If other Load calls are received within the delay window,
// they are batched into a single batch call to batchFn.
func (l *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	ch := make(chan loaderResult[V], 1)

	l.mu.Lock()
	l.pending[key] = append(l.pending[key], ch)

	if l.timer == nil {
		l.timer = time.AfterFunc(l.delay, func() {
			l.executeBatch()
		})
	}

	l.mu.Unlock()

	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case res := <-ch:
		return res.val, res.err
	}
}

func (l *DataLoader[K, V]) executeBatch() {
	l.mu.Lock()
	pending := l.pending
	l.pending = make(map[K][]chan loaderResult[V])
	l.timer = nil
	l.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	keys := make([]K, 0, len(pending))
	for k := range pending {
		keys = append(keys, k)
	}

	results, err := l.batchFn(context.Background(), keys)

	for _, k := range keys {
		chans := pending[k]

		var (
			val     V
			itemErr error
		)

		if err != nil {
			itemErr = err
		} else if v, ok := results[k]; ok {
			val = v
		} else {
			itemErr = errors.New("aoni dataloader: key not found in batch results")
		}

		for _, ch := range chans {
			ch <- loaderResult[V]{val: val, err: itemErr}
		}
	}
}
