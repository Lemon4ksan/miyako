// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package yumi

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/time/rate"
)

// Pipeline manages concurrent processing of pipelines with rate limiting and order preservation.
type Pipeline[In, Out any] struct {
	config  PipelineConfig
	limiter *rate.Limiter
	pool    sync.Pool
}

// NewPipeline creates and returns a new Pipeline instance.
func NewPipeline[In, Out any](cfg PipelineConfig) *Pipeline[In, Out] {
	cfg.resolveDefaults()

	p := &Pipeline[In, Out]{
		config: cfg,
	}
	if cfg.RPS > 0 {
		p.limiter = rate.NewLimiter(rate.Limit(cfg.RPS), cfg.Burst)
	}

	p.pool.New = func() any {
		return &task[In, Out]{}
	}

	return p
}

// Process concurrent processes slice of inputs using the mapper function.
// It preserves original order and honors rate limits and FailFast configurations.
func (p *Pipeline[In, Out]) Process(
	ctx context.Context,
	inputs []In,
	mapper func(context.Context, In) (Out, error),
) ([]Out, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasksCh := make(chan *task[In, Out], len(inputs))
	resultsCh := make(chan *task[In, Out], len(inputs))

	tasksChClosed := false
	// Recycle any unprocessed tasks in case of early exit
	defer func() {
		if !tasksChClosed {
			close(tasksCh)
		}

		for t := range tasksCh {
			t.val = *new(In)
			t.res = *new(Out)
			t.err = nil
			p.pool.Put(t)
		}
	}()

	if err := runCtx.Err(); err != nil {
		return nil, err
	}

	for i, in := range inputs {
		t := p.pool.Get().(*task[In, Out])
		t.idx = i
		t.val = in
		t.res = *new(Out)

		t.err = nil
		tasksCh <- t
	}

	close(tasksCh)

	tasksChClosed = true

	var wg sync.WaitGroup

	workers := min(p.config.Workers, len(inputs))

	for range workers {
		wg.Go(func() {
			for {
				select {
				case <-runCtx.Done():
					return
				case t, ok := <-tasksCh:
					if !ok {
						return
					}

					if err := runCtx.Err(); err != nil {
						t.err = err
						resultsCh <- t
						return
					}

					if p.limiter != nil {
						if err := p.limiter.Wait(runCtx); err != nil {
							t.err = err
							resultsCh <- t
							continue
						}
					}

					res, err := mapper(runCtx, t.val)
					t.res = res

					t.err = err
					resultsCh <- t
				}
			}
		})
	}

	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	outputs := make([]Out, len(inputs))

	var (
		collectedErrors []error
		firstErr        error
	)

	for t := range resultsCh {
		if t.err != nil {
			if p.config.FailFast {
				if firstErr == nil {
					firstErr = t.err

					cancel() // Stop all other workers
				}
			} else {
				collectedErrors = append(collectedErrors, t.err)
			}
		} else {
			outputs[t.idx] = t.res
		}

		// Recycle task
		t.val = *new(In)
		t.res = *new(Out)
		t.err = nil
		p.pool.Put(t)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if firstErr != nil {
		return nil, firstErr
	}

	if len(collectedErrors) > 0 {
		return nil, errors.Join(collectedErrors...)
	}

	return outputs, nil
}

// Stream processes items from the input channel concurrently and yields results to the output channel.
// It closes both channels upon completion or early cancellation.
func (p *Pipeline[In, Out]) Stream(
	ctx context.Context,
	in <-chan In,
	mapper func(context.Context, In) (Out, error),
) (<-chan Out, <-chan error) {
	out := make(chan Out, p.config.Workers)
	errs := make(chan error, p.config.Workers)

	go func() {
		defer close(out)
		defer close(errs)

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var (
			wg      sync.WaitGroup
			errOnce sync.Once
		)

		workers := p.config.Workers
		for range workers {
			wg.Go(func() {
				for {
					select {
					case <-runCtx.Done():
						return
					case val, ok := <-in:
						if !ok {
							return
						}

						if p.limiter != nil {
							if err := p.limiter.Wait(runCtx); err != nil {
								select {
								case errs <- err:
								default:
								}

								errOnce.Do(func() {
									if p.config.FailFast {
										cancel()
									}
								})

								return
							}
						}

						res, err := mapper(runCtx, val)
						if err != nil {
							select {
							case errs <- err:
							default:
							}

							errOnce.Do(func() {
								if p.config.FailFast {
									cancel()
								}
							})

							if p.config.FailFast {
								return
							}
						} else {
							select {
							case out <- res:
							case <-runCtx.Done():
								return
							}
						}
					}
				}
			})
		}

		wg.Wait()
	}()

	return out, errs
}
