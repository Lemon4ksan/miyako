// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package yumi

import (
	"context"
)

// PipelineConfig defines the operation parameters for the pipeline.
type PipelineConfig struct {
	Workers  int     // Maximum number of parallel goroutines
	RPS      float64 // Rate limit in requests per second (0 means no limit)
	Burst    int     // Rate limiter bucket size (defaults to 1 if not specified)
	FailFast bool    // Immediately cancel context of other workers on first error
}

func (c *PipelineConfig) resolveDefaults() {
	if c.Workers <= 0 {
		c.Workers = 1
	}

	if c.Burst <= 0 {
		c.Burst = 1
	}
}

// task holds metadata and processing state of an execution item.
type task[In, Out any] struct {
	idx int
	val In
	res Out
	err error
}

// Map is a high-level helper to transform a slice of inputs using a pipeline.
func Map[In, Out any](
	ctx context.Context,
	cfg PipelineConfig,
	inputs []In,
	mapper func(context.Context, In) (Out, error),
) ([]Out, error) {
	p := NewPipeline[In, Out](cfg)
	return p.Process(ctx, inputs, mapper)
}

// ForEach is a high-level helper to process a slice of inputs for side-effects in parallel.
func ForEach[In any](
	ctx context.Context,
	cfg PipelineConfig,
	inputs []In,
	fn func(context.Context, In) error,
) error {
	p := NewPipeline[In, struct{}](cfg)
	_, err := p.Process(ctx, inputs, func(c context.Context, in In) (struct{}, error) {
		return struct{}{}, fn(c, in)
	})

	return err
}
