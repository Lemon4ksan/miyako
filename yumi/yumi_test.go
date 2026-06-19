// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package yumi

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_OrderPreservation(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 12,
	}
	p := NewPipeline[int, int](cfg)

	inputs := make([]int, 1000)
	for i := 0; i < len(inputs); i++ {
		inputs[i] = i + 1
	}

	mapper := func(ctx context.Context, in int) (int, error) {
		// Random sleep to disrupt scheduling order
		time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
		return in * 10, nil
	}

	outputs, err := p.Process(context.Background(), inputs, mapper)
	require.NoError(t, err)
	require.Len(t, outputs, 1000)

	for i := 0; i < len(inputs); i++ {
		assert.Equal(t, inputs[i]*10, outputs[i])
	}
}

func TestPipeline_FailFast(t *testing.T) {
	cfg := PipelineConfig{
		Workers:  1,
		FailFast: true,
	}
	p := NewPipeline[int, int](cfg)

	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	errDummy := errors.New("dummy error")

	var runCount atomic.Int32

	mapper := func(ctx context.Context, in int) (int, error) {
		runCount.Add(1)

		if in == 2 {
			return 0, errDummy
		}

		time.Sleep(10 * time.Millisecond)

		return in, nil
	}

	_, err := p.Process(context.Background(), inputs, mapper)
	assert.ErrorIs(t, err, errDummy)

	// Due to FailFast with 1 worker, only 2 or 3 tasks should be processed before context cancel takes effect
	assert.LessOrEqual(t, runCount.Load(), int32(3))
}

func TestPipeline_NoFailFast(t *testing.T) {
	cfg := PipelineConfig{
		Workers:  3,
		FailFast: false,
	}
	p := NewPipeline[int, int](cfg)

	inputs := []int{1, 2, 3}
	errDummy := errors.New("dummy error")

	mapper := func(ctx context.Context, in int) (int, error) {
		if in == 2 || in == 3 {
			return 0, errDummy
		}

		return in, nil
	}

	_, err := p.Process(context.Background(), inputs, mapper)
	assert.Error(t, err)
	// Since FailFast is false, it should run all and join errors
	assert.Contains(t, err.Error(), "dummy error")
}

func TestPipeline_RateLimiting(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 2,
		RPS:     50, // 50 requests per second, so 20ms per request
		Burst:   1,
	}
	p := NewPipeline[int, int](cfg)

	inputs := []int{1, 2, 3}
	start := time.Now()

	_, err := p.Process(context.Background(), inputs, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})
	require.NoError(t, err)

	duration := time.Since(start)
	// 3 requests with limit 50/sec should take at least ~40ms
	assert.GreaterOrEqual(t, duration, 35*time.Millisecond)
}

func TestPipeline_ContextCancelled_NoLeaks(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	cfg := PipelineConfig{
		Workers: 5,
	}
	p := NewPipeline[int, int](cfg)

	inputs := make([]int, 100)
	for i := 0; i < len(inputs); i++ {
		inputs[i] = i
	}

	ctx, cancel := context.WithCancel(context.Background())

	mapper := func(ctx context.Context, in int) (int, error) {
		if in == 5 {
			cancel() // Cancel context mid-execution
		}

		time.Sleep(10 * time.Millisecond)

		return in, nil
	}

	_, err := p.Process(ctx, inputs, mapper)
	assert.ErrorIs(t, err, context.Canceled)

	// Wait for goroutines to clean up
	time.Sleep(50 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Check that we didn't leak worker goroutines
	assert.LessOrEqual(t, finalGoroutines, initialGoroutines+2)
}

func TestPipeline_Stream(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 3,
	}
	p := NewPipeline[int, int](cfg)

	inCh := make(chan int, 10)
	for i := 1; i <= 5; i++ {
		inCh <- i
	}

	close(inCh)

	outCh, errCh := p.Stream(context.Background(), inCh, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	var results []int
	for val := range outCh {
		results = append(results, val)
	}

	// We expect 5 results
	assert.Len(t, results, 5)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, results)

	for err := range errCh {
		t.Fatalf("unexpected error from stream: %v", err)
	}
}

func TestPipeline_Stream_FailFast(t *testing.T) {
	cfg := PipelineConfig{
		Workers:  3,
		FailFast: true,
	}
	p := NewPipeline[int, int](cfg)

	inCh := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		inCh <- i
	}

	close(inCh)

	errDummy := errors.New("stream error")
	outCh, errCh := p.Stream(context.Background(), inCh, func(ctx context.Context, in int) (int, error) {
		if in == 3 {
			return 0, errDummy
		}

		time.Sleep(10 * time.Millisecond)

		return in, nil
	})

	// Drain output
	for range outCh {
	}

	var encounteredErrs []error
	for err := range errCh {
		encounteredErrs = append(encounteredErrs, err)
	}

	assert.NotEmpty(t, encounteredErrs)
	assert.ErrorIs(t, encounteredErrs[0], errDummy)
}

func TestMapAndForEach(t *testing.T) {
	ctx := context.Background()
	cfg := PipelineConfig{Workers: 4}

	// Test Map
	res, err := Map(ctx, cfg, []int{1, 2, 3}, func(ctx context.Context, in int) (string, error) {
		return "val", nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"val", "val", "val"}, res)

	// Test ForEach
	var sum atomic.Int64

	err = ForEach(ctx, cfg, []int{10, 20, 30}, func(ctx context.Context, in int) error {
		sum.Add(int64(in))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(60), sum.Load())
}

func TestPipeline_EmptyAndDefaults(t *testing.T) {
	// Empty inputs
	res, err := Map[int, int](
		context.Background(),
		PipelineConfig{Workers: 3},
		nil,
		func(ctx context.Context, in int) (int, error) {
			return in, nil
		},
	)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// Resolve defaults for Workers <= 0 and Burst <= 0
	p := NewPipeline[int, int](PipelineConfig{Workers: 0, Burst: 0})
	assert.Equal(t, 1, p.config.Workers)
	assert.Equal(t, 1, p.config.Burst)

	// Already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = p.Process(ctx, []int{1}, func(c context.Context, in int) (int, error) {
		return in, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPipeline_RateLimiting_Cancelled(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 1,
		RPS:     1, // very slow to force wait block
		Burst:   1,
	}
	p := NewPipeline[int, int](cfg)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel context asynchronously so rate limiter Wait will fail
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := p.Process(ctx, []int{1, 2}, func(c context.Context, in int) (int, error) {
		return in, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPipeline_Stream_Cancelled(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 2,
	}
	p := NewPipeline[int, int](cfg)

	inCh := make(chan int, 5)
	inCh <- 1

	inCh <- 2

	ctx, cancel := context.WithCancel(context.Background())

	outCh, errCh := p.Stream(ctx, inCh, func(c context.Context, in int) (int, error) {
		if in == 1 {
			cancel() // cancel stream context mid-stream
		}

		time.Sleep(10 * time.Millisecond)

		return in, nil
	})

	// Drain output
	for range outCh {
	}

	var streamErrs []error
	for err := range errCh {
		streamErrs = append(streamErrs, err)
	}

	assert.Empty(t, streamErrs)
	assert.Contains(t, []error{nil, context.Canceled}, ctx.Err())
}

func TestPipeline_Stream_RateLimiting(t *testing.T) {
	cfg := PipelineConfig{
		Workers: 1,
		RPS:     100,
		Burst:   1,
	}
	p := NewPipeline[int, int](cfg)

	inCh := make(chan int, 2)
	inCh <- 1

	inCh <- 2

	close(inCh)

	outCh, errCh := p.Stream(context.Background(), inCh, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})

	var results []int
	for val := range outCh {
		results = append(results, val)
	}

	assert.ElementsMatch(t, []int{1, 2}, results)

	for err := range errCh {
		t.Fatalf("unexpected stream error: %v", err)
	}
}

func TestPipeline_Stream_RateLimiting_Cancelled(t *testing.T) {
	cfg := PipelineConfig{
		Workers:  1,
		RPS:      1, // slow
		Burst:    1,
		FailFast: true,
	}
	p := NewPipeline[int, int](cfg)

	inCh := make(chan int, 2)
	inCh <- 1

	inCh <- 2

	close(inCh)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	outCh, errCh := p.Stream(ctx, inCh, func(c context.Context, in int) (int, error) {
		return in, nil
	})

	for range outCh {
	}

	var streamErrs []error
	for err := range errCh {
		streamErrs = append(streamErrs, err)
	}

	assert.NotEmpty(t, streamErrs)
	assert.ErrorIs(t, streamErrs[0], context.Canceled)
}

func BenchmarkPipeline_Process(b *testing.B) {
	cfg := PipelineConfig{
		Workers: 8,
	}
	p := NewPipeline[int, int](cfg)

	inputs := make([]int, 100)
	for i := 0; i < len(inputs); i++ {
		inputs[i] = i
	}

	mapper := func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = p.Process(context.Background(), inputs, mapper)
	}
}
