package unison

import (
	"runtime/debug"
	"sync"
)

// batchCall represents a group of values being collected or processed together.
type batchCall[T any] struct {
	wg     sync.WaitGroup
	values []T
	err    error
}

// Batch collects multiple calls and processes them together.
// When the first call arrives, execution starts immediately with that single value.
// Any calls that arrive while execution is in progress are collected and processed
// together in the next batch once the current execution completes.
type Batch[T any] struct {
	mu      sync.Mutex
	fn      func([]T) error
	current *batchCall[T]
	pending *batchCall[T]
}

// NewBatch creates a new Batch with the given processing function.
// The function will be called with batches of values as they are collected.
func NewBatch[T any](fn func([]T) error) *Batch[T] {
	return &Batch[T]{fn: fn}
}

// Do adds a value to be processed and waits for the batch to complete.
// All callers in the same batch receive the same error result.
func (b *Batch[T]) Do(value T) error {
	b.mu.Lock()

	if b.current != nil {
		// Add to pending batch
		p := b.pending
		if p == nil {
			p = &batchCall[T]{}
			p.wg.Add(1)
			b.pending = p
		}
		p.values = append(p.values, value)
		c := b.current
		b.mu.Unlock()

		// Wait for current batch to finish
		c.wg.Wait()

		// Try to become the runner for the pending batch
		b.mu.Lock()
		if b.pending == p {
			b.pending = nil
			b.current = p
			b.mu.Unlock()
			b.run(p)
			return p.err
		}
		b.mu.Unlock()

		// Someone else is running it, wait for completion
		p.wg.Wait()
		return p.err
	}

	// No current execution, start one immediately
	c := &batchCall[T]{values: []T{value}}
	c.wg.Add(1)
	b.current = c
	b.mu.Unlock()

	b.run(c)
	return c.err
}

// run executes a batch.
func (b *Batch[T]) run(c *batchCall[T]) {
	defer func() {
		if r := recover(); r != nil {
			c.err = &PanicError{Value: r, Stack: debug.Stack()}
		}

		b.mu.Lock()
		b.current = nil
		b.mu.Unlock()

		c.wg.Done()
	}()

	c.err = b.fn(c.values)
}
