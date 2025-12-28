package unison

import (
	"runtime"
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
	mu         sync.Mutex
	fn         func([]T) error
	current    *batchCall[T]
	pending    *batchCall[T]
	overflow   *batchCall[T] // second buffer when pending is full
	spareSlice []T           // pooled slice for reuse
	MaxSize    int           // Maximum batch size, 0 means unlimited
}

// NewBatch creates a new Batch with the given processing function.
// The function will be called with batches of values as they are collected.
func NewBatch[T any](fn func([]T) error) *Batch[T] {
	return &Batch[T]{fn: fn}
}

// acquireSlice gets a slice from the pool or returns nil.
// Must be called with b.mu held.
func (b *Batch[T]) acquireSlice() []T {
	if b.spareSlice != nil {
		s := b.spareSlice[:0]
		b.spareSlice = nil
		return s
	}
	return nil
}

// releaseSlice returns a slice to the pool for reuse.
// Must be called with b.mu held.
func (b *Batch[T]) releaseSlice(s []T) {
	// Only keep if spare is empty and capacity is reasonable
	if b.spareSlice == nil && cap(s) > 0 && cap(s) <= 1024 {
		// Clear slice elements to allow GC of referenced values
		clear(s[:cap(s)])
		b.spareSlice = s[:0]
	}
}

// Do adds a value to be processed and waits for the batch to complete.
// All callers in the same batch receive the same error result.
// If MaxSize is set and the pending batch is full, the caller blocks until
// there is room.
func (b *Batch[T]) Do(value T) error {
	b.mu.Lock()

	for {
		// Current is running, try to add to pending or overflow
		if b.current != nil {
			// Try pending first
			p := b.pending
			if p == nil || b.MaxSize == 0 || len(p.values) < b.MaxSize {
				if p == nil {
					p = &batchCall[T]{values: b.acquireSlice()}
					p.wg.Add(1)
					b.pending = p
				}
				p.values = append(p.values, value)
				myBatch := p
				c := b.current
				b.mu.Unlock()

				// Wait for current to complete
				c.wg.Wait()

				// Try to become the runner for pending
				b.mu.Lock()
				if b.pending == myBatch && b.current == nil {
					b.pending = b.overflow
					b.overflow = nil
					b.current = myBatch
					b.mu.Unlock()
					b.run(myBatch)
					return myBatch.err
				}
				b.mu.Unlock()

				// Someone else is running it
				myBatch.wg.Wait()
				return myBatch.err
			}

			// Pending full, try overflow
			o := b.overflow
			if o == nil || b.MaxSize == 0 || len(o.values) < b.MaxSize {
				if o == nil {
					o = &batchCall[T]{values: b.acquireSlice()}
					o.wg.Add(1)
					b.overflow = o
				}
				o.values = append(o.values, value)
				myBatch := o
				c := b.current
				p := b.pending
				b.mu.Unlock()

				// Wait for current to complete
				c.wg.Wait()
				// Wait for pending to complete (our overflow becomes pending after that)
				p.wg.Wait()

				// Try to become the runner for our batch (now pending)
				b.mu.Lock()
				if b.pending == myBatch && b.current == nil {
					b.pending = b.overflow
					b.overflow = nil
					b.current = myBatch
					b.mu.Unlock()
					b.run(myBatch)
					return myBatch.err
				}
				b.mu.Unlock()

				// Someone else is running it
				myBatch.wg.Wait()
				return myBatch.err
			}

			// Both full, wait for current to complete then retry
			c := b.current
			b.mu.Unlock()
			c.wg.Wait()
			runtime.Gosched() // Let others wake up too
			b.mu.Lock()
			continue
		}

		// No current execution
		// First check if there's a pending batch to promote
		if b.pending != nil {
			c := b.pending
			// Try to add our value to this batch
			if b.MaxSize == 0 || len(c.values) < b.MaxSize {
				c.values = append(c.values, value)
				b.pending = b.overflow
				b.overflow = nil
				b.current = c
				b.mu.Unlock()
				b.run(c)
				return c.err
			}

			// Batch is full, create new pending for our value
			np := &batchCall[T]{values: b.acquireSlice()}
			np.values = append(np.values, value)
			np.wg.Add(1)

			// Promote pending to current
			b.pending = b.overflow
			b.overflow = nil
			b.current = c

			// Place our new batch in the queue
			if b.pending == nil {
				b.pending = np
			} else {
				b.overflow = np
			}

			myBatch := np
			b.mu.Unlock()
			b.run(c)
			myBatch.wg.Wait()
			return myBatch.err
		}

		// No pending, start fresh with our value
		c := &batchCall[T]{values: b.acquireSlice()}
		c.values = append(c.values, value)
		c.wg.Add(1)
		b.current = c
		b.mu.Unlock()
		b.run(c)
		return c.err
	}
}

// run executes a single batch.
func (b *Batch[T]) run(c *batchCall[T]) {
	defer func() {
		if r := recover(); r != nil {
			c.err = &PanicError{Value: r, Stack: debug.Stack()}
		}

		b.mu.Lock()
		b.current = nil
		b.releaseSlice(c.values)
		b.mu.Unlock()

		c.wg.Done()
	}()

	c.err = b.fn(c.values)
}
