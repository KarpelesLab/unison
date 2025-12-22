package unison

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// PanicError wraps a panic value as an error, including the stack trace.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// call represents an in-flight or completed function call.
type call[T any] struct {
	wg   sync.WaitGroup
	val  T
	err  error
	done bool
	exp  time.Time
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group[K comparable, T any] struct {
	mu sync.RWMutex
	m  map[K]*call[T]
}

// Len returns the number of entries currently in the group.
// This includes both in-flight calls and cached results (which may be expired).
func (g *Group[K, T]) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.m)
}

// Has reports whether a key exists in the group with a valid (non-expired) entry.
// Returns true if the key has an in-flight call or a cached result that hasn't expired.
func (g *Group[K, T]) Has(key K) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	c, ok := g.m[key]
	if !ok {
		return false
	}
	// If it's done and expired, it's not valid
	if c.done && time.Now().After(c.exp) {
		return false
	}
	return true
}

// Cleanup removes all expired entries from the group.
// This is useful for reclaiming memory when using DoUntil with many unique keys.
func (g *Group[K, T]) Cleanup() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	for key, c := range g.m {
		if c.done && now.After(c.exp) {
			delete(g.m, key)
		}
	}
}

// Forget removes a key from the group, causing future calls to execute
// the function again even if a previous call is still in-flight.
// Goroutines already waiting for the result will still receive it.
func (g *Group[K, T]) Forget(key K) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a time.
// If a duplicate comes in, the duplicate caller waits for the original
// to complete and receives the same results.
func (g *Group[K, T]) Do(key K, fn func() (T, error)) (val T, err error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[T])
	}

	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(call[T])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			c.err = &PanicError{Value: r, Stack: debug.Stack()}
		}
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		c.wg.Done()
		val, err = c.val, c.err
	}()

	c.val, c.err = fn()

	return c.val, c.err
}

// DoUntil is like Do but caches the result for the specified duration.
// Subsequent calls within the cache window return the cached result without
// executing the function again.
func (g *Group[K, T]) DoUntil(key K, dur time.Duration, fn func() (T, error)) (val T, err error) {
	// Fast path: check for cached result with read lock
	g.mu.RLock()
	if g.m != nil {
		if c, ok := g.m[key]; ok {
			if c.done && !time.Now().After(c.exp) {
				// cached and still valid
				g.mu.RUnlock()
				return c.val, c.err
			}
			if !c.done {
				// in-flight, wait for it
				g.mu.RUnlock()
				c.wg.Wait()
				return c.val, c.err
			}
		}
	}
	g.mu.RUnlock()

	// Slow path: need write lock to create or replace entry
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[T])
	}

	// Double-check after acquiring write lock
	if c, ok := g.m[key]; ok {
		if c.done && time.Now().After(c.exp) {
			// cached result has expired, remove it and continue to create new call
			delete(g.m, key)
		} else if c.done {
			// cached and still valid
			g.mu.Unlock()
			return c.val, c.err
		} else {
			// in-flight, wait for it
			g.mu.Unlock()
			c.wg.Wait()
			return c.val, c.err
		}
	}

	c := new(call[T])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			c.err = &PanicError{Value: r, Stack: debug.Stack()}
		}
		g.mu.Lock()
		c.exp = time.Now().Add(dur)
		c.done = true
		g.mu.Unlock()
		c.wg.Done()
		val, err = c.val, c.err
	}()

	c.val, c.err = fn()

	return c.val, c.err
}
