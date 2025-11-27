package unison

import (
	"fmt"
	"sync"
	"time"
)

// PanicError wraps a panic value as an error.
type PanicError struct {
	Value any
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
	mu sync.Mutex
	m  map[K]*call[T]
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
			c.err = &PanicError{Value: r}
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
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[T])
	}

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
			c.err = &PanicError{Value: r}
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
