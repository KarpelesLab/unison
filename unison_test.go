package unison

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	var g Group[string, string]
	v, err := g.Do("key", func() (string, error) {
		return "bar", nil
	})
	if got, want := v, "bar"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

func TestDoErr(t *testing.T) {
	var g Group[string, string]
	someErr := errors.New("some error")
	v, err := g.Do("key", func() (string, error) {
		return "", someErr
	})
	if err != someErr {
		t.Errorf("Do error = %v; want %v", err, someErr)
	}
	if v != "" {
		t.Errorf("Do value = %v; want empty", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32
	var wg sync.WaitGroup

	fn := func() (string, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return "bar", nil
	}

	const n = 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			v, err := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error = %v", err)
			}
			if v != "bar" {
				t.Errorf("Do = %v; want %v", v, "bar")
			}
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestDoNewCallAfterComplete(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32

	fn := func() (int, error) {
		return int(calls.Add(1)), nil
	}

	v1, _ := g.Do("key", fn)
	v2, _ := g.Do("key", fn)

	if v1 != 1 {
		t.Errorf("first call = %d; want 1", v1)
	}
	if v2 != 2 {
		t.Errorf("second call = %d; want 2", v2)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}

func TestDoDifferentKeys(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32
	var wg sync.WaitGroup

	fn := func(key string) func() (string, error) {
		return func() (string, error) {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return key, nil
		}
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		v, _ := g.Do("key1", fn("key1"))
		if v != "key1" {
			t.Errorf("Do key1 = %v; want key1", v)
		}
	}()
	go func() {
		defer wg.Done()
		v, _ := g.Do("key2", fn("key2"))
		if v != "key2" {
			t.Errorf("Do key2 = %v; want key2", v)
		}
	}()
	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}

func TestDoUntil(t *testing.T) {
	var g Group[string, string]
	v, err := g.DoUntil("key", time.Second, func() (string, error) {
		return "bar", nil
	})
	if got, want := v, "bar"; got != want {
		t.Errorf("DoUntil = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("DoUntil error = %v", err)
	}
}

func TestDoUntilCached(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32

	fn := func() (int, error) {
		return int(calls.Add(1)), nil
	}

	v1, _ := g.DoUntil("key", 100*time.Millisecond, fn)
	v2, _ := g.DoUntil("key", 100*time.Millisecond, fn)
	v3, _ := g.DoUntil("key", 100*time.Millisecond, fn)

	if v1 != 1 || v2 != 1 || v3 != 1 {
		t.Errorf("cached values = %d, %d, %d; want 1, 1, 1", v1, v2, v3)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestDoUntilExpired(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32

	fn := func() (int, error) {
		return int(calls.Add(1)), nil
	}

	v1, _ := g.DoUntil("key", 50*time.Millisecond, fn)
	time.Sleep(60 * time.Millisecond)
	v2, _ := g.DoUntil("key", 50*time.Millisecond, fn)

	if v1 != 1 {
		t.Errorf("first call = %d; want 1", v1)
	}
	if v2 != 2 {
		t.Errorf("second call after expiry = %d; want 2", v2)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}

func TestDoUntilDupSuppress(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32
	var wg sync.WaitGroup

	fn := func() (string, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return "bar", nil
	}

	const n = 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			v, err := g.DoUntil("key", time.Second, fn)
			if err != nil {
				t.Errorf("DoUntil error = %v", err)
			}
			if v != "bar" {
				t.Errorf("DoUntil = %v; want %v", v, "bar")
			}
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestDoIntKey(t *testing.T) {
	var g Group[int, string]
	var calls atomic.Int32

	fn := func() (string, error) {
		calls.Add(1)
		return "value", nil
	}

	v1, _ := g.Do(123, fn)
	v2, _ := g.Do(456, fn)

	if v1 != "value" || v2 != "value" {
		t.Errorf("values = %v, %v; want value, value", v1, v2)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}

func TestForget(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32
	var wg sync.WaitGroup

	fn := func() (int, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return int(calls.Load()), nil
	}

	// Start first call
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Do("key", fn)
	}()

	// Wait a bit for first call to start
	time.Sleep(20 * time.Millisecond)

	// Forget the key while first call is in-flight
	g.Forget("key")

	// Start second call - should execute since key was forgotten
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Do("key", fn)
	}()

	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}

func TestForgetDoUntil(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32

	fn := func() (int, error) {
		return int(calls.Add(1)), nil
	}

	// First call caches the result
	v1, _ := g.DoUntil("key", time.Minute, fn)

	// Second call returns cached result
	v2, _ := g.DoUntil("key", time.Minute, fn)

	// Forget invalidates the cache
	g.Forget("key")

	// Third call executes again
	v3, _ := g.DoUntil("key", time.Minute, fn)

	if v1 != 1 || v2 != 1 || v3 != 2 {
		t.Errorf("values = %d, %d, %d; want 1, 1, 2", v1, v2, v3)
	}
}

func TestDoPanic(t *testing.T) {
	var g Group[string, string]
	_, err := g.Do("key", func() (string, error) {
		panic("test panic")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	panicErr, ok := err.(*PanicError)
	if !ok {
		t.Fatalf("expected *PanicError, got %T", err)
	}
	if panicErr.Value != "test panic" {
		t.Errorf("panic value = %v; want 'test panic'", panicErr.Value)
	}
}

func TestDoPanicDupSuppress(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32
	var wg sync.WaitGroup

	fn := func() (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		panic("test panic")
	}

	const n = 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := g.Do("key", fn)
			if err == nil {
				t.Error("expected error from panic")
				return
			}
			if _, ok := err.(*PanicError); !ok {
				t.Errorf("expected *PanicError, got %T", err)
			}
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestDoUntilPanic(t *testing.T) {
	var g Group[string, string]
	_, err := g.DoUntil("key", time.Second, func() (string, error) {
		panic("test panic")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	panicErr, ok := err.(*PanicError)
	if !ok {
		t.Fatalf("expected *PanicError, got %T", err)
	}
	if panicErr.Value != "test panic" {
		t.Errorf("panic value = %v; want 'test panic'", panicErr.Value)
	}
}

func TestDoUntilPanicCached(t *testing.T) {
	var g Group[string, string]
	var calls atomic.Int32

	fn := func() (string, error) {
		calls.Add(1)
		panic("test panic")
	}

	// First call panics
	_, err1 := g.DoUntil("key", 100*time.Millisecond, fn)
	// Second call should return cached panic error
	_, err2 := g.DoUntil("key", 100*time.Millisecond, fn)

	if err1 == nil || err2 == nil {
		t.Fatal("expected errors from panic")
	}
	if _, ok := err1.(*PanicError); !ok {
		t.Errorf("expected *PanicError for err1, got %T", err1)
	}
	if _, ok := err2.(*PanicError); !ok {
		t.Errorf("expected *PanicError for err2, got %T", err2)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("number of calls = %d; want 1 (panic should be cached)", got)
	}
}

func TestPanicErrorStack(t *testing.T) {
	var g Group[string, string]
	_, err := g.Do("key", func() (string, error) {
		panic("test panic")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	panicErr, ok := err.(*PanicError)
	if !ok {
		t.Fatalf("expected *PanicError, got %T", err)
	}
	if len(panicErr.Stack) == 0 {
		t.Error("expected stack trace in PanicError")
	}
	// Verify stack trace contains relevant info
	stackStr := string(panicErr.Stack)
	if len(stackStr) < 100 {
		t.Error("stack trace seems too short")
	}
}

func TestDoUntilPanicErrorStack(t *testing.T) {
	var g Group[string, string]
	_, err := g.DoUntil("key", time.Second, func() (string, error) {
		panic("test panic")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	panicErr, ok := err.(*PanicError)
	if !ok {
		t.Fatalf("expected *PanicError, got %T", err)
	}
	if len(panicErr.Stack) == 0 {
		t.Error("expected stack trace in PanicError")
	}
}

func TestLen(t *testing.T) {
	var g Group[string, int]

	if g.Len() != 0 {
		t.Errorf("Len = %d; want 0", g.Len())
	}

	g.DoUntil("key1", time.Minute, func() (int, error) { return 1, nil })
	g.DoUntil("key2", time.Minute, func() (int, error) { return 2, nil })

	if g.Len() != 2 {
		t.Errorf("Len = %d; want 2", g.Len())
	}
}

func TestHas(t *testing.T) {
	var g Group[string, int]

	// Empty group
	if g.Has("key") {
		t.Error("Has returned true for non-existent key")
	}

	// Add a cached entry
	g.DoUntil("key", 100*time.Millisecond, func() (int, error) {
		return 42, nil
	})

	if !g.Has("key") {
		t.Error("Has returned false for existing key")
	}

	// Wait for expiration
	time.Sleep(110 * time.Millisecond)

	if g.Has("key") {
		t.Error("Has returned true for expired key")
	}
}

func TestHasInFlight(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	proceed := make(chan struct{})

	go func() {
		g.Do("key", func() (int, error) {
			close(started)
			<-proceed
			return 42, nil
		})
	}()

	<-started // Wait for goroutine to start

	if !g.Has("key") {
		t.Error("Has returned false for in-flight key")
	}

	close(proceed) // Let the goroutine finish
}

func TestCleanup(t *testing.T) {
	var g Group[string, int]
	var calls atomic.Int32

	fn := func() (int, error) {
		return int(calls.Add(1)), nil
	}

	// Create some cached entries
	g.DoUntil("key1", 50*time.Millisecond, fn)
	g.DoUntil("key2", 50*time.Millisecond, fn)
	g.DoUntil("key3", 200*time.Millisecond, fn)

	if g.Len() != 3 {
		t.Errorf("Len = %d; want 3", g.Len())
	}

	// Wait for some to expire
	time.Sleep(60 * time.Millisecond)

	// Cleanup should remove expired entries
	g.Cleanup()

	if g.Len() != 1 {
		t.Errorf("Len after Cleanup = %d; want 1", g.Len())
	}

	// key3 should still be there
	if !g.Has("key3") {
		t.Error("key3 should still exist after cleanup")
	}
}

func TestCleanupEmpty(t *testing.T) {
	var g Group[string, int]

	// Should not panic on empty group
	g.Cleanup()

	if g.Len() != 0 {
		t.Errorf("Len = %d; want 0", g.Len())
	}
}
