package unison

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	var g Group[string]
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
	var g Group[string]
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
	var g Group[string]
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
	var g Group[int]
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
	var g Group[string]
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
	var g Group[string]
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
	var g Group[int]
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
	var g Group[int]
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
	var g Group[string]
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
