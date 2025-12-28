package unison

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchDo(t *testing.T) {
	var received []int

	b := NewBatch(func(values []int) error {
		received = append(received, values...)
		return nil
	})

	err := b.Do(42)

	if err != nil {
		t.Errorf("Do error = %v", err)
	}
	if len(received) != 1 || received[0] != 42 {
		t.Errorf("received = %v; want [42]", received)
	}
}

func TestBatchDoErr(t *testing.T) {
	someErr := errors.New("some error")

	b := NewBatch(func(values []int) error {
		return someErr
	})

	err := b.Do(42)

	if err != someErr {
		t.Errorf("Do error = %v; want %v", err, someErr)
	}
}

func TestBatchDoCollectsValues(t *testing.T) {
	var calls atomic.Int32
	var receivedBatches [][]int
	var mu sync.Mutex
	var wg sync.WaitGroup

	b := NewBatch(func(values []int) error {
		calls.Add(1)
		mu.Lock()
		receivedBatches = append(receivedBatches, append([]int(nil), values...))
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	// First call starts immediately
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Do(1)
	}()

	// Wait a bit for first call to start
	time.Sleep(20 * time.Millisecond)

	// These calls should be batched together
	for i := 2; i <= 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedBatches) != 2 {
		t.Fatalf("number of batches = %d; want 2", len(receivedBatches))
	}

	// First batch should have single value
	if len(receivedBatches[0]) != 1 || receivedBatches[0][0] != 1 {
		t.Errorf("first batch = %v; want [1]", receivedBatches[0])
	}

	// Second batch should have remaining values (order may vary)
	if len(receivedBatches[1]) != 4 {
		t.Errorf("second batch length = %d; want 4", len(receivedBatches[1]))
	}
}

func TestBatchDoSharedError(t *testing.T) {
	someErr := errors.New("batch error")
	var wg sync.WaitGroup
	var errors []error
	var mu sync.Mutex

	b := NewBatch(func(values []int) error {
		time.Sleep(50 * time.Millisecond)
		return someErr
	})

	// Start multiple calls that will be batched
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			err := b.Do(v)
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// All callers should receive the same error
	for i, err := range errors {
		if err != someErr {
			t.Errorf("caller %d error = %v; want %v", i, err, someErr)
		}
	}
}

func TestBatchDoPanic(t *testing.T) {
	b := NewBatch(func(values []int) error {
		panic("test panic")
	})

	err := b.Do(42)

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
	if len(panicErr.Stack) == 0 {
		t.Error("expected stack trace in PanicError")
	}
}

func TestBatchDoPanicShared(t *testing.T) {
	var wg sync.WaitGroup
	var errors []error
	var mu sync.Mutex

	b := NewBatch(func(values []int) error {
		time.Sleep(50 * time.Millisecond)
		panic("batch panic")
	})

	// Start multiple calls that will be batched
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			err := b.Do(v)
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// All callers should receive the panic error
	for i, err := range errors {
		if err == nil {
			t.Errorf("caller %d: expected error from panic", i)
			continue
		}
		if _, ok := err.(*PanicError); !ok {
			t.Errorf("caller %d: expected *PanicError, got %T", i, err)
		}
	}
}

func TestBatchDoSequentialBatches(t *testing.T) {
	var calls atomic.Int32

	b := NewBatch(func(values []int) error {
		calls.Add(1)
		return nil
	})

	// Sequential calls should each trigger their own execution
	b.Do(1)
	b.Do(2)
	b.Do(3)

	if got := calls.Load(); got != 3 {
		t.Errorf("number of calls = %d; want 3", got)
	}
}

func TestBatchDoStringType(t *testing.T) {
	var received []string

	b := NewBatch(func(values []string) error {
		received = append(received, values...)
		return nil
	})

	err := b.Do("hello")

	if err != nil {
		t.Errorf("Do error = %v", err)
	}
	if len(received) != 1 || received[0] != "hello" {
		t.Errorf("received = %v; want [hello]", received)
	}
}

func TestBatchDoMaxSize(t *testing.T) {
	var batchSizes []int
	var mu sync.Mutex

	// Control each batch precisely
	batchGates := make([]chan struct{}, 4)
	for i := range batchGates {
		batchGates[i] = make(chan struct{})
	}
	batchCount := atomic.Int32{}

	b := NewBatch(func(values []int) error {
		n := int(batchCount.Add(1))
		mu.Lock()
		batchSizes = append(batchSizes, len(values))
		mu.Unlock()

		if n <= len(batchGates) {
			<-batchGates[n-1]
		}
		return nil
	})
	b.MaxSize = 2 // Limit pending batch to 2 values

	var wg sync.WaitGroup

	// Goroutine 1: starts batch 1 immediately
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Do(1)
	}()

	// Wait for batch 1 to be running
	for batchCount.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	// Goroutines 2,3: will add to pending (MaxSize=2, so both fit)
	for i := 2; i <= 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	// Wait for goroutines to enter Do() and add to pending
	time.Sleep(50 * time.Millisecond)

	// Goroutines 4,5: pending is full, these will block until batch 1 completes
	for i := 4; i <= 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	// Wait for goroutines to block
	time.Sleep(50 * time.Millisecond)

	// Release batch 1 - goroutines 2,3 will become batch 2
	close(batchGates[0])

	// Wait for batch 2 to start
	for batchCount.Load() < 2 {
		time.Sleep(time.Millisecond)
	}

	// Wait for goroutines 4,5 to add to new pending
	time.Sleep(50 * time.Millisecond)

	// Release batch 2 - goroutines 4,5 will become batch 3
	close(batchGates[1])

	// Wait for batch 3 to start
	for batchCount.Load() < 3 {
		time.Sleep(time.Millisecond)
	}

	// Release batch 3
	close(batchGates[2])

	// Release any additional batches (shouldn't happen)
	close(batchGates[3])

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Expect exactly 3 batches: [1], [2,3], [4,5]
	if len(batchSizes) != 3 {
		t.Fatalf("number of batches = %d; want 3; sizes = %v", len(batchSizes), batchSizes)
	}
	if batchSizes[0] != 1 {
		t.Errorf("batch 1 size = %d; want 1", batchSizes[0])
	}
	if batchSizes[1] != 2 {
		t.Errorf("batch 2 size = %d; want 2", batchSizes[1])
	}
	if batchSizes[2] != 2 {
		t.Errorf("batch 3 size = %d; want 2", batchSizes[2])
	}
}

func TestBatchDoMultipleBatches(t *testing.T) {
	var calls atomic.Int32
	var batchSizes []int
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Use channels to control batch execution timing
	batch1Started := make(chan struct{})
	batch1Proceed := make(chan struct{})
	batch2Started := make(chan struct{})
	batch2Proceed := make(chan struct{})
	batchCount := atomic.Int32{}

	b := NewBatch(func(values []int) error {
		n := batchCount.Add(1)
		mu.Lock()
		batchSizes = append(batchSizes, len(values))
		mu.Unlock()
		calls.Add(1)

		switch n {
		case 1:
			close(batch1Started)
			<-batch1Proceed
		case 2:
			close(batch2Started)
			<-batch2Proceed
		}
		return nil
	})

	// First batch: single value
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Do(1)
	}()

	<-batch1Started // Wait for first batch to start

	// Second batch: 3 values (will be queued while first is running)
	for i := 2; i <= 4; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	time.Sleep(20 * time.Millisecond) // Let goroutines queue up
	close(batch1Proceed)              // Let first batch complete

	<-batch2Started // Wait for second batch to start

	// Third batch: 2 values (will be queued while second is running)
	for i := 5; i <= 6; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	time.Sleep(20 * time.Millisecond) // Let goroutines queue up
	close(batch2Proceed)              // Let second batch complete

	wg.Wait()

	if got := calls.Load(); got != 3 {
		t.Errorf("number of calls = %d; want 3", got)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(batchSizes) != 3 {
		t.Fatalf("number of batches = %d; want 3", len(batchSizes))
	}
	if batchSizes[0] != 1 {
		t.Errorf("first batch size = %d; want 1", batchSizes[0])
	}
	if batchSizes[1] != 3 {
		t.Errorf("second batch size = %d; want 3", batchSizes[1])
	}
	if batchSizes[2] != 2 {
		t.Errorf("third batch size = %d; want 2", batchSizes[2])
	}
}

func TestBatchDoReusesSliceCapacity(t *testing.T) {
	// Test that the batch reuses slice capacity when possible
	var batchSizes []int
	var mu sync.Mutex

	b := NewBatch(func(values []int) error {
		mu.Lock()
		batchSizes = append(batchSizes, len(values))
		mu.Unlock()
		return nil
	})

	// Run multiple sequential batches
	for i := 0; i < 10; i++ {
		b.Do(i)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(batchSizes) != 10 {
		t.Errorf("expected 10 batches, got %d", len(batchSizes))
	}
}

func TestBatchDoConcurrentStress(t *testing.T) {
	const numGoroutines = 100
	const numIterations = 100

	var totalItems atomic.Int64
	var totalBatches atomic.Int64

	b := NewBatch(func(values []int) error {
		totalItems.Add(int64(len(values)))
		totalBatches.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				b.Do(id*numIterations + j)
			}
		}(i)
	}

	wg.Wait()

	expected := int64(numGoroutines * numIterations)
	if got := totalItems.Load(); got != expected {
		t.Errorf("total items = %d; want %d", got, expected)
	}

	batches := totalBatches.Load()
	t.Logf("Processed %d items in %d batches (avg %.1f items/batch)",
		expected, batches, float64(expected)/float64(batches))
}

func TestBatchDoRapidCalls(t *testing.T) {
	// Test rapid sequential calls to ensure no race conditions
	var count atomic.Int64

	b := NewBatch(func(values []int) error {
		count.Add(int64(len(values)))
		return nil
	})

	for i := 0; i < 1000; i++ {
		b.Do(i)
	}

	if got := count.Load(); got != 1000 {
		t.Errorf("count = %d; want 1000", got)
	}
}

func TestBatchDoEmptyRecovery(t *testing.T) {
	// Test that after a panic, the batch can continue processing
	var calls atomic.Int32

	b := NewBatch(func(values []int) error {
		n := calls.Add(1)
		if n == 1 {
			panic("first call panics")
		}
		return nil
	})

	// First call panics
	err1 := b.Do(1)
	if err1 == nil {
		t.Error("expected panic error for first call")
	}

	// Second call should work
	err2 := b.Do(2)
	if err2 != nil {
		t.Errorf("second call error = %v; want nil", err2)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("calls = %d; want 2", got)
	}
}

func TestBatchDoSlicePooling(t *testing.T) {
	// Test that slice pooling works correctly
	var sliceCaps []int
	var mu sync.Mutex

	b := NewBatch(func(values []int) error {
		mu.Lock()
		sliceCaps = append(sliceCaps, cap(values))
		mu.Unlock()
		return nil
	})

	// Run several sequential batches - the pooled slice should grow
	// but we should reuse it
	for i := 0; i < 5; i++ {
		b.Do(i)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(sliceCaps) != 5 {
		t.Fatalf("expected 5 batches, got %d", len(sliceCaps))
	}

	// All batches should use slices with capacity >= 1
	for i, c := range sliceCaps {
		if c < 1 {
			t.Errorf("batch %d: slice cap = %d; want >= 1", i, c)
		}
	}
}

// Benchmarks

func BenchmarkBatchDoSequential(b *testing.B) {
	batch := NewBatch(func(values []int) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch.Do(i)
	}
}

func BenchmarkBatchDoParallel(b *testing.B) {
	batch := NewBatch(func(values []int) error {
		// Simulate some work
		time.Sleep(time.Microsecond)
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			batch.Do(i)
			i++
		}
	})
}

func BenchmarkBatchDoParallelNoWork(b *testing.B) {
	batch := NewBatch(func(values []int) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			batch.Do(i)
			i++
		}
	})
}

func BenchmarkBatchDoWithMaxSize(b *testing.B) {
	batch := NewBatch(func(values []int) error {
		time.Sleep(time.Microsecond)
		return nil
	})
	batch.MaxSize = 10

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			batch.Do(i)
			i++
		}
	})
}

func BenchmarkBatchDoLargeValues(b *testing.B) {
	type LargeValue struct {
		ID   int
		Data [1024]byte
	}

	batch := NewBatch(func(values []LargeValue) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch.Do(LargeValue{ID: i})
	}
}

func BenchmarkBatchDoPointerValues(b *testing.B) {
	type Value struct {
		ID   int
		Data [1024]byte
	}

	batch := NewBatch(func(values []*Value) error {
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch.Do(&Value{ID: i})
	}
}
