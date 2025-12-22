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
		received = values
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
		receivedBatches = append(receivedBatches, values)
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
		received = values
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
	var wg sync.WaitGroup

	// Use channels to control execution timing
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
	b.MaxSize = 2 // Limit pending batch to 2 values

	// First batch: single value (runs immediately)
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Do(1)
	}()

	<-batch1Started // Wait for first batch to start

	// Add 2 values to pending (should fill it)
	for i := 2; i <= 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	time.Sleep(20 * time.Millisecond) // Let goroutines queue up

	// Add 2 more values - these should block until pending clears
	for i := 4; i <= 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			b.Do(v)
		}(i)
	}

	time.Sleep(20 * time.Millisecond) // Let goroutines attempt to queue
	close(batch1Proceed)              // Let first batch complete

	<-batch2Started                   // Wait for second batch to start
	time.Sleep(20 * time.Millisecond) // Let blocked goroutines queue into new pending
	close(batch2Proceed)              // Let second batch complete

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(batchSizes) != 3 {
		t.Fatalf("number of batches = %d; want 3", len(batchSizes))
	}
	if batchSizes[0] != 1 {
		t.Errorf("first batch size = %d; want 1", batchSizes[0])
	}
	if batchSizes[1] != 2 {
		t.Errorf("second batch size = %d; want 2 (MaxSize limit)", batchSizes[1])
	}
	if batchSizes[2] != 2 {
		t.Errorf("third batch size = %d; want 2", batchSizes[2])
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
