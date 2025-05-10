package subpub_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"VK1/pkg/subpub"
)

func TestBasicPublishSubscribe(t *testing.T) {
	bus := subpub.NewSubPub()

	var wg sync.WaitGroup
	got := make([]interface{}, 0)
	mu := sync.Mutex{}

	sub, err := bus.Subscribe("topic", func(msg interface{}) {
		mu.Lock()
		got = append(got, msg)
		mu.Unlock()
		wg.Done()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	n := 5
	wg.Add(n)
	for i := 0; i < n; i++ {
		bus.Publish("topic", i)
	}

	wg.Wait()
	sub.Unsubscribe()

	if len(got) != n {
		t.Fatalf("expected %d messages, got %d", n, len(got))
	}
	for i := 0; i < n; i++ {
		if got[i] != i {
			t.Errorf("expected msg %d, got %v", i, got[i])
		}
	}
}

func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	bus := subpub.NewSubPub()

	doneFast := make(chan struct{})
	doneSlow := make(chan struct{})

	_, _ = bus.Subscribe("topic", func(msg interface{}) {
		time.Sleep(100 * time.Millisecond)
		doneSlow <- struct{}{}
	})

	_, _ = bus.Subscribe("topic", func(msg interface{}) {
		doneFast <- struct{}{}
	})

	bus.Publish("topic", "test")

	select {
	case <-doneFast:
	case <-time.After(50 * time.Millisecond):
		t.Error("fast subscriber was blocked")
	}

	select {
	case <-doneSlow:
	case <-time.After(200 * time.Millisecond):
		t.Error("slow subscriber did not process message in time")
	}
}

func TestFIFOOrderPerSubscriber(t *testing.T) {
	bus := subpub.NewSubPub()

	var mu sync.Mutex
	received := []int{}
	want := []int{1, 2, 3, 4, 5}

	var wg sync.WaitGroup
	wg.Add(len(want))

	_, _ = bus.Subscribe("fifo", func(msg interface{}) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, msg.(int))
		wg.Done()
	})

	for _, v := range want {
		bus.Publish("fifo", v)
	}

	wg.Wait()

	for i := range want {
		if received[i] != want[i] {
			t.Errorf("FIFO violation: expected %d, got %d", want[i], received[i])
		}
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := subpub.NewSubPub()

	received := make(chan interface{}, 1)

	sub, _ := bus.Subscribe("off", func(msg interface{}) {
		received <- msg
	})

	bus.Publish("off", "hello")
	time.Sleep(10 * time.Millisecond)

	sub.Unsubscribe()
	bus.Publish("off", "bye")

	time.Sleep(10 * time.Millisecond)
	close(received)

	count := 0
	for range received {
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 message before unsubscribe, got %d", count)
	}
}

func TestCloseShutsDownProperly(t *testing.T) {
	bus := subpub.NewSubPub()

	done := make(chan struct{})

	_, _ = bus.Subscribe("topic", func(msg interface{}) {
		time.Sleep(100 * time.Millisecond)
		done <- struct{}{}
	})

	bus.Publish("topic", "x")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Error("subscriber did not finish before close")
	}

	err := bus.Close(ctx)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

func TestCloseTimeout(t *testing.T) {
	bus := subpub.NewSubPub()

	blocker := make(chan struct{})
	_, _ = bus.Subscribe("topic", func(msg interface{}) {
		<-blocker // blocking forever
	})

	bus.Publish("topic", "blocking")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := bus.Close(ctx)
	if err == nil {
		t.Error("expected timeout error, got nil")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}

	close(blocker)
}
