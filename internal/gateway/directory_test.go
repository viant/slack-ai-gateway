package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cacheSource struct {
	calls   atomic.Int64
	fail    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (d *cacheSource) Channel(ctx context.Context, w, c string, _ bool) (ChannelInfo, error) {
	d.calls.Add(1)
	if d.entered != nil {
		select {
		case d.entered <- struct{}{}:
		default:
		}
	}
	if d.release != nil {
		select {
		case <-d.release:
		case <-ctx.Done():
			return ChannelInfo{}, ctx.Err()
		}
	}
	if d.fail.Load() {
		return ChannelInfo{}, ErrMetadataUnavailable
	}
	return ChannelInfo{WorkspaceID: w, ID: c, Member: true}, nil
}
func (d *cacheSource) User(_ context.Context, w, u string) (UserInfo, error) {
	d.calls.Add(1)
	return UserInfo{WorkspaceID: w, ID: u}, nil
}

func TestDirectoryTTLRefreshFailureAndCapacity(t *testing.T) {
	ctx := context.Background()
	source := &cacheSource{}
	d := NewCachedDirectory(source)
	now := time.Unix(100, 0)
	d.now = func() time.Time { return now }
	for range 2 {
		if _, err := d.Channel(ctx, "T", "C", false); err != nil {
			t.Fatal(err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatal("cache miss")
	}
	if _, err := d.User(ctx, "T", "C"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Channel(ctx, "OTHER", "C", false); err != nil {
		t.Fatal(err)
	}
	if source.calls.Load() != 3 {
		t.Fatal("cache did not isolate workspace or object kind")
	}
	now = now.Add(time.Minute)
	if _, err := d.Channel(ctx, "T", "C", false); err != nil {
		t.Fatal(err)
	}
	if source.calls.Load() != 4 {
		t.Fatal("TTL not respected")
	}
	source.fail.Store(true)
	for _, fresh := range []bool{true, false} {
		if _, err := d.Channel(ctx, "T", "C", fresh); !errors.Is(err, ErrMetadataUnavailable) {
			t.Fatal("stale positive metadata reused")
		}
	}
	if source.calls.Load() != 6 {
		t.Fatal("failed refresh was cached")
	}
	source.fail.Store(false)
	for i := range 1030 {
		now = now.Add(time.Millisecond)
		if _, err := d.Channel(ctx, "T", fmt.Sprint(i), false); err != nil {
			t.Fatal(err)
		}
		if _, err := d.User(ctx, "T", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[bool]int{}
	for k := range d.entries {
		counts[k.user]++
	}
	if counts[false] != 1024 || counts[true] != 1024 {
		t.Fatalf("unbounded cache: %v", counts)
	}
}

func TestDirectoryCoalescesAndHonorsWaiterCancellation(t *testing.T) {
	s := &cacheSource{entered: make(chan struct{}, 1), release: make(chan struct{})}
	d := NewCachedDirectory(s)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := d.Channel(context.Background(), "T", "C", true); err != nil {
			t.Error(err)
		}
	}()
	<-s.entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := d.Channel(ctx, "T", "C", true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter cancellation: %v", err)
	}
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.Channel(context.Background(), "T", "C", false); err != nil {
				t.Error(err)
			}
		}()
	}
	close(s.release)
	wg.Wait()
	if s.calls.Load() != 1 {
		t.Fatalf("calls: %d", s.calls.Load())
	}
}

type deliveryMemoryStore struct {
	Store
	part int
}

func (s *deliveryMemoryStore) ClaimDelivery(context.Context) (*Delivery, error) {
	s.part++
	return &Delivery{ID: int64(s.part), JobID: 1, Part: s.part}, nil
}
func (s *deliveryMemoryStore) GetDeliveryContext(context.Context, Delivery) (*DeliveryContext, error) {
	return &DeliveryContext{}, nil
}
func (s *deliveryMemoryStore) Delivered(context.Context, int64, string) error { return nil }

type freshPolicy struct{ checks []bool }

func (p *freshPolicy) AuthorizeDelivery(_ context.Context, _ DeliveryContext, fresh bool) error {
	p.checks = append(p.checks, fresh)
	return nil
}

type memorySender struct{}

func (memorySender) Send(context.Context, Delivery) (string, error) { return "1.001", nil }

func TestLongDeliveryRefreshesAfterOneMinute(t *testing.T) {
	p := &freshPolicy{}
	w := DeliveryWorker{Store: &deliveryMemoryStore{}, Policy: p, Sender: memorySender{}}
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.verifiedAt = time.Now().Add(-time.Minute)
	if _, err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.checks) != 3 || !p.checks[0] || p.checks[1] || !p.checks[2] {
		t.Fatalf("refresh checks %v", p.checks)
	}
}
