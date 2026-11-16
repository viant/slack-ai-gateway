package datly

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func ownerKey(root string) gateway.PhysicalThreadKey {
	return gateway.PhysicalThreadKey{AppID: "A", WorkspaceID: "T", ChannelID: "C", RootTS: root}
}

func TestThreadOwnersSerializeOneThreadAndRunIndependentThreads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newThreadOwners()
		defer o.close()
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 3)
		var firstDone atomic.Bool
		go func() {
			done <- o.do(context.Background(), ownerKey("one"), func(*threadOwner) error { close(entered); <-release; firstDone.Store(true); return nil })
		}()
		<-entered
		go func() {
			done <- o.do(context.Background(), ownerKey("one"), func(*threadOwner) error {
				if !firstDone.Load() {
					return errors.New("same thread overlapped")
				}
				return nil
			})
		}()
		go func() { done <- o.do(context.Background(), ownerKey("two"), func(*threadOwner) error { return nil }) }()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if firstDone.Load() {
			t.Fatal("independent thread did not finish while first was blocked")
		}
		close(release)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		o.mu.Lock()
		remaining := len(o.owners)
		o.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("settled owners retained: %d", remaining)
		}
	})
}

func TestThreadOwnersBoundMailboxAndSkipCanceledQueuedCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newThreadOwners()
		defer o.close()
		key := ownerKey("full")
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- o.do(context.Background(), key, func(*threadOwner) error { close(entered); <-release; return nil })
		}()
		<-entered
		ctx, cancel := context.WithCancel(context.Background())
		var ran, completed atomic.Int32
		for range ownerMailbox {
			if !o.try(ctx, key, func(*threadOwner) error { ran.Add(1); return nil }, func(error) { completed.Add(1) }) {
				t.Fatal("mailbox rejected before its bound")
			}
		}
		if o.try(ctx, key, func(*threadOwner) error { return nil }, func(error) {}) {
			t.Fatal("full mailbox admitted another command")
		}
		cancel()
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ran.Load() != 0 || completed.Load() != ownerMailbox {
			t.Fatalf("canceled commands ran=%d completed=%d", ran.Load(), completed.Load())
		}
	})
}

func TestThreadOwnersHoldExecutorUntilActualCancellationCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newThreadOwners()
		defer o.close()
		entered, release, done := make(chan struct{}, ownerExecutors), make(chan struct{}), make(chan error, ownerExecutors)
		ctx, cancel := context.WithCancel(context.Background())
		for i := range ownerExecutors {
			key := ownerKey(string(rune('a' + i)))
			go func() {
				done <- o.do(ctx, key, func(*threadOwner) error { entered <- struct{}{}; <-release; return ctx.Err() })
			}()
		}
		for range ownerExecutors {
			<-entered
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("caller released before native completion")
		default:
		}
		var fifth atomic.Bool
		fifthDone := make(chan error, 1)
		go func() {
			fifthDone <- o.do(context.Background(), ownerKey("fifth"), func(*threadOwner) error { fifth.Store(true); return nil })
		}()
		synctest.Wait()
		if fifth.Load() {
			t.Fatal("cancellation released an occupied DB executor")
		}
		close(release)
		for range ownerExecutors {
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		if err := <-fifthDone; err != nil {
			t.Fatal(err)
		}
	})
}

func TestThreadOwnersRetainAcrossContinuationAndPassivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newThreadOwners()
		defer o.close()
		key := ownerKey("retained")
		release := o.retain(key)
		var generation uint64
		if err := o.do(context.Background(), key, func(owner *threadOwner) error { generation = owner.generation; return nil }); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := o.do(context.Background(), key, func(owner *threadOwner) error {
			if generation != owner.generation {
				t.Fatal("continuation owner was replaced")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		release()
		release()
		synctest.Wait()
		if err := o.do(context.Background(), key, func(owner *threadOwner) error {
			if owner.generation == generation {
				t.Fatal("passivated owner generation reused")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
