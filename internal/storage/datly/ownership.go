package datly

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

const ownerExecutors = 4
const ownerMailbox = 64

var errOwnersClosed = errors.New("thread ownership is closed")

type ownerCommand struct {
	ctx      context.Context
	run      func(*threadOwner) error
	reply    chan error
	started  chan struct{}
	complete func(error)
}

type threadOwner struct {
	key           gateway.PhysicalThreadKey
	generation    uint64
	queue         []ownerCommand
	busy, ready   bool
	refs          int
	preferMessage bool
}

// threadOwners multiplexes logical owners over a fixed number of DB executors.
// It owns no database transaction or timer. A command holds its slot until the
// native invocation actually returns, including completion and cancellation.
type threadOwners struct {
	mu        sync.Mutex
	owners    map[gateway.PhysicalThreadKey]*threadOwner
	ready     []*threadOwner
	changed   chan struct{}
	available chan struct{}
	closed    bool
	next      uint64
	workers   sync.WaitGroup
}

func newThreadOwners() *threadOwners {
	o := &threadOwners{owners: make(map[gateway.PhysicalThreadKey]*threadOwner), changed: make(chan struct{}), available: make(chan struct{}, 1)}
	for range ownerExecutors {
		o.workers.Add(1)
		go o.worker()
	}
	return o
}

func (o *threadOwners) signalLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
	select {
	case o.available <- struct{}{}:
	default:
	}
}

func (s *Store) OwnershipChanges() <-chan struct{} { return s.workOwner().owners.available }

// Activation follows exclusive startup recovery and identity reconciliation.
func (s *Store) StartThreadOwnership() { s.workOwner().active.Store(true) }

func (s *Store) failUnstartedThreads(ctx context.Context, reason string) error {
	if s.scope <= 0 {
		return gateway.ErrConflict
	}
	var cursor *gateway.WorkCursor
	now := time.Now()
	for _, mode := range []string{"cleanup_keys", "backend_keys"} {
		cursor = nil
		for {
			in := workInput(now, mode, cursor)
			in.SetIntegrationID(int(s.scope))
			rows, err := s.readWork(ctx, in)
			if err != nil {
				return err
			}
			keys := workRows(rows, now)
			for _, candidate := range keys {
				if mode == "backend_keys" {
					s.reconcileThreadHint(candidate.Thread)
					continue
				}
				if err := s.ForThread(candidate.Thread).FailUnstarted(ctx, reason); err != nil {
					return err
				}
			}
			if len(keys) < 256 {
				break
			}
			cursor = &gateway.WorkCursor{After: keys[len(keys)-1].Thread, Cutoff: in.Cutoff}
		}
	}
	return nil
}

func (o *threadOwners) getLocked(key gateway.PhysicalThreadKey) *threadOwner {
	entry := o.owners[key]
	if entry == nil {
		o.next++
		entry = &threadOwner{key: key, generation: o.next}
		o.owners[key] = entry
	}
	return entry
}

func (o *threadOwners) retain(key gateway.PhysicalThreadKey) func() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return func() {}
	}
	entry := o.getLocked(key)
	entry.refs++
	o.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			o.mu.Lock()
			entry.refs--
			o.passivateLocked(entry)
			o.mu.Unlock()
		})
	}
}

func (o *threadOwners) passivateLocked(entry *threadOwner) {
	if !entry.busy && !entry.ready && entry.refs == 0 && len(entry.queue) == 0 && o.owners[entry.key] == entry {
		delete(o.owners, entry.key)
	}
}

func (o *threadOwners) do(ctx context.Context, key gateway.PhysicalThreadKey, run func(*threadOwner) error) error {
	if !key.Valid() {
		return gateway.ErrConflict
	}
	command := ownerCommand{ctx: ctx, run: run, reply: make(chan error, 1), started: make(chan struct{})}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		o.mu.Lock()
		if o.closed {
			o.mu.Unlock()
			return errOwnersClosed
		}
		entry := o.getLocked(key)
		if len(entry.queue) < ownerMailbox {
			entry.queue = append(entry.queue, command)
			if !entry.busy && !entry.ready {
				entry.ready = true
				o.ready = append(o.ready, entry)
			}
			o.signalLocked()
			o.mu.Unlock()
			break
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	// A caller may abandon its buffered reply. The worker still owns transaction
	// completion; cancellation cannot release its executor early.
	select {
	case err := <-command.reply:
		return err
	case <-ctx.Done():
		select {
		case <-command.started:
			return <-command.reply
		default:
			// Once canceled, a not-yet-started command cannot invoke run.
			return ctx.Err()
		}
	}
}

func (o *threadOwners) worker() {
	defer o.workers.Done()
	for {
		o.mu.Lock()
		for len(o.ready) == 0 && !o.closed {
			changed := o.changed
			o.mu.Unlock()
			<-changed
			o.mu.Lock()
		}
		if len(o.ready) == 0 && o.closed {
			o.mu.Unlock()
			return
		}
		entry := o.ready[0]
		o.ready[0] = nil
		o.ready = o.ready[1:]
		entry.ready, entry.busy = false, true
		command := entry.queue[0]
		entry.queue[0] = ownerCommand{}
		entry.queue = entry.queue[1:]
		o.signalLocked()
		o.mu.Unlock()
		close(command.started)
		err := command.ctx.Err()
		if err == nil {
			err = command.run(entry)
		}
		if command.reply != nil {
			command.reply <- err
		}
		if command.complete != nil {
			command.complete(err)
		}
		o.mu.Lock()
		entry.busy = false
		if len(entry.queue) != 0 {
			entry.ready = true
			o.ready = append(o.ready, entry)
		}
		o.passivateLocked(entry)
		o.signalLocked()
		o.mu.Unlock()
	}
}

func (o *threadOwners) try(ctx context.Context, key gateway.PhysicalThreadKey, run func(*threadOwner) error, complete func(error)) bool {
	if ctx.Err() != nil || !key.Valid() {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	entry := o.getLocked(key)
	if len(entry.queue) >= ownerMailbox {
		return false
	}
	entry.queue = append(entry.queue, ownerCommand{ctx: ctx, run: run, complete: complete, started: make(chan struct{})})
	if !entry.busy && !entry.ready {
		entry.ready = true
		o.ready = append(o.ready, entry)
	}
	o.signalLocked()
	return true
}

func (o *threadOwners) close() {
	o.mu.Lock()
	o.closed = true
	o.signalLocked()
	o.mu.Unlock()
	o.workers.Wait()
}

func (s *Store) ForThread(key gateway.PhysicalThreadKey) gateway.Store {
	root := s.workOwner()
	return &Store{server: root.server, root: root, scope: s.scope, scopeName: s.scopeName, thread: &key}
}

func (s *Store) RetainThread() func() {
	if s.thread == nil || s.workOwner().owners == nil {
		return func() {}
	}
	return s.workOwner().owners.retain(*s.thread)
}

func (s *Store) InThread(ctx context.Context, fn func(gateway.Store) error) error {
	if s.thread == nil || s.local {
		return fn(s)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	queued := time.Now()
	return s.workOwner().owners.do(ctx, *s.thread, func(entry *threadOwner) error {
		local := &Store{server: s.server, root: s.workOwner(), scope: s.scope, scopeName: s.scopeName,
			thread: s.thread, local: true, owner: entry, commandDeadline: deadline, queueWait: time.Since(queued)}
		return fn(local)
	})
}

func (s *Store) TryInThread(ctx context.Context, fn func(gateway.Store) error, complete func(error)) bool {
	if s.thread == nil || s.local {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	deadline, _ := ctx.Deadline()
	queued := time.Now()
	accepted := s.workOwner().owners.try(ctx, *s.thread, func(entry *threadOwner) error {
		local := &Store{server: s.server, root: s.workOwner(), scope: s.scope, scopeName: s.scopeName,
			thread: s.thread, local: true, owner: entry, commandDeadline: deadline, queueWait: time.Since(queued)}
		return fn(local)
	}, func(err error) { cancel(); complete(err) })
	if !accepted {
		cancel()
	}
	return accepted
}

func (s *Store) commandContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if !s.commandDeadline.IsZero() {
		return context.WithDeadline(ctx, s.commandDeadline)
	}
	return context.WithTimeout(ctx, 30*time.Second)
}
