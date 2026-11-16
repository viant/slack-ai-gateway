package gateway

import "context"

// PhysicalThreadKey identifies the Slack resource shared across assignment history.
// An integration is an execution identity, not a second owner of that resource.
type PhysicalThreadKey struct {
	AppID, WorkspaceID, ChannelID, RootTS string
}

func (k PhysicalThreadKey) Valid() bool {
	return k.AppID != "" && k.WorkspaceID != "" && k.ChannelID != "" && k.RootTS != ""
}

func (m Message) PhysicalThread() PhysicalThreadKey {
	return PhysicalThreadKey{m.AppID, m.WorkspaceID, m.ChannelID, m.RootTS}
}

// ThreadScopedStore binds native predicates and operation ownership together.
type ThreadScopedStore interface {
	ForThread(PhysicalThreadKey) Store
}

// ThreadExecutor lends a synchronous local store inside one owner command.
// The callback must perform local operations only and must not retain the store.
type ThreadExecutor interface {
	InThread(context.Context, func(Store) error) error
}

// AsyncThreadStore admits a short command without occupying an HTTP executor.
// false means no command was admitted; the caller may release its local permit.
type AsyncThreadStore interface {
	TryInThread(context.Context, func(Store) error, func(error)) bool
}

func ForThread(store Store, key PhysicalThreadKey) Store {
	if scoped, ok := store.(ThreadScopedStore); ok {
		return scoped.ForThread(key)
	}
	return store
}

// InThread enters ownership before acquiring a backend readiness guard.
func InThread(ctx context.Context, store Store, fn func(Store) error) error {
	if executor, ok := store.(ThreadExecutor); ok {
		return executor.InThread(ctx, fn)
	}
	return fn(store)
}

// RetainThread keeps an owner alive while an external continuation is in flight.
func RetainThread(store Store) func() {
	if retained, ok := store.(interface{ RetainThread() func() }); ok {
		return retained.RetainThread()
	}
	return func() {}
}
