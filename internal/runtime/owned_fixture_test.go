package runtime_test

import (
	"context"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

// Native fixture decorators preserve the storage capabilities exercised by the
// actual runtime instead of accidentally selecting the legacy fake scheduler.
type nativeRuntimeStore struct{ storage.Store }

func (s nativeRuntimeStore) ForThread(key gateway.PhysicalThreadKey) gateway.Store {
	return nativeRuntimeStore{Store: gateway.ForThread(s.Store, key).(storage.Store)}
}
func (s nativeRuntimeStore) InThread(ctx context.Context, fn func(gateway.Store) error) error {
	return gateway.InThread(ctx, s.Store, fn)
}
func (s nativeRuntimeStore) TryInThread(ctx context.Context, fn func(gateway.Store) error, done func(error)) bool {
	return s.Store.(gateway.AsyncThreadStore).TryInThread(ctx, fn, done)
}
func (s nativeRuntimeStore) RetainThread() func() { return gateway.RetainThread(s.Store) }
func (s nativeRuntimeStore) WorkPage(ctx context.Context, now time.Time, cursor *gateway.WorkCursor) (gateway.WorkPage, error) {
	return s.Store.(gateway.ThreadWorkPlanner).WorkPage(ctx, now, cursor)
}
func (s nativeRuntimeStore) TakeWorkHints() []gateway.WorkCandidate {
	return s.Store.(gateway.ThreadWorkPlanner).TakeWorkHints()
}
func (s nativeRuntimeStore) ThreadRevision(key gateway.PhysicalThreadKey) uint64 {
	return s.Store.(gateway.ThreadWorkPlanner).ThreadRevision(key)
}
func (s nativeRuntimeStore) StartThreadOwnership() {
	s.Store.(interface{ StartThreadOwnership() }).StartThreadOwnership()
}
func (s nativeRuntimeStore) OwnershipChanges() <-chan struct{} {
	return s.Store.(interface{ OwnershipChanges() <-chan struct{} }).OwnershipChanges()
}

type failedQueryThread struct {
	nativeRuntimeStore
	claim func(context.Context) (*gateway.GatewayJob, error)
}

func fixtureThread(store gateway.Store, root string) gateway.Store {
	return gateway.ForThread(store, gateway.PhysicalThreadKey{AppID: "A", WorkspaceID: "T", ChannelID: "C", RootTS: root})
}

func (s failedQueryThread) ClaimJob(ctx context.Context) (*gateway.GatewayJob, error) {
	return s.claim(ctx)
}
