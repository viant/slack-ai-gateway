package datly

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	workread "github.com/viant/slack-ai-gateway/internal/datly/operations/work"
	topologyread "github.com/viant/slack-ai-gateway/internal/datly/topology/read"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
)

type physicalBot struct{ app, workspace string }

type workState struct {
	mu        sync.Mutex
	changes   chan struct{}
	global    uint64
	revisions map[int64]uint64
	bots      map[int64]physicalBot
	threads   map[gateway.PhysicalThreadKey]uint64
	pending   map[gateway.PhysicalThreadKey]gateway.WorkCandidate
}

func newWorkState() *workState {
	return &workState{changes: make(chan struct{}, 1), revisions: map[int64]uint64{}, bots: map[int64]physicalBot{}, threads: map[gateway.PhysicalThreadKey]uint64{}, pending: map[gateway.PhysicalThreadKey]gateway.WorkCandidate{}}
}

func (s *Store) workOwner() *Store {
	if s.root != nil {
		return s.root
	}
	return s
}

var _ gateway.WorkPlanner = (*Store)(nil)

func (s *Store) IntegrationID() int64         { return s.scope }
func (s *Store) WorkChanges() <-chan struct{} { return s.workOwner().work.changes }

// Zero tracks unscoped invalidation only; a scoped write must not unpark an
// unrelated integration whose authoritative worker already found no work.
func (s *Store) WorkRevision(id int64) uint64 {
	state := s.workOwner().work
	state.mu.Lock()
	defer state.mu.Unlock()
	if id == 0 {
		return state.global
	}
	return state.revisions[id]
}

func (s *Store) workChanged(out *workflow.Output, action string) {
	changed, id, app, workspace := out.WorkChange()
	if !changed {
		return
	}
	state := s.workOwner().work
	state.mu.Lock()
	if id == 0 {
		state.global++
		for _, integration := range out.Integrations {
			state.bots[integration.ID] = physicalBot{integration.AppID, integration.WorkspaceID}
		}
	} else {
		bot := physicalBot{app, workspace}
		state.bots[id] = bot
		// Slack Retry-After and physical-thread publications cross assignment
		// boundaries, so all matching physical bots share invalidation.
		for candidate, other := range state.bots {
			if other == bot {
				state.revisions[candidate]++
			}
		}
		keys := out.AffectedThreads
		if s.thread != nil {
			keys = append(keys, *s.thread)
		}
		for _, key := range keys {
			if !key.Valid() {
				continue
			}
			state.threads[key]++
			refresh := state.pending[key].Reconcile || action == "schedule_poll" || action == "finish_status" || action == "retry_delivery"
			state.pending[key] = gateway.WorkCandidate{IntegrationID: id, Thread: key,
				Query: gateway.WorkHint{ProbeNow: true}, Outbound: gateway.WorkHint{ProbeNow: true}, Reconcile: refresh}
		}
	}
	state.mu.Unlock()
	select {
	case state.changes <- struct{}{}:
	default:
	}
}

func (s *Store) TakeWorkHints() []gateway.WorkCandidate {
	state := s.workOwner().work
	state.mu.Lock()
	defer state.mu.Unlock()
	result := make([]gateway.WorkCandidate, 0, len(state.pending))
	for _, hint := range state.pending {
		result = append(result, hint)
	}
	clear(state.pending)
	return result
}

func (s *Store) ThreadRevision(key gateway.PhysicalThreadKey) uint64 {
	state := s.workOwner().work
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.threads[key]
}

func (s *Store) ForgetWorkThread(key gateway.PhysicalThreadKey) {
	state := s.workOwner().work
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, pending := state.pending[key]; !pending {
		delete(state.threads, key)
	}
}

var workTarget = dexec.ComponentTarget{
	Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[workread.ReaderComponent]().PkgPath(), Name: "reader"},
	Route:     spec.RouteRef{Method: "GET", Path: "/internal/gateway/operations/work"},
}

// readWork uses the existing native metadata reader without a workflow or transaction.
func (s *Store) readWork(ctx context.Context, input *workread.Input) ([]*workread.WorkTiming, error) {
	owner := s.workOwner()
	if owner.server == nil {
		return nil, fmt.Errorf("gateway storage is not initialized")
	}
	value, err := s.invokeReader(ctx, dexec.ComponentRequest{Target: workTarget, Input: input})
	if err != nil {
		return nil, err
	}
	out, ok := value.(*workread.Output)
	if !ok || out == nil {
		return nil, fmt.Errorf("unexpected work discovery output %T", value)
	}
	return out.Data, nil
}

func workInput(now time.Time, mode string, cursor *gateway.WorkCursor) *workread.Input {
	in := &workread.Input{}
	in.SetMode(mode)
	in.SetNow(now.UTC().Truncate(time.Millisecond))
	cutoff := now
	if cursor != nil {
		cutoff = cursor.Cutoff
		in.SetAfterApp(cursor.After.AppID)
		in.SetAfterWorkspace(cursor.After.WorkspaceID)
		in.SetAfterChannel(cursor.After.ChannelID)
		in.SetAfterRoot(cursor.After.RootTS)
	}
	in.SetCutoff(cutoff.UTC().Truncate(time.Millisecond))
	return in
}

func workRows(rows []*workread.WorkTiming, now time.Time) []gateway.WorkCandidate {
	type key struct {
		id     int64
		thread gateway.PhysicalThreadKey
	}
	byKey := map[key]*gateway.WorkCandidate{}
	var result []gateway.WorkCandidate
	for _, row := range rows {
		k := key{row.IntegrationId, gateway.PhysicalThreadKey{AppID: row.SlackAppId, WorkspaceID: row.SlackWorkspaceId, ChannelID: row.SlackChannelId, RootTS: row.SlackThreadRootMessageId}}
		item := byKey[k]
		if item == nil {
			item = &gateway.WorkCandidate{IntegrationID: k.id, Thread: k.thread}
			byKey[k] = item
		}
		switch row.Kind {
		case "query":
			addWorkTime(&item.Query, row.CandidateAt, now)
		case "outbound":
			addWorkTime(&item.Outbound, row.CandidateAt, now)
			if deadline := row.RefreshAt; deadline != nil && deadline.After(now) {
				at := *deadline
				if row.CandidateAt != nil && row.CandidateAt.After(at) {
					at = *row.CandidateAt
				}
				addWorkTime(&item.Outbound, &at, now)
			}
		}
	}
	for _, item := range byKey {
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool { return threadLess(result[i].Thread, result[j].Thread) })
	return result
}
func threadLess(a, b gateway.PhysicalThreadKey) bool {
	if a.AppID != b.AppID {
		return a.AppID < b.AppID
	}
	if a.WorkspaceID != b.WorkspaceID {
		return a.WorkspaceID < b.WorkspaceID
	}
	if a.ChannelID != b.ChannelID {
		return a.ChannelID < b.ChannelID
	}
	return a.RootTS < b.RootTS
}

func (s *Store) WorkPage(ctx context.Context, now time.Time, cursor *gateway.WorkCursor) (gateway.WorkPage, error) {
	in := workInput(now, "page", cursor)
	rows, err := s.readWork(ctx, in)
	if err != nil {
		return gateway.WorkPage{}, err
	}
	page := gateway.WorkPage{Candidates: workRows(rows, now)}
	if len(page.Candidates) >= 256 {
		last := page.Candidates[len(page.Candidates)-1]
		page.Next = &gateway.WorkCursor{After: last.Thread, Cutoff: in.Cutoff}
	}
	deadlines, err := s.readWork(ctx, workInput(now, "deadline", nil))
	if err != nil {
		return gateway.WorkPage{}, err
	}
	var earliest time.Time
	for _, candidate := range workRows(deadlines, now) {
		at := candidate.Query.NextAt
		if at.IsZero() || !candidate.Outbound.NextAt.IsZero() && candidate.Outbound.NextAt.Before(at) {
			at = candidate.Outbound.NextAt
		}
		if !at.IsZero() && (earliest.IsZero() || at.Before(earliest)) {
			copy := candidate
			page.Deadline = &copy
			earliest = at
		}
	}
	return page, nil
}

// WorkCandidates retains the integration-level diagnostic contract. Runtime
// dispatch uses bounded WorkPage instead of collecting historical thread keys.
func (s *Store) WorkCandidates(ctx context.Context, now time.Time) ([]gateway.WorkCandidate, error) {
	in := &topologyread.Input{}
	in.SetActive(1)
	value, err := s.invokeReader(ctx, dexec.ComponentRequest{Target: identityWorkTarget, Input: in})
	if err != nil {
		return nil, err
	}
	identities, ok := value.(*topologyread.Output)
	if !ok || identities == nil {
		return nil, fmt.Errorf("unexpected work identity output")
	}
	byID := map[int64]*gateway.WorkCandidate{}
	for _, identity := range identities.Data {
		id := int64(identity.Id)
		byID[id] = &gateway.WorkCandidate{IntegrationID: id}
	}
	rows, err := s.readWork(ctx, workInput(now, "target", nil))
	if err != nil {
		return nil, err
	}
	for _, c := range workRows(rows, now) {
		dest := byID[c.IntegrationID]
		if dest == nil {
			continue
		}
		dest.Query.ProbeNow = dest.Query.ProbeNow || c.Query.ProbeNow
		dest.Outbound.ProbeNow = dest.Outbound.ProbeNow || c.Outbound.ProbeNow
		for _, pair := range []struct {
			target *gateway.WorkHint
			source gateway.WorkHint
		}{{&dest.Query, c.Query}, {&dest.Outbound, c.Outbound}} {
			if !pair.source.NextAt.IsZero() && (pair.target.NextAt.IsZero() || pair.source.NextAt.Before(pair.target.NextAt)) {
				pair.target.NextAt = pair.source.NextAt
			}
		}
	}
	result := make([]gateway.WorkCandidate, 0, len(byID))
	for _, c := range byID {
		result = append(result, *c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IntegrationID < result[j].IntegrationID })
	return result, nil
}

var identityWorkTarget = dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[topologyread.ReaderComponent]().PkgPath(), Name: "reader"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/gateway/topology"}}

func addWorkTime(hint *gateway.WorkHint, at *time.Time, now time.Time) {
	if at == nil || !at.After(now) {
		hint.ProbeNow = true
		return
	}
	if hint.NextAt.IsZero() || at.Before(hint.NextAt) {
		hint.NextAt = at.UTC()
	}
}
