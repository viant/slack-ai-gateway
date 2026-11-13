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
}

func newWorkState() *workState {
	return &workState{changes: make(chan struct{}, 1), revisions: map[int64]uint64{}, bots: map[int64]physicalBot{}}
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

func (s *Store) workChanged(out *workflow.Output) {
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
	}
	state.mu.Unlock()
	select {
	case state.changes <- struct{}{}:
	default:
	}
}

var workTarget = dexec.ComponentTarget{
	Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[workread.ReaderComponent]().PkgPath(), Name: "reader"},
	Route:     spec.RouteRef{Method: "GET", Path: "/internal/gateway/operations/work"},
}

// WorkCandidates invokes the native timing reader directly. It has no workflow,
// identity read, transaction starter, lock, payload relation or state transition.
func (s *Store) WorkCandidates(ctx context.Context, now time.Time) ([]gateway.WorkCandidate, error) {
	owner := s.workOwner()
	if owner.server == nil {
		return nil, fmt.Errorf("gateway storage is not initialized")
	}
	now = now.UTC().Truncate(time.Millisecond)
	in := &workread.Input{}
	in.SetNow(now)
	value, err := owner.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: workTarget, Input: in})
	if err != nil {
		return nil, err
	}
	out, ok := value.(*workread.Output)
	if !ok || out == nil {
		return nil, fmt.Errorf("unexpected work discovery output %T", value)
	}
	type timings struct {
		candidate gateway.WorkCandidate
		cooldown  time.Time
		outbound  bool
	}
	byID := map[int64]*timings{}
	bots := map[int64]physicalBot{}
	for _, row := range out.Data {
		id := row.IntegrationId
		bots[id] = physicalBot{row.SlackAppId, row.SlackWorkspaceId}
		item := byID[id]
		if item == nil {
			item = &timings{candidate: gateway.WorkCandidate{IntegrationID: id}}
			byID[id] = item
		}
		switch row.Kind {
		case "query":
			addWorkTime(&item.candidate.Query, row.CandidateAt, now)
		case "outbound":
			item.outbound = true
			addWorkTime(&item.candidate.Outbound, row.CandidateAt, now)
			// CandidateAt is the retry gate; refresh and staleness remain
			// independent future wakeups even when an immediate probe is consumed.
			for _, deadline := range []*time.Time{row.RefreshAt, observationDeadline(row.ObservedAt)} {
				if deadline != nil && deadline.After(now) {
					at := *deadline
					if row.CandidateAt != nil && row.CandidateAt.After(at) {
						at = *row.CandidateAt
					}
					addWorkTime(&item.candidate.Outbound, &at, now)
				}
			}
		case "cooldown":
			if row.CandidateAt != nil && row.CandidateAt.After(item.cooldown) {
				item.cooldown = *row.CandidateAt
			}
		}
	}
	result := make([]gateway.WorkCandidate, 0, len(byID))
	for _, item := range byID {
		if item.outbound && item.cooldown.After(now) {
			immediate := item.candidate.Outbound.ProbeNow
			item.candidate.Outbound.ProbeNow = false
			if immediate || item.candidate.Outbound.NextAt.IsZero() || item.candidate.Outbound.NextAt.Before(item.cooldown) {
				item.candidate.Outbound.NextAt = item.cooldown
			}
		}
		result = append(result, item.candidate)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IntegrationID < result[j].IntegrationID })
	owner.work.mu.Lock()
	for id, bot := range bots {
		owner.work.bots[id] = bot
	}
	owner.work.mu.Unlock()
	return result, nil
}

func addWorkTime(hint *gateway.WorkHint, at *time.Time, now time.Time) {
	if at == nil || !at.After(now) {
		hint.ProbeNow = true
		return
	}
	if hint.NextAt.IsZero() || at.Before(hint.NextAt) {
		hint.NextAt = at.UTC()
	}
}

func observationDeadline(observed *time.Time) *time.Time {
	if observed == nil {
		return nil
	}
	// The authoritative projection uses age > maxAge, with millisecond storage.
	at := observed.Add(gateway.ObservationMaxAge + time.Millisecond)
	return &at
}
