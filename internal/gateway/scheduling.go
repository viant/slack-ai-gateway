package gateway

import (
	"context"
	"time"
)

// WorkHint separates a conservative immediate probe from a future deadline.
// A probe is consumed when an authoritative scoped worker finds no work.
type WorkHint struct {
	ProbeNow bool
	NextAt   time.Time
}

// WorkCandidate contains scheduling metadata only. Atomic scoped claims remain
// authoritative; discovery neither claims work nor authorizes its execution.
type WorkCandidate struct {
	IntegrationID int64
	Thread        PhysicalThreadKey
	// Reconcile requests refreshed deadline metadata after a timing change.
	Reconcile bool
	Query     WorkHint
	Outbound  WorkHint
}

type WorkCursor struct {
	After  PhysicalThreadKey
	Cutoff time.Time
}

type WorkPage struct {
	Candidates []WorkCandidate
	Next       *WorkCursor
	Deadline   *WorkCandidate
}

// ThreadWorkPlanner preserves bounded discovery and direct, committed-key hints.
type ThreadWorkPlanner interface {
	WorkPage(context.Context, time.Time, *WorkCursor) (WorkPage, error)
	TakeWorkHints() []WorkCandidate
	ThreadRevision(PhysicalThreadKey) uint64
}

// WorkPlanner is supplied by the shared storage owner and its scoped handles.
// Changes are coalesced, nonblocking hints. Revision(0) advances for global
// invalidations only; scoped writes advance the affected integration generations
// so unrelated activity cannot repeatedly unpark an empty worker.
type WorkPlanner interface {
	WorkCandidates(context.Context, time.Time) ([]WorkCandidate, error)
	WorkChanges() <-chan struct{}
	WorkRevision(int64) uint64
	IntegrationID() int64
}
