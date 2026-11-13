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
	Query         WorkHint
	Outbound      WorkHint
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
