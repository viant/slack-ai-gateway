package workflow

import (
	"context"
	"fmt"
	"time"

	tw "github.com/viant/slack-ai-gateway/internal/datly/topology/integrations/write"
	tr "github.com/viant/slack-ai-gateway/internal/datly/topology/read"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

func (x *execution) topology(ctx context.Context, id int64, lock bool) ([]*tr.Integration, error) {
	in := &tr.Input{}
	if id > 0 {
		in.SetIntegrationID(int(id))
	}
	out, err := readGraph[tr.Output](ctx, x.invoke, graphTarget[tr.ReaderComponent]("reader", "GET", "topology"), in, lock)
	if err != nil {
		return nil, err
	}
	return out.Data, nil
}

func identityValue(row *tr.Integration) (storage.Integration, error) {
	if row == nil {
		return storage.Integration{}, fmt.Errorf("missing integration assignment")
	}
	return storage.Integration{ID: int64(row.Id), Name: row.Name, Active: row.Active != 0, AppID: row.SlackAppId, WorkspaceID: row.SlackWorkspaceId,
		BackendType: row.BackendType, BackendURL: row.BackendEndpoint, BackendAgentID: row.BackendAgentId, PrincipalID: row.BackendPrincipalId}, nil
}

func (x *execution) readIdentity(ctx context.Context, id int64) (*storage.Integration, error) {
	rows, err := x.topology(ctx, id, false)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("integration assignment is not registered")
	}
	v, err := identityValue(rows[0])
	return &v, err
}

func sameAssignment(a, b storage.Integration) bool {
	return a.Name == b.Name && a.AppID == b.AppID && a.WorkspaceID == b.WorkspaceID && a.BackendType == b.BackendType && a.BackendURL == b.BackendURL && a.BackendAgentID == b.BackendAgentID && a.PrincipalID == b.PrincipalID
}

func (x *execution) writeIntegrations(ctx context.Context, rows []*tw.Integration) error {
	if len(rows) == 0 {
		return nil
	}
	in := &tw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[tw.WriterComponent]("writer", "PATCH", "topology/integrations"), in)
}

// The existing transaction owns recovery, assignment reconciliation and closure.
// Historical identities are never reactivated; no external operation happens here.
func (x *execution) reconcileIntegrations(ctx context.Context, wanted []storage.Integration, out *Output) error {
	if x.scope != 0 || x.name != "" {
		return fmt.Errorf("integration reconciliation requires the database owner")
	}
	rows, err := x.topology(ctx, 0, true)
	if err != nil {
		return err
	}
	if err = x.recover(ctx, &Input{}, &Output{}); err != nil {
		return err
	}
	previous := make([]storage.Integration, 0, len(rows))
	active := map[string]storage.Integration{}
	for _, row := range rows {
		v, e := identityValue(row)
		if e != nil {
			return e
		}
		previous = append(previous, v)
		if v.Active {
			if _, exists := active[v.Name]; exists {
				return fmt.Errorf("ambiguous active integration assignment")
			}
			active[v.Name] = v
		}
	}
	now := timestamp(time.Now())
	names := map[string]bool{}
	updates := []*tw.Integration{}
	result := make([]storage.Integration, 0, len(wanted))
	pending := map[int]*tw.Integration{}
	for _, v := range wanted {
		if v.Name == "" || names[v.Name] {
			return fmt.Errorf("integration names must be unique and nonempty")
		}
		names[v.Name] = true
		if old, ok := active[v.Name]; ok && sameAssignment(old, v) {
			result = append(result, old)
			continue
		}
		row := &tw.Integration{Name: v.Name, Active: 1, SlackAppId: v.AppID, SlackWorkspaceId: v.WorkspaceID, BackendType: v.BackendType, BackendEndpoint: v.BackendURL, BackendAgentId: v.BackendAgentID, BackendPrincipalId: v.PrincipalID, CreatedAt: now, UpdatedAt: now}
		pending[len(result)] = row
		updates = append(updates, row)
		v.ID = 0
		v.Active = true
		result = append(result, v)
	}
	for i := range previous {
		old := &previous[i]
		if !old.Active {
			continue
		}
		keep := false
		for _, v := range result {
			if v.ID == old.ID {
				keep = true
				break
			}
		}
		if !keep {
			row := &tw.Integration{}
			row.SetId(int(old.ID))
			row.SetActive(0)
			row.SetUpdatedAt(now)
			updates = append(updates, row)
			old.Active = false
		}
	}
	if err = x.writeIntegrations(ctx, updates); err != nil {
		return err
	}
	for i, row := range pending {
		result[i].ID = int64(row.Id)
	}
	if err = x.closeInactive(ctx, previous, result, now); err != nil {
		return err
	}
	out.Integrations = result
	return nil
}
