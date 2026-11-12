package workflow

import (
	"context"
	"fmt"
	"reflect"

	dexec "github.com/viant/datly/exec"
	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	dr "github.com/viant/slack-ai-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/slack-ai-gateway/internal/datly/deliveries/write"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/xdatly/handler"
)

func graphTarget[T any](name, method, path string) handler.ComponentTarget {
	return handler.ComponentTarget{Scope: reflect.TypeFor[T]().PkgPath(), Name: name, Method: method, Path: "/internal/gateway/" + path}
}
func readGraph[T any](ctx context.Context, invoker handler.ComponentInvoker, target handler.ComponentTarget, input any, lock bool) (*T, error) {
	if lock {
		ctx = (dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}}).Context(ctx)
	}
	value, err := invoker.Invoke(ctx, handler.ComponentRequest{Target: target, Input: input})
	if err != nil {
		return nil, err
	}
	out, ok := value.(*T)
	if !ok || out == nil {
		return nil, fmt.Errorf("unexpected graph output %T", value)
	}
	return out, nil
}
func writeGraph(ctx context.Context, invoker handler.ComponentInvoker, target handler.ComponentTarget, input any) error {
	_, err := invoker.Invoke(ctx, handler.ComponentRequest{Target: target, Input: input})
	return err
}
func (x *execution) readJobs(ctx context.Context, in *jr.Input, lock bool) ([]*jr.GatewayJob, error) {
	if x.scope != 0 {
		in.SetIntegrationID(int(x.scope))
	}
	if x.identity != nil {
		in.SetAppID(x.identity.AppID)
		in.SetWorkspaceID(x.identity.WorkspaceID)
		if in.Mode == "publication" || in.Mode == "stale_thread" || in.Mode == "physical_thread" {
			in.SetIntegrationID(0)
		}
	}
	out, e := readGraph[jr.Output](ctx, x.invoke, graphTarget[jr.ReaderComponent]("reader", "GET", "jobs"), in, lock)
	if e != nil {
		return nil, e
	}
	for _, j := range out.Data {
		if j == nil || (x.identity != nil && (j.SlackAppId != x.identity.AppID || j.SlackWorkspaceId != x.identity.WorkspaceID)) {
			return nil, gateway.ErrConflict
		}
		if _, err := contextFromJob(j); err != nil {
			return nil, err
		}
	}
	return out.Data, nil
}

func (x *execution) publicationJob(ctx context.Context, id int64, lock bool) (*jr.GatewayJob, error) {
	in := &jr.Input{}
	in.SetMode("publication")
	in.SetID(id)
	rows, err := x.readJobs(ctx, in, lock)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, gateway.ErrConflict
	}
	return rows[0], nil
}
func (x *execution) writeJobs(ctx context.Context, rows []*jw.GatewayJob) error {
	in := &jw.Input{}
	in.SetGatewayJobs(rows)
	return writeGraph(ctx, x.invoke, graphTarget[jw.WriterComponent]("writer", "PATCH", "jobs"), in)
}
func (x *execution) job(ctx context.Context, id int64, lock bool) (*jr.GatewayJob, error) {
	in := &jr.Input{}
	in.SetMode("key")
	in.SetID(id)
	rows, e := x.readJobs(ctx, in, lock)
	if e != nil {
		return nil, e
	}
	if len(rows) != 1 {
		return nil, gateway.ErrConflict
	}
	return rows[0], nil
}
func (x *execution) readBindings(ctx context.Context, in *br.Input, lock bool) ([]*br.ConversationBinding, error) {
	if x.scope != 0 {
		in.SetIntegrationID(int(x.scope))
	}
	if x.identity != nil && in.Mode == "physical_thread" {
		in.SetIntegrationID(0)
		in.SetAppID(x.identity.AppID)
		in.SetWorkspaceID(x.identity.WorkspaceID)
	}
	out, e := readGraph[br.Output](ctx, x.invoke, graphTarget[br.ReaderComponent]("reader", "GET", "bindings"), in, lock)
	if e != nil {
		return nil, e
	}
	return out.Data, nil
}
func (x *execution) writeBindings(ctx context.Context, rows []*bw.ConversationBinding) error {
	in := &bw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[bw.WriterComponent]("writer", "PATCH", "bindings"), in)
}
func (x *execution) readDeliveries(ctx context.Context, in *dr.Input, lock bool) ([]*dr.SlackOutgoingMessage, error) {
	in.SetIntegrationID(int(x.scope))
	if x.name != "" {
		in.SetIntegrationName(x.name)
	}
	out, e := readGraph[dr.Output](ctx, x.invoke, graphTarget[dr.ReaderComponent]("reader", "GET", "deliveries"), in, lock)
	if e != nil {
		return nil, e
	}
	for _, d := range out.Data {
		if d == nil || (x.identity != nil && (d.SlackAppId != x.identity.AppID || d.SlackWorkspaceId != x.identity.WorkspaceID)) {
			return nil, gateway.ErrConflict
		}
	}
	return out.Data, nil
}
func (x *execution) writeDeliveries(ctx context.Context, rows []*dw.SlackOutgoingMessage) error {
	in := &dw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[dw.WriterComponent]("writer", "PATCH", "deliveries"), in)
}
func (x *execution) readSessions(ctx context.Context, in *sr.Input, lock bool) ([]*sr.SlackAgentSessionPublication, error) {
	if x.scope != 0 {
		in.SetIntegrationID(int(x.scope))
	}
	if x.identity != nil {
		in.SetAppID(x.identity.AppID)
		in.SetWorkspaceID(x.identity.WorkspaceID)
	}
	out, e := readGraph[sr.Output](ctx, x.invoke, graphTarget[sr.ReaderComponent]("reader", "GET", "sessions"), in, lock)
	if e != nil {
		return nil, e
	}
	for _, p := range out.Data {
		if p == nil || (x.identity != nil && (p.SlackAppId != x.identity.AppID || p.SlackWorkspaceId != x.identity.WorkspaceID)) {
			return nil, gateway.ErrConflict
		}
		var source, latest bool
		for _, j := range p.GatewayJobs {
			if j == nil {
				continue
			}
			// The native relation already selects this physical bot/thread.
			// Both single-FK pointers must identify a job in that relation.
			source = source || deref(j.Id) == p.StatusSourceJobId
			latest = latest || deref(j.Id) == p.LatestStatusJobId
		}
		if !source || !latest {
			return nil, gateway.ErrConflict
		}
	}
	return out.Data, nil
}
func (x *execution) writeSessions(ctx context.Context, rows []*sw.SlackAgentSessionPublication) error {
	in := &sw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[sw.WriterComponent]("writer", "PATCH", "sessions"), in)
}
