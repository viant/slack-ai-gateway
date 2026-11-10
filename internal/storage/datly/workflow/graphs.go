package workflow

import (
	"context"
	"fmt"
	"reflect"

	br "github.com/viant/agently-slack-gateway/internal/datly/bindings/read"
	bw "github.com/viant/agently-slack-gateway/internal/datly/bindings/write"
	dr "github.com/viant/agently-slack-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/agently-slack-gateway/internal/datly/deliveries/write"
	jr "github.com/viant/agently-slack-gateway/internal/datly/read"
	sr "github.com/viant/agently-slack-gateway/internal/datly/sessions/read"
	sw "github.com/viant/agently-slack-gateway/internal/datly/sessions/write"
	jw "github.com/viant/agently-slack-gateway/internal/datly/write"
	"github.com/viant/agently-slack-gateway/internal/gateway"
	dexec "github.com/viant/datly/exec"
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
func (x *execution) readJobs(ctx context.Context, in *jr.Input, lock bool) ([]*jr.Job, error) {
	out, e := readGraph[jr.Output](ctx, x.invoke, graphTarget[jr.ReaderComponent]("reader", "GET", "jobs"), in, lock)
	if e != nil {
		return nil, e
	}
	return out.Data, nil
}
func (x *execution) writeJobs(ctx context.Context, rows []*jw.Job) error {
	in := &jw.Input{}
	in.SetJobs(rows)
	return writeGraph(ctx, x.invoke, graphTarget[jw.WriterComponent]("writer", "PATCH", "jobs"), in)
}
func (x *execution) job(ctx context.Context, id int64, lock bool) (*jr.Job, error) {
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
func (x *execution) readBindings(ctx context.Context, in *br.Input, lock bool) ([]*br.Binding, error) {
	out, e := readGraph[br.Output](ctx, x.invoke, graphTarget[br.ReaderComponent]("reader", "GET", "bindings"), in, lock)
	if e != nil {
		return nil, e
	}
	return out.Data, nil
}
func (x *execution) writeBindings(ctx context.Context, rows []*bw.Binding) error {
	in := &bw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[bw.WriterComponent]("writer", "PATCH", "bindings"), in)
}
func (x *execution) readDeliveries(ctx context.Context, in *dr.Input, lock bool) ([]*dr.Delivery, error) {
	out, e := readGraph[dr.Output](ctx, x.invoke, graphTarget[dr.ReaderComponent]("reader", "GET", "deliveries"), in, lock)
	if e != nil {
		return nil, e
	}
	return out.Data, nil
}
func (x *execution) writeDeliveries(ctx context.Context, rows []*dw.Delivery) error {
	in := &dw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[dw.WriterComponent]("writer", "PATCH", "deliveries"), in)
}
func (x *execution) readSessions(ctx context.Context, in *sr.Input, lock bool) ([]*sr.Publication, error) {
	out, e := readGraph[sr.Output](ctx, x.invoke, graphTarget[sr.ReaderComponent]("reader", "GET", "sessions"), in, lock)
	if e != nil {
		return nil, e
	}
	return out.Data, nil
}
func (x *execution) writeSessions(ctx context.Context, rows []*sw.Publication) error {
	in := &sw.Input{}
	in.SetRows(rows)
	return writeGraph(ctx, x.invoke, graphTarget[sw.WriterComponent]("writer", "PATCH", "sessions"), in)
}
