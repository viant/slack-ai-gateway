// Package datly implements gateway persistence through shared embedded graphs.
// The caller prepares the schema and configures the database connector.
package datly

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storage"
	br "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/bindings/read"
	bw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/bindings/write"
	dr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/read"
	dw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/write"
	opc "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/operations/check"
	opn "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/operations/notbefore"
	ops "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/operations/status"
	jr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/read"
	sr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/sessions/read"
	sw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/sessions/write"
	jw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/write"
	"github.com/viant/agently-slack-gateway/internal/storage/datly/workflow"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/sqlx/io/errx"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/xdatly/handler"
)

type Store struct {
	server        *standalone.Server
	statusMu      sync.Mutex
	preferMessage bool
}

var _ storage.Store = (*Store)(nil)

func Open(ctx context.Context, connection connector.Config) (*Store, error) {
	server, err := standalone.New(ctx, standalone.Options{RequireLinked: true, Holders: []any{&workflow.Component{}, &br.ReaderComponent{}, &bw.WriterComponent{}, &dr.ReaderComponent{}, &dw.WriterComponent{}, &jr.ReaderComponent{}, &jw.WriterComponent{}, &sr.ReaderComponent{}, &sw.WriterComponent{}, &ops.ReaderComponent{}, &opc.ReaderComponent{}, &opn.ReaderComponent{}}, Config: &config.Config{
		Connector: "slack", Connectors: []connector.Config{connection},
		GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/agently-slack-gateway/internal/storage/datly/..."}},
	}})
	if err != nil {
		return nil, fmt.Errorf("cannot open gateway storage")
	}
	if err = server.Reload(ctx, 1); err != nil {
		_ = server.Shutdown(context.Background())
		return nil, fmt.Errorf("cannot initialize gateway storage runtime")
	}
	return &Store{server: server}, nil
}
func (s *Store) Close() error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(context.Background())
}

var target = dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[workflow.Component]().PkgPath(), Name: "Workflow"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/slack/storage"}}

func (s *Store) call(ctx context.Context, in *workflow.Input) (*workflow.Output, error) {
	if s == nil || s.server == nil {
		return nil, fmt.Errorf("gateway storage is not initialized")
	}
	value, err := s.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in})
	if err != nil {
		return nil, err
	}
	out, ok := value.(*workflow.Output)
	if !ok || out == nil {
		return nil, fmt.Errorf("unexpected storage workflow output %T", value)
	}
	return out, nil
}
func (s *Store) apply(ctx context.Context, in *workflow.Input) error {
	_, err := s.call(ctx, in)
	return err
}
func (s *Store) Enqueue(ctx context.Context, m gateway.Message) (bool, error) {
	if s == nil || s.server == nil {
		return false, fmt.Errorf("gateway storage is not initialized")
	}
	// Observe the native root's actual completion before adopting a concurrent
	// duplicate. This never replays a mutation or accepts an uncertain outcome.
	var outcome handler.Outcome
	value, err := s.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target,
		Input:      &workflow.Input{Action: "enqueue", Message: m},
		Completion: func(report handler.Outcome) { outcome = report },
	})
	if err == nil {
		output, ok := value.(*workflow.Output)
		if !ok || output == nil {
			return false, fmt.Errorf("unexpected storage intake output %T", value)
		}
		return output.Inserted, nil
	}
	if !errx.IsDuplicateKey(err) || outcome.State() != handler.TransactionRolledBack {
		return false, err
	}
	existing, lookupErr := s.call(ctx, &workflow.Input{Action: "existing_event", Message: m})
	if lookupErr != nil || existing.Job == nil {
		return false, err
	}
	return false, nil
}

func (s *Store) GetBinding(ctx context.Context, k gateway.ThreadKey) (*gateway.Binding, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "get_binding", Key: k})
	if e != nil {
		return nil, e
	}
	return o.Binding, nil
}
func (s *Store) ClaimJob(ctx context.Context) (*gateway.Job, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "claim_job"})
	if e != nil {
		return nil, e
	}
	return o.Job, nil
}
func (s *Store) BindJob(ctx context.Context, id int64, d gateway.Decision) (*gateway.Binding, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "bind_job", ID: id, Decision: d})
	if e != nil {
		return nil, e
	}
	return o.Binding, nil
}
func (s *Store) GetDeliveryContext(ctx context.Context, d gateway.Delivery) (*gateway.DeliveryContext, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "delivery_context", Delivery: d})
	if e != nil {
		return nil, e
	}
	return o.Context, nil
}
func (s *Store) ClaimDelivery(ctx context.Context) (*gateway.Delivery, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "claim_delivery"})
	if e != nil {
		return nil, e
	}
	return o.Delivery, nil
}
func (s *Store) DeliveryNotBefore(ctx context.Context, w string) (time.Time, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "not_before", Key: gateway.ThreadKey{WorkspaceID: w}})
	if e != nil {
		return time.Time{}, e
	}
	return o.NotBefore, nil
}
func (s *Store) ClaimStatus(ctx context.Context, at time.Time, kind string) (*gateway.RequestStatus, error) {
	if s == nil || s.server == nil {
		return nil, fmt.Errorf("gateway storage is not initialized")
	}
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	o, e := s.call(ctx, &workflow.Input{Action: "claim_status", At: at, Kind: kind, PreferMessage: s.preferMessage})
	if e != nil {
		return nil, e
	}
	if o.Status != nil {
		s.preferMessage = o.Status.Target == gateway.StatusTargetAgentSession
	}
	return o.Status, nil
}
func (s *Store) GetStatusContext(ctx context.Context, v gateway.RequestStatus) (*gateway.DeliveryContext, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "status_context", Status: v})
	if e != nil {
		return nil, e
	}
	return o.Context, nil
}
func (s *Store) Status(ctx context.Context, limit int) (*storage.Status, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "status", Limit: limit})
	if e != nil {
		return nil, e
	}
	return o.Snapshot, nil
}
func (s *Store) CheckRunBindings(ctx context.Context, r gateway.Route, p string) error {
	return s.apply(ctx, &workflow.Input{Action: "check_bindings", Route: r, Principal: p})
}
func (s *Store) ResolveDelivery(ctx context.Context, id int64, action, ts string, risk bool) error {
	return s.apply(ctx, &workflow.Input{Action: "resolve_delivery", ID: id, Kind: action, Text: ts, AcceptDuplicateRisk: risk})
}
func (s *Store) Complete(ctx context.Context, id int64, turn, result string, parts []gateway.ReplyPart) error {
	return s.apply(ctx, &workflow.Input{Action: "complete", ID: id, TurnID: turn, Text: result, Parts: parts})
}
func (s *Store) RecordObservation(ctx context.Context, id int64, state string, at time.Time) error {
	return s.apply(ctx, &workflow.Input{Action: "observe", ID: id, Text: state, At: at})
}
func (s *Store) FinishStatus(ctx context.Context, v gateway.RequestStatus, r gateway.StatusResult, at time.Time) error {
	return s.apply(ctx, &workflow.Input{Action: "finish_status", Status: v, StatusResult: r, At: at})
}
func (s *Store) BeginCreate(ctx context.Context, id int64) error {
	return s.apply(ctx, &workflow.Input{Action: "begin_create", ID: id})
}
func (s *Store) SetConversation(ctx context.Context, id int64, text string) error {
	return s.apply(ctx, &workflow.Input{Action: "set_conversation", ID: id, Text: text})
}
func (s *Store) BeginQuery(ctx context.Context, id int64) error {
	return s.apply(ctx, &workflow.Input{Action: "begin_query", ID: id})
}
func (s *Store) SchedulePoll(ctx context.Context, id int64, at time.Time) error {
	return s.apply(ctx, &workflow.Input{Action: "schedule_poll", ID: id, At: at})
}
func (s *Store) Review(ctx context.Context, id int64, text string) error {
	return s.apply(ctx, &workflow.Input{Action: "review", ID: id, Text: text})
}
func (s *Store) Reject(ctx context.Context, id int64, text string) error {
	return s.apply(ctx, &workflow.Input{Action: "reject", ID: id, Text: text})
}
func (s *Store) Delivered(ctx context.Context, id int64, text string) error {
	return s.apply(ctx, &workflow.Input{Action: "delivered", ID: id, Text: text})
}
func (s *Store) DeliveryUnknown(ctx context.Context, id int64) error {
	return s.apply(ctx, &workflow.Input{Action: "delivery_unknown", ID: id})
}
func (s *Store) RetryDelivery(ctx context.Context, id int64, at time.Time) error {
	return s.apply(ctx, &workflow.Input{Action: "retry_delivery", ID: id, At: at})
}
func (s *Store) FailDelivery(ctx context.Context, id int64) error {
	return s.apply(ctx, &workflow.Input{Action: "fail_delivery", ID: id})
}
func (s *Store) Recover(ctx context.Context) error {
	return s.apply(ctx, &workflow.Input{Action: "recover"})
}
