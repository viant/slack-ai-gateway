// Package datly implements gateway persistence through shared embedded graphs.
// The caller prepares the schema and configures the database connector.
package datly

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	dr "github.com/viant/slack-ai-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/slack-ai-gateway/internal/datly/deliveries/write"
	opc "github.com/viant/slack-ai-gateway/internal/datly/operations/check"
	opn "github.com/viant/slack-ai-gateway/internal/datly/operations/notbefore"
	ops "github.com/viant/slack-ai-gateway/internal/datly/operations/status"
	opw "github.com/viant/slack-ai-gateway/internal/datly/operations/work"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	er "github.com/viant/slack-ai-gateway/internal/datly/read/event"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	tgw "github.com/viant/slack-ai-gateway/internal/datly/topology/integrations/write"
	tr "github.com/viant/slack-ai-gateway/internal/datly/topology/read"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
	"github.com/viant/sqlx/io/errx"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/xdatly/handler"
)

type Store struct {
	server        *standalone.Server
	scope         int64
	scopeName     string
	root          *Store
	statusMu      sync.Mutex
	preferMessage bool
	work          *workState
}

var _ storage.Store = (*Store)(nil)

func Open(ctx context.Context, connection connector.Config) (*Store, error) {
	server, err := standalone.New(ctx, standalone.Options{RequireLinked: true, Holders: []any{&workflow.Component{}, &br.ReaderComponent{}, &bw.WriterComponent{}, &dr.ReaderComponent{}, &dw.WriterComponent{}, &jr.ReaderComponent{}, &er.ReaderComponent{}, &jw.WriterComponent{}, &sr.ReaderComponent{}, &sw.WriterComponent{}, &ops.ReaderComponent{}, &opc.ReaderComponent{}, &opn.ReaderComponent{}, &opw.ReaderComponent{}, &tr.ReaderComponent{}, &tgw.WriterComponent{}}, Config: &config.Config{
		Connector: "slack", Connectors: []connector.Config{connection},
		GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{
			"github.com/viant/slack-ai-gateway/internal/datly/...",
			"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow",
		}},
	}})
	if err != nil {
		return nil, fmt.Errorf("cannot open gateway storage")
	}
	if err = server.Reload(ctx, 1); err != nil {
		_ = server.Shutdown(context.Background())
		return nil, fmt.Errorf("cannot initialize gateway storage runtime")
	}
	return &Store{server: server, work: newWorkState()}, nil
}
func (s *Store) Close() error {
	if s != nil && s.root != nil {
		return nil // The owner of the shared runtime closes it once.
	}
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
	if !in.At.IsZero() {
		in.At = in.At.UTC().Truncate(time.Millisecond)
	}
	in.Scope = s.scope
	in.IntegrationName = s.scopeName
	value, err := s.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in})
	if err != nil {
		return nil, err
	}
	out, ok := value.(*workflow.Output)
	if !ok || out == nil {
		return nil, fmt.Errorf("unexpected storage workflow output %T", value)
	}
	s.workChanged(out)
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
		Input:      &workflow.Input{Action: "enqueue", Message: m, Scope: s.scope},
		Completion: func(report handler.Outcome) { outcome = report },
	})
	if err == nil {
		output, ok := value.(*workflow.Output)
		if !ok || output == nil {
			return false, fmt.Errorf("unexpected storage intake output %T", value)
		}
		s.workChanged(output)
		return output.Inserted, nil
	}
	if outcome.State() != handler.TransactionRolledBack {
		return false, err
	}
	duplicate := errx.IsDuplicateKey(err)
	var validation *handler.Validation
	if !duplicate && errors.As(err, &validation) && len(validation.Violations) == 1 {
		violation := validation.Violations[0]
		// A concurrent insert can become visible to the native unique
		// validator before it reaches SQL. Consume that exact checked event
		// fact, without treating other validation failures as duplicates.
		if violation != nil && violation.Field == "SlackEventId" && violation.Check == "unique" && violation.HasCheckedValue {
			eventID, ok := violation.CheckedValue.(string)
			duplicate = ok && eventID == m.EventID
		}
	}
	if !duplicate {
		return false, err
	}
	existing, lookupErr := s.call(ctx, &workflow.Input{Action: "existing_event", Message: m})
	if lookupErr != nil || !existing.Duplicate {
		return false, err
	}
	return false, nil
}

func (s *Store) GetBinding(ctx context.Context, k gateway.ThreadKey) (*gateway.ConversationBinding, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "get_binding", Key: k})
	if e != nil {
		return nil, e
	}
	return o.ConversationBinding, nil
}
func (s *Store) ClaimJob(ctx context.Context) (*gateway.GatewayJob, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "claim_job"})
	if e != nil {
		return nil, e
	}
	return o.GatewayJob, nil
}
func (s *Store) BindJob(ctx context.Context, id int64, d gateway.Decision) (*gateway.ConversationBinding, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "bind_job", ID: id, Decision: d})
	if e != nil {
		return nil, e
	}
	return o.ConversationBinding, nil
}
func (s *Store) GetDeliveryContext(ctx context.Context, d gateway.SlackOutgoingMessage) (*gateway.DeliveryContext, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "delivery_context", SlackOutgoingMessage: d})
	if e != nil {
		return nil, e
	}
	return o.Context, nil
}
func (s *Store) ClaimDelivery(ctx context.Context) (*gateway.SlackOutgoingMessage, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "claim_delivery"})
	if e != nil {
		return nil, e
	}
	return o.SlackOutgoingMessage, nil
}
func (s *Store) DeliveryNotBefore(ctx context.Context, w string) (time.Time, error) {
	o, e := s.call(ctx, &workflow.Input{Action: "not_before", Key: gateway.ThreadKey{WorkspaceID: w}})
	if e != nil {
		return time.Time{}, e
	}
	return o.NotBefore, nil
}
func (s *Store) ClaimStatus(ctx context.Context, at time.Time, kind string) (*gateway.GatewayJobProgress, error) {
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
func (s *Store) GetStatusContext(ctx context.Context, v gateway.GatewayJobProgress) (*gateway.DeliveryContext, error) {
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
func (s *Store) Complete(ctx context.Context, id int64, requestID, result string, parts []gateway.ReplyPart, protocol ...gateway.ProtocolSnapshot) error {
	return s.apply(ctx, &workflow.Input{Action: "complete", ID: id, GatewayRequestID: requestID, Text: result, Parts: parts, Protocol: protocol})
}
func (s *Store) FailUnstarted(ctx context.Context, reason string) error {
	return s.apply(ctx, &workflow.Input{Action: "fail_unstarted", Text: reason})
}
func (s *Store) FailCleanup(ctx context.Context, reason string) error {
	return s.apply(ctx, &workflow.Input{Action: "fail_cleanup", Text: reason})
}
func (s *Store) RecordObservation(ctx context.Context, id int64, state string, at time.Time) error {
	return s.apply(ctx, &workflow.Input{Action: "observe", ID: id, Text: state, At: at})
}
func (s *Store) FinishStatus(ctx context.Context, v gateway.GatewayJobProgress, r gateway.StatusResult, at time.Time) error {
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
