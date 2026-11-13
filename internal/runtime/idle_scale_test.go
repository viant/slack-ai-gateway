//go:build darwin || linux

package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// TestIdleScale is an opt-in process measurement, not a timing assertion in the
// ordinary suite. It runs the native shared database and runtime group while
// every external adapter remains local. MySQL fixtures own fresh databases.
// Use a separately compiled test binary: go test's build CPU is not measured.
func TestIdleScale(t *testing.T) {
	if os.Getenv("SLACK_AI_GATEWAY_IDLE_SCALE") != "1" {
		t.Skip("set SLACK_AI_GATEWAY_IDLE_SCALE=1 for isolated idle measurements")
	}
	driver := os.Getenv("IDLE_SCALE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}
	counts := []int{2, 10, 50}
	if value := os.Getenv("IDLE_SCALE_COUNT"); value != "" {
		n, err := strconv.Atoi(value)
		storetest.Must(t, err)
		if n <= 0 || n > 100 {
			t.Fatal("IDLE_SCALE_COUNT must be between 1 and 100")
		}
		counts = []int{n}
	}
	warm := idleScaleDuration(t, "IDLE_SCALE_WARMUP", 30*time.Second)
	window := idleScaleDuration(t, "IDLE_SCALE_WINDOW", time.Minute)
	windows := idleScaleInt(t, "IDLE_SCALE_WINDOWS", 3)
	history := idleScaleInt(t, "IDLE_SCALE_HISTORY", 1)
	for _, count := range counts {
		t.Run(fmt.Sprintf("%s_%d_history_%d", driver, count, history), func(t *testing.T) {
			idleScaleMeasure(t, driver, count, history, warm, window, windows)
		})
	}
}

func idleScaleDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		d, err := time.ParseDuration(value)
		storetest.Must(t, err)
		if d <= 0 {
			t.Fatalf("%s must be positive", name)
		}
		return d
	}
	return fallback
}

func idleScaleInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		n, err := strconv.Atoi(value)
		storetest.Must(t, err)
		if n < 0 || n > 1000 || (name != "IDLE_SCALE_HISTORY" && n == 0) {
			t.Fatalf("invalid %s", name)
		}
		return n
	}
	return fallback
}

func idleScaleCPU(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	storetest.Must(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second +
		time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

type idleScaleSample struct {
	Driver       string   `json:"driver"`
	Integrations int      `json:"integrations"`
	History      int      `json:"settled_jobs_per_integration"`
	Window       int      `json:"window"`
	Elapsed      float64  `json:"elapsed_seconds"`
	CPU          float64  `json:"cpu_percent_one_core"`
	SQL          *int64   `json:"sql_statements,omitempty"`
	QPS          *float64 `json:"sql_statements_per_second,omitempty"`
	BackendCalls int32    `json:"backend_calls"`
	SlackWrites  int64    `json:"fake_slack_writes"`
}

type idleScaleSender struct{ writes atomic.Int64 }

func (s *idleScaleSender) Send(context.Context, gateway.SlackOutgoingMessage) (string, error) {
	s.writes.Add(1)
	return "2.000001", nil
}

func (s *idleScaleSender) SendStatus(_ context.Context, p gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	s.writes.Add(1)
	if p.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: p.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}

func idleScaleMeasure(t *testing.T, driver string, count, history int, warm, window time.Duration, windows int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var root storage.Store
	var sqlCount func() map[uint64]uint64
	switch driver {
	case "sqlite":
		s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "idle.sqlite"), true)
		storetest.Must(t, err)
		storetest.Must(t, s.Migrate(ctx))
		root = s
	case "mysql":
		dsn := storetest.MySQLDSN(t)
		s, err := mysql.Open(ctx, dsn, nil)
		storetest.Must(t, err)
		root = s
		cfg, err := mysqldriver.ParseDSN(dsn)
		storetest.Must(t, err)
		database := cfg.DBName
		cfg.DBName = "" // Monitoring is never attributed to the owned schema.
		monitor, err := sql.Open("mysql", cfg.FormatDSN())
		storetest.Must(t, err)
		t.Cleanup(func() { storetest.Must(t, monitor.Close()) })
		sqlCount = func() map[uint64]uint64 {
			// Native prepared SELECTs increment Questions and COM_EXECUTE,
			// but not this server's schema digest or SQL/select counters.
			rows, err := monitor.QueryContext(ctx, `SELECT t.THREAD_ID,CAST(v.VARIABLE_VALUE AS UNSIGNED)
FROM performance_schema.threads t JOIN performance_schema.status_by_thread v USING(THREAD_ID)
WHERE t.PROCESSLIST_DB=? AND v.VARIABLE_NAME='Questions'`, database)
			storetest.Must(t, err)
			defer rows.Close()
			result := make(map[uint64]uint64)
			for rows.Next() {
				var id, questions uint64
				storetest.Must(t, rows.Scan(&id, &questions))
				result[id] = questions
			}
			storetest.Must(t, rows.Err())
			if len(result) == 0 {
				t.Fatal("native database connections are not observable")
			}
			return result
		}
	default:
		t.Fatal("IDLE_SCALE_DRIVER must be sqlite or mysql")
	}
	t.Cleanup(func() { storetest.Must(t, root.Close()) })
	identities := make([]storage.Integration, count)
	for n := range identities {
		identities[n] = storetest.DefaultIntegration()
		identities[n].Name = fmt.Sprintf("idle-%d", n)
		identities[n].AppID = fmt.Sprintf("A%d", n)
	}
	assignments, err := root.ReconcileIntegrations(ctx, identities)
	storetest.Must(t, err)
	storetest.Must(t, root.Recover(ctx))
	ready := make(chan struct{}, count)
	sender := &idleScaleSender{}
	backends := make([]*backend, 0, count)
	group := runner.Group{Store: root, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for n, assignment := range assignments {
		scope := root.Scope(assignment.ID)
		for h := range history {
			idleScaleHistory(t, scope, assignment.AppID, fmt.Sprintf("history-%d-%d", n, h))
		}
		b := &backend{}
		backends = append(backends, b)
		r := &runner.Runtime{Store: scope, Clients: b, AgentID: "steward",
			Route:  storetest.Route(assignment.AppID, "T", "C"),
			Policy: storetest.Policy(assignment.AppID, "T", "shared", "C"),
			Source: sourceFunc(func(ctx context.Context, _ gateway.EventSink, signal func()) error {
				signal()
				ready <- struct{}{}
				<-ctx.Done()
				return nil
			}),
			Sender: sender,
			Log:    group.Log}
		group.Integrations = append(group.Integrations, runner.Integration{ID: assignment.Name, Runtime: r})
	}
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = group.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			storetest.Must(t, runErr)
		case <-time.After(30 * time.Second):
			t.Error("idle runtime group did not join")
		}
	})
	wait := func(duration time.Duration) {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-done:
			t.Fatalf("runtime stopped during idle measurement: %v", runErr)
		case <-timer.C:
		}
		select {
		case <-done:
			t.Fatalf("runtime stopped during idle measurement: %v", runErr)
		default:
		}
	}
	for range count {
		select {
		case <-ready:
		case <-done:
			t.Fatalf("runtime stopped before readiness: %v", runErr)
		case <-time.After(30 * time.Second):
			t.Fatal("idle runtimes did not become ready")
		}
	}
	// Optional process barrier keeps concurrent measurements out of one
	// another's cold schema, fixture and runtime startup work.
	if dir := os.Getenv("IDLE_SCALE_BARRIER_DIR"); dir != "" {
		expected := idleScaleInt(t, "IDLE_SCALE_BARRIER_COUNT", 1)
		name := fmt.Sprintf("%s-%d-%d.ready", driver, count, history)
		storetest.Must(t, os.WriteFile(filepath.Join(dir, name), []byte("ready\n"), 0600))
		deadline := time.Now().Add(10 * time.Minute)
		for {
			files, err := filepath.Glob(filepath.Join(dir, "*.ready"))
			storetest.Must(t, err)
			released := true
			if release := os.Getenv("IDLE_SCALE_BARRIER_RELEASE"); release != "" {
				_, err := os.Stat(release)
				released = err == nil
			}
			if len(files) == expected && released {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("isolated measurement processes did not reach startup barrier")
			}
			wait(100 * time.Millisecond)
		}
	}
	wait(warm)
	for n := range windows {
		var beforeSQL map[uint64]uint64
		if sqlCount != nil {
			beforeSQL = sqlCount()
		}
		beforeCPU, start := idleScaleCPU(t), time.Now()
		wait(window)
		elapsed, used := time.Since(start), idleScaleCPU(t)-beforeCPU
		sample := idleScaleSample{Driver: driver, Integrations: count, History: history, Window: n + 1,
			Elapsed: elapsed.Seconds(), CPU: 100 * float64(used) / float64(elapsed), SlackWrites: sender.writes.Load()}
		if sqlCount != nil {
			statements := idleScaleSQLDelta(t, beforeSQL, sqlCount())
			qps := float64(statements) / elapsed.Seconds()
			sample.SQL, sample.QPS = &statements, &qps
		}
		for _, b := range backends {
			sample.BackendCalls += b.queries.Load()
		}
		encoded, err := json.Marshal(sample)
		storetest.Must(t, err)
		t.Log("IDLE_SCALE " + string(encoded))
		if sample.BackendCalls != 0 || sample.SlackWrites != 0 {
			t.Error("settled idle fixture produced external work")
		}
		if os.Getenv("IDLE_SCALE_ENFORCE") == "1" {
			if sample.CPU > 5 {
				t.Errorf("idle CPU %.3f%% exceeds 5%% of one core", sample.CPU)
			}
			if sample.QPS != nil && *sample.QPS > 1 {
				t.Errorf("idle SQL %.3f statements/s exceeds 1", *sample.QPS)
			}
		}
	}
}

func idleScaleSQLDelta(t *testing.T, before, after map[uint64]uint64) int64 {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("database thread set changed; SQL measurement is invalid")
	}
	var total int64
	for id, previous := range before {
		next, ok := after[id]
		if !ok || next < previous {
			t.Fatal("database thread changed or counter decreased; SQL measurement is invalid")
		}
		total += int64(next - previous)
	}
	return total
}

func idleScaleHistory(t *testing.T, s storage.Store, app, event string) {
	t.Helper()
	ctx := context.Background()
	m := storetest.Message(event, event, "alice")
	m.AppID, m.StatusMode = app, gateway.StatusMessage
	inserted, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("settled fixture event was not inserted")
	}
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if j == nil {
		t.Fatal("settled fixture job was not claimed")
	}
	storetest.Must(t, s.Complete(ctx, j.ID, "", "settled", []gateway.ReplyPart{{Text: "settled", Format: gateway.ReplyPlain}}))
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil {
		t.Fatal("settled fixture delivery was not claimed")
	}
	storetest.Must(t, s.Delivered(ctx, d.ID, "1.000001"))
	for range 4 {
		p, err := s.ClaimStatus(ctx, time.Now(), "urgent")
		storetest.Must(t, err)
		if p == nil {
			return
		}
		storetest.Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", SlackTS: "1.000002", Acknowledged: true}, time.Now()))
	}
	t.Fatal("settled fixture statuses did not settle")
}
