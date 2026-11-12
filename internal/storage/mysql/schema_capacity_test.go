package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// Exercise physical capacity directly in an owned fixture. These deliberately
// non-business strings do not change the native writer's enum validation.
func TestSchemaCapacityWithDefaultRowFormat(t *testing.T) {
	_, db := fixtureDB(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	storetest.Must(t, err)
	defer conn.Close()
	var version, mode, defaultFormat string
	var pageSize int
	storetest.Must(t, conn.QueryRowContext(ctx, "SELECT VERSION(), @@session.sql_mode, @@innodb_default_row_format, @@innodb_page_size").Scan(&version, &mode, &defaultFormat, &pageSize))
	if !strings.Contains(mode, "STRICT_TRANS_TABLES") && !strings.Contains(mode, "STRICT_ALL_TABLES") {
		t.Fatalf("capacity checks require strict SQL mode, got %s", mode)
	}
	t.Logf("MySQL=%s default row format=%s page size=%d; session SQL mode=%s", version, defaultFormat, pageSize, mode)

	type column struct {
		name, kind, extra string
		length            sql.NullInt64
	}
	tables := []string{"integration", "conversation_binding", "gateway_job", "slack_outgoing_message", "slack_delivery_resolution", "slack_agent_session_publication"}
	columns := map[string][]column{}
	ids := map[string]int64{}
	values := map[string]map[string]any{}
	foreignKeys := map[string]map[string]string{
		"conversation_binding":            {"integration_id": "integration"},
		"gateway_job":                     {"integration_id": "integration", "conversation_binding_id": "conversation_binding"},
		"slack_outgoing_message":          {"job_id": "gateway_job"},
		"slack_delivery_resolution":       {"outgoing_message_id": "slack_outgoing_message"},
		"slack_agent_session_publication": {"initial_integration_id": "integration", "latest_status_job_id": "gateway_job", "status_source_job_id": "gateway_job"},
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, table := range tables {
		var format string
		storetest.Must(t, conn.QueryRowContext(ctx, "SELECT row_format FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", table).Scan(&format))
		if !strings.EqualFold(format, defaultFormat) {
			t.Fatalf("%s row format %s differs from server default %s", table, format, defaultFormat)
		}
		rows, err := conn.QueryContext(ctx, "SELECT column_name,data_type,character_maximum_length,extra FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY ordinal_position", table)
		storetest.Must(t, err)
		for rows.Next() {
			var c column
			storetest.Must(t, rows.Scan(&c.name, &c.kind, &c.length, &c.extra))
			if !strings.Contains(c.extra, "auto_increment") {
				columns[table] = append(columns[table], c)
			}
		}
		storetest.Must(t, rows.Err())
		storetest.Must(t, rows.Close())
		if len(columns[table]) == 0 {
			t.Fatalf("missing table %s", table)
		}
		values[table] = map[string]any{}
		var names, placeholders []string
		var args []any
		for _, c := range columns[table] {
			var value any
			switch c.kind {
			case "varchar":
				value = strings.Repeat("😀", int(c.length.Int64))
			case "text", "mediumtext", "longtext":
				value = "capacity fixture 😀"
			case "datetime":
				value = now
			case "tinyint", "int", "bigint":
				value = 1
			default:
				t.Fatalf("unhandled capacity type %s.%s: %s", table, c.name, c.kind)
			}
			if parent := foreignKeys[table][c.name]; parent != "" {
				value = ids[parent]
			}
			values[table][c.name] = value
			names = append(names, "`"+c.name+"`")
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		result, err := conn.ExecContext(ctx, "INSERT INTO `"+table+"` ("+strings.Join(names, ",")+") VALUES ("+strings.Join(placeholders, ",")+")", args...)
		storetest.Must(t, err)
		ids[table], err = result.LastInsertId()
		storetest.Must(t, err)
		for _, c := range columns[table] {
			if c.kind != "varchar" {
				continue
			}
			var actual string
			var chars, octets int64
			storetest.Must(t, conn.QueryRowContext(ctx, "SELECT `"+c.name+"`,CHAR_LENGTH(`"+c.name+"`),OCTET_LENGTH(`"+c.name+"`) FROM `"+table+"` WHERE id=?", ids[table]).Scan(&actual, &chars, &octets))
			if actual != values[table][c.name] || chars != c.length.Int64 || octets != 4*c.length.Int64 {
				t.Fatalf("%s.%s did not preserve maximum four-byte Unicode capacity", table, c.name)
			}
		}
	}

	expanded := map[string]map[string]int{
		"integration":                     {"backend_type": 128},
		"conversation_binding":            {"slack_thread_root_message_id": 255, "gateway_access_mode": 128, "gateway_binding_state": 128},
		"gateway_job":                     {"gateway_request_id": 255, "slack_conversation_type": 128, "slack_thread_root_message_id": 255, "slack_prompt_message_id": 255, "gateway_execution_state": 128, "gateway_execution_outcome": 128, "slack_progress_mode": 128, "slack_progress_message_id": 255, "slack_progress_state": 128},
		"slack_outgoing_message":          {"slack_message_kind": 128, "slack_delivery_state": 128, "slack_message_id": 255, "slack_message_format": 128},
		"slack_delivery_resolution":       {"resolution_action": 128, "slack_message_id": 255},
		"slack_agent_session_publication": {"slack_thread_root_message_id": 255, "slack_agent_desired_status": 128, "slack_agent_observed_status": 128, "slack_session_observed_status": 128, "slack_publication_state": 128},
	}
	for _, table := range tables {
		for _, c := range columns[table] {
			length, ok := expanded[table][c.name]
			if !ok {
				continue
			}
			t.Run(table+"/"+c.name, func(t *testing.T) {
				if !c.length.Valid || int(c.length.Int64) != length {
					t.Fatalf("declared capacity %v, expected %d", c.length, length)
				}
				update := "UPDATE `" + table + "` SET `" + c.name + "`=? WHERE id=?"
				// Explicitly cross the previous 32/64 limits before the new limit.
				for _, size := range []int{33, 65, length} {
					_, err := conn.ExecContext(ctx, update, strings.Repeat("x", size), ids[table])
					storetest.Must(t, err)
				}
				_, err := conn.ExecContext(ctx, update, strings.Repeat("x", length+1), ids[table])
				requireMySQLError(t, err, 1406)
				var actual string
				storetest.Must(t, conn.QueryRowContext(ctx, "SELECT `"+c.name+"` FROM `"+table+"` WHERE id=?", ids[table]).Scan(&actual))
				if actual != strings.Repeat("x", length) {
					t.Fatal("failed overflow write modified or truncated the stored value")
				}
				_, err = conn.ExecContext(ctx, update, values[table][c.name], ids[table])
				storetest.Must(t, err)
			})
		}
	}

	t.Run("full_unique_identifiers", func(t *testing.T) {
		var names, parameters []string
		for _, c := range columns["gateway_job"] {
			names = append(names, "`"+c.name+"`")
			parameters = append(parameters, "?")
		}
		insert := "INSERT INTO gateway_job (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(parameters, ",") + ")"
		for _, suffix := range []string{"A", "B", "a", " "} {
			identifier := strings.Repeat("q", 254) + suffix
			var args []any
			for _, c := range columns["gateway_job"] {
				value := values["gateway_job"][c.name]
				if c.name == "gateway_request_id" || c.name == "slack_event_id" {
					value = identifier
				}
				args = append(args, value)
			}
			result, err := conn.ExecContext(ctx, insert, args...)
			storetest.Must(t, err)
			id, err := result.LastInsertId()
			storetest.Must(t, err)
			var actual string
			storetest.Must(t, conn.QueryRowContext(ctx, "SELECT gateway_request_id FROM gateway_job WHERE id=?", id).Scan(&actual))
			if actual != identifier {
				t.Fatalf("identifier suffix %q was not preserved", suffix)
			}
			_, err = conn.ExecContext(ctx, insert, args...)
			requireMySQLError(t, err, 1062)
		}
	})
	t.Run("foreign_key_still_enforced", func(t *testing.T) {
		_, err := conn.ExecContext(ctx, "UPDATE gateway_job SET conversation_binding_id=-1 WHERE id=?", ids["gateway_job"])
		requireMySQLError(t, err, 1452)
	})
}

func requireMySQLError(t *testing.T, err error, number uint16) {
	t.Helper()
	var native *driver.MySQLError
	if !errors.As(err, &native) || native.Number != number {
		t.Fatalf("expected MySQL error %d, got %v", number, err)
	}
}
