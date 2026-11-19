package mysql

import (
	"context"
	"database/sql"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestSchema026ChecksExistingTablesWithoutVersionMetadata(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		name := "conforming_schema"
		if legacy {
			name = "obsolete_boolean_schema"
		}
		t.Run(name, func(t *testing.T) {
			dsn := fixtureDSN(t)
			cfg, err := driver.ParseDSN(dsn)
			storetest.Must(t, err)
			db, err := sql.Open("mysql", dsn)
			storetest.Must(t, err)
			t.Cleanup(func() { storetest.Must(t, db.Close()) })
			_, err = db.Exec("DELETE FROM schema_version")
			storetest.Must(t, err)
			if legacy {
				_, err = db.Exec("ALTER TABLE gateway_job DROP COLUMN backend_observation_uncertain_since, ADD COLUMN backend_observation_uncertain TINYINT NOT NULL DEFAULT 0")
				storetest.Must(t, err)
			}
			definition, err := os.ReadFile("../../../script/mysql/schema_versioned.ddl")
			storetest.Must(t, err)
			// The native client owns DELIMITER and routine processing, as in the
			// existing fixture and provisioning workflow. No schema parser is added.
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			args := []string{"--binary-mode", "--user=" + cfg.User}
			if cfg.Net == "unix" {
				args = append(args, "--protocol=SOCKET", "--socket="+cfg.Addr)
			} else {
				host, port, err := net.SplitHostPort(cfg.Addr)
				storetest.Must(t, err)
				args = append(args, "--protocol=TCP", "--host="+host, "--port="+port)
			}
			cmd := exec.CommandContext(ctx, "mysql", args...)
			cmd.Env = append(os.Environ(), "MYSQL_PWD="+cfg.Passwd)
			cmd.Stdin = strings.NewReader(strings.ReplaceAll(string(definition), "`slack_ai_gateway`", "`"+cfg.DBName+"`"))
			output, applyErr := cmd.CombinedOutput()
			if legacy {
				if applyErr == nil || !strings.Contains(string(output), "Incompatible gateway schema") {
					t.Fatalf("obsolete unversioned schema was accepted: err=%v output=%s", applyErr, output)
				}
				var versions, sinceColumns int
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM schema_version").Scan(&versions))
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gateway_job' AND column_name='backend_observation_uncertain_since'").Scan(&sinceColumns))
				if versions != 0 || sinceColumns != 0 {
					t.Fatal("failed validation changed the installed schema/version")
				}
				return
			}
			if applyErr != nil {
				t.Fatalf("conforming unversioned schema was rejected: %v %s", applyErr, output)
			}
			var version int
			storetest.Must(t, db.QueryRow("SELECT MAX(version_number) FROM schema_version").Scan(&version))
			if version != 2 {
				t.Fatalf("conforming schema version = %d, want 2", version)
			}
		})
	}
}
