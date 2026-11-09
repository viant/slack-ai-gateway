package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// MySQLDSN provisions a fresh, owned database for a storage contract test.
// AGENTLY_SLACK_MYSQL_TEST_DSN supplies an explicit administrative connection;
// its original database is never altered. The native client owns DELIMITER and
// stored-routine processing, exactly as in the Endly provisioning workflow.
func MySQLDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AGENTLY_SLACK_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set AGENTLY_SLACK_MYSQL_TEST_DSN for isolated MySQL storage tests")
	}
	mysql, err := exec.LookPath("mysql")
	Must(t, err)
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid MySQL test DSN")
	}
	client := []string{"[client]", "user=" + clientValue(cfg.User), "password=" + clientValue(cfg.Passwd)}
	switch cfg.Net {
	case "tcp":
		host, port, e := net.SplitHostPort(cfg.Addr)
		Must(t, e)
		client = append(client, "protocol=TCP", "host="+clientValue(host), "port="+port)
	case "unix":
		client = append(client, "protocol=SOCKET", "socket="+clientValue(cfg.Addr))
	default:
		t.Fatal("MySQL test DSN must use tcp or unix")
	}
	options := filepath.Join(t.TempDir(), "mysql.cnf")
	Must(t, os.WriteFile(options, []byte(strings.Join(client, "\n")+"\n"), 0600))
	invoke := func(sql string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, mysql, "--defaults-extra-file="+options, "--binary-mode")
		cmd.Stdin = strings.NewReader(sql)
		if output, e := cmd.CombinedOutput(); e != nil {
			return fmt.Errorf("MySQL fixture command failed: %w: %s", e, bytes.TrimSpace(output))
		}
		return nil
	}
	token := make([]byte, 8)
	_, err = rand.Read(token)
	Must(t, err)
	name := "asgt_" + hex.EncodeToString(token)
	// CREATE without IF NOT EXISTS proves ownership before registering DROP.
	Must(t, invoke("CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;"))
	t.Cleanup(func() { Must(t, invoke("DROP DATABASE `"+name+"`;")) })
	_, file, _, _ := runtime.Caller(0)
	ddl, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../script/mysql/schema_versioned.ddl"))
	Must(t, err)
	Must(t, invoke(strings.ReplaceAll(string(ddl), "`agently_steward_slack`", "`"+name+"`")))
	cfg.DBName = name
	cfg.ParseTime = true
	return cfg.FormatDSN()
}

func clientValue(value string) string {
	value = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\r", "\\r").Replace(value)
	return "\"" + value + "\""
}
