package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestStorageDiagnosticsPreserveClassificationWithoutSecretOutput(t *testing.T) {
	private := errors.New("SQL and private prompt and password")
	issue := &StorageError{Operation: "enqueue", IntegrationID: 4, TransactionState: "rolled_back", MySQLCode: 1213, SQLState: "40001", CompletionObserved: true, Attempts: 2, Cause: private}
	if !errors.Is(issue, private) {
		t.Fatal("native error chain lost")
	}
	var output bytes.Buffer
	LogStorageError(slog.New(slog.NewTextHandler(&output, nil)), issue)
	encoded, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{issue.Error(), output.String(), string(encoded)} {
		if strings.Contains(text, "password") || strings.Contains(text, "private prompt") {
			t.Fatalf("private diagnostic leaked: %s", text)
		}
	}
	if !strings.Contains(output.String(), "mysql_code=1213") || !strings.Contains(output.String(), "transaction_state=rolled_back") || !strings.Contains(output.String(), "attempts=2") {
		t.Fatal("safe native diagnostic missing")
	}
}
