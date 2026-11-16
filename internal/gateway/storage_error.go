package gateway

import (
	"errors"
	"log/slog"
)

// StorageError preserves native errors for errors.Is/As while its printable
// representation and diagnostic attributes contain no SQL, values or secrets.
type StorageError struct {
	Operation          string
	IntegrationID      int64
	TransactionState   string
	MySQLCode          uint16
	SQLState           string
	CompletionObserved bool
	WaitMillis         int64
	ExecutionMillis    int64
	Cause              error `json:"-"`
}

func (*StorageError) Error() string   { return "gateway storage operation failed" }
func (e *StorageError) Unwrap() error { return e.Cause }

func LogStorageError(log *slog.Logger, err error) {
	var issue *StorageError
	if !errors.As(err, &issue) || log == nil {
		return
	}
	log.Error("storage_operation_failed", "operation", issue.Operation, "integration_id", issue.IntegrationID,
		"transaction_state", issue.TransactionState, "completion_observed", issue.CompletionObserved,
		"mysql_code", issue.MySQLCode, "sqlstate", issue.SQLState, "wait_ms", issue.WaitMillis, "execution_ms", issue.ExecutionMillis)
}
