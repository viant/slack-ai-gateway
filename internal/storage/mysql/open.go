// Package mysql configures the shared Datly store for a provisioned MySQL database.
package mysql

import (
	"context"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/agently-slack-gateway/internal/storage/datly"
	"github.com/viant/datly/bootstrap/connector"
)

// Store uses the same graphs and operations as the SQLite store.
type Store = datly.Store

func Open(ctx context.Context, dsn string) (*Store, error) {
	return datly.Open(ctx, connector.Config{
		Name: "slack", Driver: "mysql", DSN: dsn,
		MaxIdleConns: 4, ConnMaxLifetimeMs: 300000,
	})
}
