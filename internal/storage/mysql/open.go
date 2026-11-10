// Package mysql configures the shared Datly store for a provisioned MySQL database.
package mysql

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afsc/aws"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/scy"
	_ "github.com/viant/scy/kms/blowfish"
	"github.com/viant/slack-ai-gateway/internal/storage/datly"
)

// Store uses the same graphs and operations as the SQLite store.
type Store = datly.Store

func Open(ctx context.Context, dsn string, secret *scy.Resource) (store *Store, err error) {
	// Native cipher implementations may panic on malformed ciphertext. Keep
	// secret-bearing provider and driver diagnostics inside this boundary.
	defer func() {
		if recover() != nil {
			store, err = nil, fmt.Errorf("cannot decode MySQL credentials")
		}
	}()
	return datly.Open(ctx, connector.Config{
		Name: "slack", Driver: "mysql", DSN: dsn, Secret: secret,
		MaxIdleConns: 4, ConnMaxLifetimeMs: 300000,
	})
}
