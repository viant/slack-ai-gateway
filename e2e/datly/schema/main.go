// Command schema prepares a SQLite metadata fixture through the gateway's
// existing file and migration lifecycle. It does not duplicate the schema.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: go run ./e2e/datly/schema FILE")
	}
	ctx := context.Background()
	store, err := sqlite.Open(ctx, os.Args[1], true)
	if err != nil {
		return err
	}
	migrationErr := store.Migrate(ctx)
	closeErr := store.Close()
	return errors.Join(migrationErr, closeErr)
}
