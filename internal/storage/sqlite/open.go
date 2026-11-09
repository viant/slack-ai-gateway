package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	_ "modernc.org/sqlite"
)

type Store struct {
	db                  *sql.DB
	lock                *os.File
	created             bool
	statusMu            sync.Mutex
	preferMessageStatus bool
}

var _ gateway.Store = (*Store)(nil)

// Open only accepts an explicit filesystem path, never a URI or memory DSN.
// A lifetime lock enforces the single-active-instance deployment model.
func Open(ctx context.Context, path string, create bool) (*Store, error) {
	if strings.TrimSpace(path) == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, fmt.Errorf("SQLite requires an explicit durable file path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if info, statErr := os.Lstat(abs); statErr == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("SQLite path must be a regular file")
		}
	} else if !os.IsNotExist(statErr) || !create {
		return nil, statErr
	}
	lock, err := acquireLock(abs + ".lock")
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = lock.Close()
		}
	}()
	var f *os.File
	created := false
	if create {
		// Record creation atomically while holding the lifetime lock. An
		// existing file must never be mistaken for a newly initialized database.
		f, err = os.OpenFile(abs, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		created = err == nil
	}
	if !create || os.IsExist(err) {
		f, err = os.OpenFile(abs, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	u := &url.URL{Scheme: "file", Path: abs}
	q := url.Values{"_pragma": {"busy_timeout(5000)", "foreign_keys(ON)", "journal_mode(WAL)"}, "_txlock": {"immediate"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// SQLite writes are serialized locally; external API work never holds this
	// connection, so queries in distinct threads can still execute concurrently.
	db.SetMaxOpenConns(1)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	failed = false
	return &Store{db: db, lock: lock, created: created}, nil
}

// Created reports whether this Open call created the database file. It does not
// report whether a schema is present; callers still use Migrate or CheckSchema.
func (s *Store) Created() bool { return s.created }

func (s *Store) Close() error {
	err := s.db.Close()
	lockErr := s.lock.Close()
	if err != nil {
		return err
	}
	return lockErr
}
