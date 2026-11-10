package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	datlystore "github.com/viant/agently-slack-gateway/internal/storage/datly"
	"github.com/viant/datly/bootstrap/connector"
	_ "modernc.org/sqlite"
)

type Store struct {
	*datlystore.Store
	db      *sql.DB // Used only while preparing or checking the SQLite schema.
	dsn     string
	lock    *os.File
	created bool
	closed  bool
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
	s := &Store{dsn: u.String(), lock: lock, created: created}
	if err = s.prepareSchema(ctx); err != nil {
		return nil, err
	}
	// Linked Datly startup is lazy: it accepts empty and foreign SQLite files
	// without reading gateway tables. Migration and validation remain explicit.
	if err = s.startRuntime(ctx); err != nil {
		return nil, err
	}
	failed = false
	return s, nil
}

// Created reports whether this Open call created the database file. It does not
// report whether a schema is present; callers still use Migrate or CheckSchema.
func (s *Store) Created() bool { return s.created }

func (s *Store) Close() error {
	if s.closed {
		return os.ErrClosed
	}
	s.closed = true
	err := s.stopRuntime()
	if s.db != nil {
		if dbErr := s.db.Close(); err == nil {
			err = dbErr
		}
		s.db = nil
	}
	lockErr := s.lock.Close()
	if err != nil {
		return err
	}
	return lockErr
}

// Schema operations are offline. The lifetime file lock stays owned while the
// single database pool passes between schema preparation and the Datly runtime.
func (s *Store) prepareSchema(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.stopRuntime(); err != nil {
		return err
	}
	if s.db != nil {
		return nil
	}
	db, err := sql.Open("sqlite", s.dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return err
	}
	s.db = db
	return nil
}

func (s *Store) stopRuntime() error {
	if s.Store == nil {
		return nil
	}
	err := s.Store.Close()
	s.Store = nil
	return err
}

func (s *Store) startRuntime(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		if err != nil {
			return err
		}
	}
	runtime, err := datlystore.Open(ctx, connector.Config{
		Name: "slack", Driver: "sqlite", DSN: s.dsn, MaxOpenConns: 1, MaxIdleConns: 1,
	})
	if err != nil {
		return err
	}
	s.Store = runtime
	return nil
}
