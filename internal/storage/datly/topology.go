package datly

import (
	"context"

	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
)

// ReconcileIntegrations records active assignments and closes superseded work
// in the same native transaction used for ordinary persistence.
func (s *Store) ReconcileIntegrations(ctx context.Context, identities []storage.Integration) ([]storage.Integration, error) {
	out, err := s.call(ctx, &workflow.Input{Action: "reconcile_integrations", Integrations: identities})
	if err != nil {
		return nil, err
	}
	return out.Integrations, nil
}

// Scope selects one integration at the existing Datly invocation boundary.
// It owns no pool, migration, recovery loop or independent server lifetime.
func (s *Store) Scope(id int64) storage.Store {
	root := s
	if s.root != nil {
		root = s.root
	}
	return &Store{server: root.server, scope: id, root: root}
}

// ScopeName is the offline diagnostic/operator scope across assignment history.
func (s *Store) ScopeName(name string) storage.Store {
	root := s
	if s.root != nil {
		root = s.root
	}
	return &Store{server: root.server, scopeName: name, root: root}
}
