package storetest

import (
	"context"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/storage"
)

// DefaultIntegration is an explicit fixture identity, registered through the
// same native graph as production configuration before business work starts.
func DefaultIntegration() storage.Integration {
	return storage.Integration{
		Name: "default", Active: true,
		AppID: "A", WorkspaceID: "T", BackendType: "agently",
		BackendURL: "https://backend.example.test", BackendAgentID: "steward", PrincipalID: "shared",
	}
}

func RegisterDefault(t testing.TB, s interface {
	ReconcileIntegrations(context.Context, []storage.Integration) ([]storage.Integration, error)
}) int64 {
	t.Helper()
	rows, err := s.ReconcileIntegrations(context.Background(), []storage.Integration{DefaultIntegration()})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID <= 0 {
		t.Fatal("default integration was not registered")
	}
	return rows[0].ID
}

// Scope resolves the active fixture assignment through the native metadata
// reader instead of depending on the allocator's concrete numeric value.
func Scope(t testing.TB, s storage.Store, name string) storage.Store {
	t.Helper()
	status, err := s.ScopeName(name).Status(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range status.Integrations {
		if row.Name == name && row.Active {
			return s.Scope(row.ID)
		}
	}
	t.Fatalf("active fixture integration %q not found", name)
	return nil
}
