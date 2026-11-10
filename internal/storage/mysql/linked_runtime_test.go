package mysql

import (
	"context"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// Embedded graph resources and linked holders must work without a source
// checkout in the process working directory, as in a deployed gateway binary.
func TestLinkedRuntimeWithoutSourceDirectory(t *testing.T) {
	dsn := fixtureDSN(t)
	t.Chdir(t.TempDir())
	s, err := Open(context.Background(), dsn, nil)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, s.Close()) })
	inserted, err := s.Enqueue(context.Background(), storetest.Message("linked", "thread", "alice"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("missing linked intake")
	}
	j, err := s.ClaimJob(context.Background())
	storetest.Must(t, err)
	if j == nil || j.Text != "diagnose linked" {
		t.Fatal("linked graph did not retain the original payload")
	}
}
