package predicate

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/sql/fragment"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestSmokeConversationDialectContract(t *testing.T) {
	for _, test := range []struct {
		name, expression string
		fail             bool
	}{
		{"mysql", "conversation_id COLLATE utf8mb4_0900_ai_ci LIKE ?", false},
		{"SQLite", "conversation_id LIKE ?", false},
		{"sqlite3", "conversation_id LIKE ?", false},
		{"other", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := fragment.WithDialect(context.Background(), &info.Dialect{Product: database.Product{Name: test.name}})
			criteria, err := (&SmokeConversation{}).Compute(ctx, true)
			if test.fail {
				if err == nil {
					t.Fatal("unsupported dialect accepted")
				}
				return
			}
			if err != nil || criteria.Expression != test.expression || len(criteria.Placeholders) != 1 || criteria.Placeholders[0] != "slack-smoke-%" {
				t.Fatalf("predicate=%+v error=%v", criteria, err)
			}
		})
	}
	if _, err := (&SmokeConversation{}).Compute(context.Background(), true); err == nil {
		t.Fatal("missing dialect accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&SmokeConversation{}).Compute(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}
