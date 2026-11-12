// Package predicate contains application predicates evaluated by native Datly.
package predicate

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/sql/fragment"
	"github.com/viant/xdatly/predicate"
)

// SmokeConversation preserves each database's existing smoke namespace check.
// Only the static SQL spelling varies; filtering stays in the database.
type SmokeConversation struct{}

var LinkedSmokeConversation = reflect.TypeFor[SmokeConversation]()

func (*SmokeConversation) Compute(ctx context.Context, _ any) (*predicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dialect := fragment.Dialect(ctx)
	if dialect == nil {
		return nil, fmt.Errorf("smoke namespace predicate requires a database dialect")
	}
	column := "backend_conversation_id"
	switch strings.ToLower(dialect.Name) {
	case "mysql":
		// This is the original MySQL case/accent-insensitive comparison.
		column += " COLLATE utf8mb4_0900_ai_ci"
	case "sqlite", "sqlite3":
		// SQLite's existing LIKE semantics include ASCII case folding.
	default:
		return nil, fmt.Errorf("unsupported smoke namespace dialect %q", dialect.Name)
	}
	return &predicate.Criteria{Expression: column + " LIKE ?", Placeholders: []any{"slack-smoke-%"}}, nil
}
