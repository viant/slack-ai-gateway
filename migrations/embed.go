package migrations

import "embed"

// SQLite contains the initial schema and future versioned migrations.
//
//go:embed sqlite/*.sql
var SQLite embed.FS
