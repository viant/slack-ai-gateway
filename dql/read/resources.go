package read

import "embed"

// DatlyResourceNamespace exposes the existing claim predicate to companion
// native readers at transcription time. Generated runtime SQL is self-contained.
const DatlyResourceNamespace = "job_claim_sql"

//go:embed sql/query_eligibility.sql
var DatlyResources embed.FS
