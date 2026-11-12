package pilot

import (
	"database/sql"
	"testing"
)

// The pilot isolates low-level native graph behavior. This synthetic parent
// is fixture data; production assignments use the generated integration writer.
func seedPilotTopology(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("INSERT INTO integration(id,name,active,slack_app_id,slack_workspace_id,backend_type,backend_endpoint,backend_agent_id,backend_principal_id,created_at,updated_at) VALUES(1,'pilot',1,'pilot','T','agui','https://agent.example.test/run','','shared','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')")
	must(t, err)
}
