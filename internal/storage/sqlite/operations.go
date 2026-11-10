package sqlite

import "github.com/viant/slack-ai-gateway/internal/storage"

// Keep the SQLite package names available to existing callers.
type Status = storage.Status
type AgentSessionMetadata = storage.AgentSessionMetadata
type RequestStatusMetadata = storage.RequestStatusMetadata
type JobStatus = storage.JobStatus
type BindingStatus = storage.BindingStatus
type DeliveryStatus = storage.DeliveryStatus
