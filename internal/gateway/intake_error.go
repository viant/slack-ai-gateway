package gateway

import "errors"

// ErrIntakePersistence marks an unacknowledged event whose durable admission
// failed. Transport adapters preserve this category without exposing the cause,
// so a shared database failure cannot be mistaken for one bot disconnecting.
var ErrIntakePersistence = errors.New("Slack intake persistence failed")
