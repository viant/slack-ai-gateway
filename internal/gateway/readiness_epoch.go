package gateway

// ReadyEpochLifecycle optionally exposes a ready permit to local durable
// admission. The epoch advances whenever a ready backend becomes unavailable;
// callbacks must perform only short storage work and must not reenter Backend.
type ReadyEpochLifecycle interface {
	WithReadyEpoch(func(uint64) error) error
}
