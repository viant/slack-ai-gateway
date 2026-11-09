package gateway

import (
	"context"
	"sync"
	"time"
)

type ChannelInfo struct {
	PeerID                                                                     string // Verified conversations.info user for a one-to-one DM.
	WorkspaceID, ID                                                            string
	Private, Member, Archived, ExternalShared, PendingExternalShared, IM, MPIM bool
}

type UserInfo struct {
	WorkspaceID, ID                         string
	Deleted, Bot, Guest, SingleChannelGuest bool
}

// Implementations must verify identity and field presence before returning
// facts. Unknown security fields are an error, never a bool's zero value.
type SlackDirectory interface {
	Channel(context.Context, string, string, bool) (ChannelInfo, error)
	User(context.Context, string, string) (UserInfo, error)
}

type metadataKey struct {
	workspace, id string
	user          bool
}
type metadataEntry struct {
	channel ChannelInfo
	user    UserInfo
	at      time.Time
}
type metadataFlight struct {
	done  chan struct{}
	entry metadataEntry
	err   error
}

type CachedDirectory struct {
	Source  SlackDirectory
	mu      sync.Mutex
	entries map[metadataKey]metadataEntry
	flights map[metadataKey]*metadataFlight
	now     func() time.Time
}

func NewCachedDirectory(source SlackDirectory) *CachedDirectory {
	return &CachedDirectory{Source: source, entries: make(map[metadataKey]metadataEntry), flights: make(map[metadataKey]*metadataFlight), now: time.Now}
}

func (d *CachedDirectory) Channel(ctx context.Context, workspace, id string, fresh bool) (ChannelInfo, error) {
	e, err := d.get(ctx, metadataKey{workspace: workspace, id: id}, fresh)
	return e.channel, err
}

func (d *CachedDirectory) User(ctx context.Context, workspace, id string) (UserInfo, error) {
	e, err := d.get(ctx, metadataKey{workspace: workspace, id: id, user: true}, false)
	return e.user, err
}

func (d *CachedDirectory) get(ctx context.Context, key metadataKey, fresh bool) (metadataEntry, error) {
	if err := ctx.Err(); err != nil {
		return metadataEntry{}, err
	}
	d.mu.Lock()
	if e, ok := d.entries[key]; ok && !fresh && d.now().Sub(e.at) < time.Minute {
		d.mu.Unlock()
		return e, nil
	}
	if flight, ok := d.flights[key]; ok {
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return metadataEntry{}, ctx.Err()
		case <-flight.done:
			return flight.entry, flight.err
		}
	}
	f := &metadataFlight{done: make(chan struct{})}
	d.flights[key] = f
	// Invalidate even a still-fresh old entry before a forced refresh. Failure
	// must not leave stale positive authorization for a subsequent caller.
	delete(d.entries, key)
	d.mu.Unlock()
	var entry metadataEntry
	var err error
	if d.Source == nil {
		err = ErrMetadataUnavailable
	} else if key.user {
		entry.user, err = d.Source.User(ctx, key.workspace, key.id)
	} else {
		entry.channel, err = d.Source.Channel(ctx, key.workspace, key.id, fresh)
	}
	d.mu.Lock()
	entry.at = d.now()
	if err == nil {
		count := 0
		var oldest metadataKey
		var oldestAt time.Time
		for k, e := range d.entries {
			if k.user != key.user {
				continue
			}
			count++
			if oldestAt.IsZero() || e.at.Before(oldestAt) {
				oldest, oldestAt = k, e.at
			}
		}
		if count >= 1024 {
			delete(d.entries, oldest)
		}
		d.entries[key] = entry
	}
	f.entry, f.err = entry, err
	delete(d.flights, key)
	close(f.done)
	d.mu.Unlock()
	return entry, err
}
