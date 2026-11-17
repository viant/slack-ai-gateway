package agently

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// OOBLoginMode controls initial OAuth logins within one gateway process. Each
// integration retains its own authenticated session regardless of this policy.
type OOBLoginMode string

const (
	OOBLoginParallel      OOBLoginMode = "parallel"
	OOBLoginSerialPerUser OOBLoginMode = "serial_per_user"
)

func (m OOBLoginMode) Validate() error {
	switch m {
	case "", OOBLoginParallel, OOBLoginSerialPerUser:
		return nil
	default:
		return fmt.Errorf("oob_login_mode must be parallel or serial_per_user")
	}
}

// The actual IdP identity is available only after native configuration and
// credential loading. Neither Slack identities nor credential resource paths
// identify an OAuth account. Keys never leave this private in-memory registry.
type oobLoginKey struct{ endpoint, username string }

func loginKey(endpoint, username string) oobLoginKey {
	// Normalize URL authority spelling without changing endpoint paths, query
	// parameters or the provider's username semantics. Validation stays in OOB.
	if u, err := url.Parse(endpoint); err == nil {
		u.Scheme, u.Host = strings.ToLower(u.Scheme), strings.ToLower(u.Host)
		if (u.Scheme == "https" && u.Port() == "443") || (u.Scheme == "http" && u.Port() == "80") {
			u.Host = strings.TrimSuffix(u.Host, ":"+u.Port())
		}
		endpoint = u.String()
	}
	return oobLoginKey{endpoint: endpoint, username: username}
}

type oobLoginSlot struct {
	available chan struct{}
	users     int
}

type oobLoginCoordinator struct {
	mu    sync.Mutex
	slots map[oobLoginKey]*oobLoginSlot
}

var processOOBLogins oobLoginCoordinator

func (c *oobLoginCoordinator) acquire(ctx context.Context, key oobLoginKey) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.slots == nil {
		c.slots = make(map[oobLoginKey]*oobLoginSlot)
	}
	slot := c.slots[key]
	if slot == nil {
		slot = &oobLoginSlot{available: make(chan struct{}, 1)}
		slot.available <- struct{}{}
		c.slots[key] = slot
	}
	slot.users++
	c.mu.Unlock()
	forget := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		slot.users--
		if slot.users == 0 {
			delete(c.slots, key)
		}
	}
	select {
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	case <-slot.available:
		if err := ctx.Err(); err != nil {
			slot.available <- struct{}{}
			forget()
			return nil, err
		}
		return func() {
			slot.available <- struct{}{}
			forget()
		}, nil
	}
}
