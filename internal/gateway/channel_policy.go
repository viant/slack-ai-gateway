package gateway

import (
	"fmt"
	"slices"
	"strings"
)

// ChannelScope controls routing, not conversation ownership.
type ChannelScope struct {
	Mode         string   `json:"mode"`
	Allowlist    []string `json:"allowlist"`
	Denylist     []string `json:"denylist"`
	AllowPrivate bool     `json:"allow_private"`
}

// Pointers distinguish inheritance from an explicit empty allow/deny list.
type UserRule struct {
	Mode      *string
	Allowlist *[]string
	Denylist  *[]string
}

type AccessRules struct {
	GlobalAllowlist *[]string
	GlobalDenylist  *[]string
	Defaults        *UserRule
	DirectMessages  *UserRule
	Channels        map[string]UserRule
}

type EffectiveChannelPolicy struct {
	ChannelID    string `json:"channel_id"`
	RouteAllowed bool   `json:"route_allowed"`
	Present      bool   `json:"policy_present"`
	UserMode     string `json:"user_mode,omitempty"`
	// nil means unrestricted; [] means nobody. Do not use omitempty.
	Allowlist    []string `json:"allowlist"`
	Denylist     []string `json:"denylist"`
	AllowPrivate bool     `json:"allow_private"`
}

// ChannelPolicyResolver owns immutable copies; callers cannot mutate a running
// policy by editing the configuration or a previously resolved result.
type ChannelPolicyResolver struct {
	scope ChannelScope
	rules AccessRules
}

func NewChannelPolicyResolver(scope ChannelScope, rules AccessRules) (*ChannelPolicyResolver, error) {
	if scope.Mode != "selected" && scope.Mode != "workspace" {
		return nil, fmt.Errorf("slack.channels.mode must be selected or workspace")
	}
	if scope.Mode == "workspace" && scope.Allowlist != nil {
		return nil, fmt.Errorf("workspace routing does not accept a channel allowlist")
	}
	if scope.Mode == "selected" && scope.Allowlist == nil {
		return nil, fmt.Errorf("selected routing requires an explicit allowlist (possibly empty)")
	}
	for _, list := range [][]string{scope.Allowlist, scope.Denylist} {
		if err := validateIDs(list); err != nil {
			return nil, err
		}
	}
	for _, list := range []*[]string{rules.GlobalAllowlist, rules.GlobalDenylist} {
		if list != nil {
			if err := validateIDs(*list); err != nil {
				return nil, err
			}
		}
	}
	if rules.Defaults != nil {
		if err := validateRule(*rules.Defaults, true); err != nil {
			return nil, err
		}
	}
	if rules.DirectMessages != nil {
		if err := validateRule(*rules.DirectMessages, true); err != nil {
			return nil, err
		}
	}
	for channel, rule := range rules.Channels {
		if err := validateIDs([]string{channel}); err != nil {
			return nil, err
		}
		if scope.Mode == "selected" && !slices.Contains(scope.Allowlist, channel) {
			return nil, fmt.Errorf("channel override is outside the selected channel allowlist")
		}
		if err := validateRule(rule, rules.Defaults == nil); err != nil {
			return nil, err
		}
	}
	r := &ChannelPolicyResolver{scope: scope, rules: rules}
	r.scope.Allowlist = slices.Clone(scope.Allowlist)
	r.scope.Denylist = slices.Clone(scope.Denylist)
	r.rules.GlobalAllowlist = cloneList(rules.GlobalAllowlist)
	r.rules.GlobalDenylist = cloneList(rules.GlobalDenylist)
	if rules.DirectMessages != nil {
		d := cloneRule(*rules.DirectMessages)
		r.rules.DirectMessages = &d
	}
	if rules.Defaults != nil {
		d := cloneRule(*rules.Defaults)
		r.rules.Defaults = &d
	}
	r.rules.Channels = make(map[string]UserRule, len(rules.Channels))
	for key, rule := range rules.Channels {
		r.rules.Channels[key] = cloneRule(rule)
	}
	return r, nil
}

func validateIDs(ids []string) error {
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || strings.TrimSpace(id) != id || strings.ContainsAny(id, " \t\r\n") || seen[id] {
			return fmt.Errorf("access and routing lists require nonempty, unique IDs without whitespace")
		}
		seen[id] = true
	}
	return nil
}

func validateRule(rule UserRule, requireMode bool) error {
	if requireMode && rule.Mode == nil {
		return fmt.Errorf("a policy without defaults must specify users.mode")
	}
	if rule.Mode != nil && *rule.Mode != "channel_members" && *rule.Mode != "members_only" {
		return fmt.Errorf("users.mode must be channel_members or members_only")
	}
	if rule.Mode == nil && rule.Allowlist == nil && rule.Denylist == nil {
		return fmt.Errorf("empty channel policies are not supported")
	}
	for _, list := range []*[]string{rule.Allowlist, rule.Denylist} {
		if list != nil {
			if err := validateIDs(*list); err != nil {
				return err
			}
		}
	}
	return nil
}

func cloneList(in *[]string) *[]string {
	if in == nil {
		return nil
	}
	v := append([]string{}, (*in)...)
	return &v
}

func cloneRule(in UserRule) UserRule {
	out := UserRule{Allowlist: cloneList(in.Allowlist), Denylist: cloneList(in.Denylist)}
	if in.Mode != nil {
		mode := *in.Mode
		out.Mode = &mode
	}
	return out
}

func (r *ChannelPolicyResolver) Scope() ChannelScope {
	s := r.scope
	s.Allowlist, s.Denylist = slices.Clone(s.Allowlist), slices.Clone(s.Denylist)
	return s
}

func (r *ChannelPolicyResolver) ChannelIDs() []string {
	ids := slices.Clone(r.scope.Allowlist)
	for id := range r.rules.Channels {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (r *ChannelPolicyResolver) Resolve(channel string) EffectiveChannelPolicy {
	p := EffectiveChannelPolicy{ChannelID: channel, AllowPrivate: r.scope.AllowPrivate}
	p.RouteAllowed = channel != "" && !slices.Contains(r.scope.Denylist, channel) &&
		(r.scope.Mode == "workspace" || slices.Contains(r.scope.Allowlist, channel))
	var local UserRule
	if r.rules.Defaults != nil {
		local = *r.rules.Defaults
	}
	if override, ok := r.rules.Channels[channel]; ok {
		if override.Mode != nil {
			local.Mode = override.Mode
		}
		if override.Allowlist != nil {
			local.Allowlist = override.Allowlist
		}
		if override.Denylist != nil {
			local.Denylist = override.Denylist
		}
	}
	return r.resolveUsers(p, local)
}

// DM has its own explicit policy; channel defaults and overrides never apply.
func (r *ChannelPolicyResolver) ResolveDirectMessages() EffectiveChannelPolicy {
	p := EffectiveChannelPolicy{RouteAllowed: true}
	var local UserRule
	if r.rules.DirectMessages != nil {
		local = *r.rules.DirectMessages
	}
	return r.resolveUsers(p, local)
}

func (r *ChannelPolicyResolver) resolveUsers(p EffectiveChannelPolicy, local UserRule) EffectiveChannelPolicy {
	if local.Mode != nil {
		p.Present, p.UserMode = true, *local.Mode
	}
	if local.Allowlist != nil {
		p.Allowlist = append([]string{}, (*local.Allowlist)...)
	}
	if r.rules.GlobalAllowlist != nil {
		global := *r.rules.GlobalAllowlist
		if p.Allowlist == nil {
			p.Allowlist = append([]string{}, global...)
		} else {
			allowed := []string{}
			for _, id := range p.Allowlist {
				if slices.Contains(global, id) {
					allowed = append(allowed, id)
				}
			}
			p.Allowlist = allowed
		}
	}
	p.Denylist = []string{}
	for _, list := range []*[]string{r.rules.GlobalDenylist, local.Denylist} {
		if list != nil {
			for _, id := range *list {
				if !slices.Contains(p.Denylist, id) {
					p.Denylist = append(p.Denylist, id)
				}
			}
		}
	}
	return p
}

func (p EffectiveChannelPolicy) AllowsUser(id string) bool {
	return p.RouteAllowed && p.Present && id != "" && !slices.Contains(p.Denylist, id) &&
		(p.Allowlist == nil || slices.Contains(p.Allowlist, id))
}
