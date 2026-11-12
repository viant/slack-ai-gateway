package gateway

import (
	"fmt"
	"slices"
	"strings"
)

// ChannelScope selects the general channel rule, not conversation ownership.
type ChannelScope struct {
	Enabled      bool     `json:"enabled"`
	Scope        string   `json:"scope"`
	Allowlist    []string `json:"allowlist"`
	Denylist     []string `json:"denylist"`
	AllowPrivate bool     `json:"allow_private"`
}

// A nil allowlist is unrestricted; a nonnil empty allowlist permits nobody.
type UserRule struct {
	Scope     string
	Allowlist []string
	Denylist  []string
}

// ChannelRule replaces the complete general rule for one exact channel ID.
type ChannelRule struct {
	Enabled      bool
	AllowPrivate bool
	Users        *UserRule
}

type DirectMessageRule struct {
	Enabled bool
	Users   *UserRule
}

type AccessRules struct {
	Users            *UserRule
	DirectMessages   *DirectMessageRule
	ChannelOverrides map[string]ChannelRule
}

type EffectiveChannelPolicy struct {
	ChannelID    string `json:"channel_id"`
	Enabled      bool   `json:"enabled"`
	RuleSource   string `json:"rule_source"`
	RouteAllowed bool   `json:"route_allowed"`
	Present      bool   `json:"policy_present"`
	UserScope    string `json:"user_scope,omitempty"`
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
	if (scope.Enabled || scope.Scope != "") && scope.Scope != "selected" && scope.Scope != "workspace_channels_with_bot" {
		return nil, fmt.Errorf("access.channels.scope must be selected or workspace_channels_with_bot")
	}
	if scope.Enabled && scope.Scope == "selected" && scope.Allowlist == nil {
		return nil, fmt.Errorf("selected routing requires an explicit allowlist (possibly empty)")
	}
	for _, list := range [][]string{scope.Allowlist, scope.Denylist} {
		if err := validateIDs(list); err != nil {
			return nil, err
		}
	}
	if err := validateRule(rules.Users, scope.Enabled); err != nil {
		return nil, fmt.Errorf("access.channels: %w", err)
	}
	if rules.DirectMessages != nil {
		if err := validateRule(rules.DirectMessages.Users, rules.DirectMessages.Enabled); err != nil {
			return nil, fmt.Errorf("access.direct_messages: %w", err)
		}
	}
	for channel, rule := range rules.ChannelOverrides {
		if err := validateIDs([]string{channel}); err != nil {
			return nil, err
		}
		if err := validateRule(rule.Users, rule.Enabled); err != nil {
			return nil, fmt.Errorf("channel override %s: %w", channel, err)
		}
	}
	r := &ChannelPolicyResolver{scope: scope, rules: rules}
	r.scope.Allowlist, r.scope.Denylist = slices.Clone(scope.Allowlist), slices.Clone(scope.Denylist)
	r.rules.Users = cloneRule(rules.Users)
	if rules.DirectMessages != nil {
		r.rules.DirectMessages = &DirectMessageRule{Enabled: rules.DirectMessages.Enabled, Users: cloneRule(rules.DirectMessages.Users)}
	}
	r.rules.ChannelOverrides = make(map[string]ChannelRule, len(rules.ChannelOverrides))
	for key, rule := range rules.ChannelOverrides {
		rule.Users = cloneRule(rule.Users)
		r.rules.ChannelOverrides[key] = rule
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

func validateRule(rule *UserRule, required bool) error {
	if rule == nil {
		if required {
			return fmt.Errorf("an enabled rule requires users with an explicit scope")
		}
		return nil
	}
	if rule.Scope != "workspace_members_only" && rule.Scope != "workspace_members_and_guests" {
		return fmt.Errorf("users.scope must be workspace_members_only or workspace_members_and_guests")
	}
	for _, list := range [][]string{rule.Allowlist, rule.Denylist} {
		if err := validateIDs(list); err != nil {
			return err
		}
	}
	return nil
}

func cloneRule(in *UserRule) *UserRule {
	if in == nil {
		return nil
	}
	return &UserRule{Scope: in.Scope, Allowlist: slices.Clone(in.Allowlist), Denylist: slices.Clone(in.Denylist)}
}

func (r *ChannelPolicyResolver) Scope() ChannelScope {
	s := r.scope
	s.Allowlist, s.Denylist = slices.Clone(s.Allowlist), slices.Clone(s.Denylist)
	return s
}

func (r *ChannelPolicyResolver) ChannelIDs() []string {
	ids := slices.Clone(r.scope.Allowlist)
	for _, id := range r.scope.Denylist {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	for id := range r.rules.ChannelOverrides {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (r *ChannelPolicyResolver) Resolve(channel string) EffectiveChannelPolicy {
	p := EffectiveChannelPolicy{ChannelID: channel, Enabled: r.scope.Enabled, RuleSource: "disabled", AllowPrivate: r.scope.AllowPrivate}
	if !r.scope.Enabled {
		return resolveUsers(p, r.rules.Users)
	}
	if override, ok := r.rules.ChannelOverrides[channel]; ok {
		p.Enabled, p.AllowPrivate = override.Enabled, override.AllowPrivate
		if override.Enabled {
			p.RuleSource, p.RouteAllowed = "channel_override", channel != ""
		}
		return resolveUsers(p, override.Users)
	}
	p.RuleSource = "general"
	p.RouteAllowed = channel != "" && !slices.Contains(r.scope.Denylist, channel) &&
		(r.scope.Allowlist == nil || slices.Contains(r.scope.Allowlist, channel))
	return resolveUsers(p, r.rules.Users)
}

// DM has its own complete rule; channel rules and overrides never apply.
func (r *ChannelPolicyResolver) ResolveDirectMessages() EffectiveChannelPolicy {
	p := EffectiveChannelPolicy{RuleSource: "disabled"}
	if rule := r.rules.DirectMessages; rule != nil {
		p.Enabled, p.RouteAllowed = rule.Enabled, rule.Enabled
		if rule.Enabled {
			p.RuleSource = "general"
		}
		return resolveUsers(p, rule.Users)
	}
	return p
}

func resolveUsers(p EffectiveChannelPolicy, rule *UserRule) EffectiveChannelPolicy {
	if rule != nil {
		p.Present, p.UserScope = true, rule.Scope
		p.Allowlist, p.Denylist = slices.Clone(rule.Allowlist), slices.Clone(rule.Denylist)
	}
	return p
}

func (p EffectiveChannelPolicy) AllowsUser(id string) bool {
	return p.Enabled && p.RouteAllowed && p.Present && id != "" && !slices.Contains(p.Denylist, id) &&
		(p.Allowlist == nil || slices.Contains(p.Allowlist, id))
}
