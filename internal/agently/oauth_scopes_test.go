package agently

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func oauthScopeCases() []struct {
	name   string
	config oauthDiscovery
	want   string
} {
	return []struct {
		name   string
		config oauthDiscovery
		want   string
	}{
		{
			name: "base plus CLI, not other surfaces",
			config: oauthDiscovery{Scopes: []string{"openid", "profile"},
				CLIScopes: []string{"cli:read"}, WebUIScopes: []string{"web:read"}, MobileUIScopes: []string{"mobile:read"}},
			want: "openid profile cli:read",
		},
		{
			name: "base plus Web UI when CLI absent",
			config: oauthDiscovery{Scopes: []string{"openid", "profile", "email", "plan:create", "plan:edit", "plan:read"},
				WebUIScopes: []string{"ROLE_STEWARD_WEB"}, MobileUIScopes: []string{"ROLE_STEWARD_MOBILE"}},
			want: "openid profile email plan:create plan:edit plan:read ROLE_STEWARD_WEB",
		},
		{
			name: "mobile when CLI and Web UI empty after normalization",
			config: oauthDiscovery{Scopes: []string{"openid"}, CLIScopes: []string{"", " \t"},
				WebUIScopes: []string{"\n"}, MobileUIScopes: []string{" mobile:read "}},
			want: "openid mobile:read",
		},
		{
			name: "trim, deduplicate and preserve order",
			config: oauthDiscovery{Scopes: []string{" profile ", "", "openid", "profile"}, CLIScopes: []string{" "},
				WebUIScopes: []string{" openid\t", "web:read", " web:read ", "email"}},
			want: "profile openid web:read email",
		},
		{
			name: "CLI overlapping base still wins",
			config: oauthDiscovery{Scopes: []string{"openid"}, CLIScopes: []string{"openid"},
				WebUIScopes: []string{"web:read"}},
			want: "openid",
		},
		{name: "base only", config: oauthDiscovery{Scopes: []string{" profile ", "email"}}, want: "profile email"},
		{name: "surface only", config: oauthDiscovery{CLIScopes: []string{"cli:read"}}, want: "cli:read"},
		{name: "no scopes", want: "openid"},
		{
			name: "all groups empty after normalization",
			config: oauthDiscovery{Scopes: []string{" "}, CLIScopes: []string{"\n"},
				WebUIScopes: []string{""}, MobileUIScopes: []string{"\t"}},
			want: "openid",
		},
	}
}

func TestHeadlessScopes(t *testing.T) {
	for _, tc := range oauthScopeCases() {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.config
			original.Scopes = slices.Clone(tc.config.Scopes)
			original.CLIScopes = slices.Clone(tc.config.CLIScopes)
			original.WebUIScopes = slices.Clone(tc.config.WebUIScopes)
			original.MobileUIScopes = slices.Clone(tc.config.MobileUIScopes)
			got := tc.config.headlessScopes()
			if !slices.Equal(got, strings.Split(tc.want, " ")) {
				t.Fatalf("scopes = %q, want %q", got, tc.want)
			}
			got[0] = "changed-output"
			if !reflect.DeepEqual(tc.config, original) {
				t.Fatal("scope selection mutated or aliased the discovery configuration")
			}
		})
	}
}

func TestOOBHeadlessScopesOnWire(t *testing.T) {
	for _, tc := range oauthScopeCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newLoginFixture(t)
			f.mu.Lock()
			f.discovery, f.expectedScopes = tc.config, tc.want
			f.mu.Unlock()
			factory, err := f.login()
			if err != nil || factory == nil {
				t.Fatalf("preflight failed: %v", err)
			}
			for _, path := range []string{"/v1/api/auth/oauth/config", "/authorize", "/token", "/v1/api/auth/session", "/v1/api/auth/me", "/v1/workspace/metadata"} {
				if got := f.count(path); got != 1 {
					t.Errorf("%s called %d times, want once", path, got)
				}
			}
			if f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 {
				t.Fatal("preflight created or queried a conversation")
			}
		})
	}
}
