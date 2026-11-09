package slack

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

func channelJSON() map[string]any {
	return map[string]any{"id": "C", "context_team_id": "T", "is_private": false, "is_member": true, "is_archived": false, "is_ext_shared": false, "is_pending_ext_shared": false, "is_im": false, "is_mpim": false}
}
func userJSON() map[string]any {
	return map[string]any{"id": "alice", "team_id": "T", "deleted": false, "is_bot": false, "is_restricted": false, "is_ultra_restricted": false}
}

func testDirectory(t *testing.T, handler http.HandlerFunc) *Directory {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	d := NewDirectory("xoxb-secret-marker", "T")
	d.baseURL = server.URL + "/"
	return d
}

func TestDirectoryReadOnlyIdentityAndFieldPresence(t *testing.T) {
	for _, kind := range []string{"channel", "user"} {
		fields := channelJSON()
		method, key, id := "conversations.info", "channel", "C"
		if kind == "user" {
			fields = userJSON()
			method, key, id = "users.info", "user", "alice"
		}
		for _, field := range []string{"", "is_private", "is_member", "is_archived", "is_ext_shared", "is_pending_ext_shared", "is_im", "is_mpim", "deleted", "is_bot", "is_restricted", "is_ultra_restricted"} {
			if field != "" {
				if _, ok := fields[field]; !ok {
					continue
				}
			}
			for _, null := range []bool{false, true} {
				t.Run(kind+"/"+field+"/"+map[bool]string{false: "missing", true: "null"}[null], func(t *testing.T) {
					projection := map[string]any{}
					for k, v := range fields {
						projection[k] = v
					}
					if field != "" {
						if null {
							projection[field] = nil
						} else {
							delete(projection, field)
						}
					}
					d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) {
						if r.Method != http.MethodGet || r.URL.Path != "/"+method || r.URL.Query().Get(key) != id {
							t.Error("unexpected metadata request")
						}
						if r.Header.Get("Authorization") != "Bearer xoxb-secret-marker" || strings.Contains(r.URL.String(), "secret-marker") {
							t.Error("unsafe credential transport")
						}
						json.NewEncoder(w).Encode(map[string]any{"ok": true, kind: projection})
					})
					var err error
					if kind == "channel" {
						_, err = d.Channel(context.Background(), "T", id, false)
					} else {
						_, err = d.User(context.Background(), "T", id)
					}
					if field == "" {
						if err != nil {
							t.Fatal(err)
						}
					} else if !errors.Is(err, gateway.ErrMetadataUnavailable) {
						t.Fatalf("absence accepted: %v", err)
					}
				})
			}
		}
	}
}

func TestDirectoryForeignAndEnterpriseUsers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(map[string]any)
		want  error
	}{
		{"local", nil, nil},
		{"foreign", func(u map[string]any) { u["team_id"] = "OTHER" }, gateway.ErrDenied},
		{"stranger_not_proof", func(u map[string]any) { u["team_id"] = "OTHER"; u["is_stranger"] = false }, gateway.ErrDenied},
		{"enterprise", func(u map[string]any) {
			u["team_id"] = "OTHER"
			u["enterprise_user"] = map[string]any{"teams": []string{"OTHER", "T"}}
		}, nil},
		{"external", func(u map[string]any) { u["is_external"] = true }, gateway.ErrDenied},
		{"stranger", func(u map[string]any) { u["is_stranger"] = true }, gateway.ErrDenied},
		{"wrong_id", func(u map[string]any) { u["id"] = "bob" }, gateway.ErrDenied},
		{"app_authorized_not_bot", func(u map[string]any) { u["is_app_user"] = true }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := userJSON()
			if tc.alter != nil {
				tc.alter(u)
			}
			d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "user": u})
			})
			_, err := d.User(context.Background(), "T", "alice")
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
	for _, field := range []string{"id", "context_team_id"} {
		c := channelJSON()
		c[field] = "OTHER"
		d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": c})
		})
		if _, err := d.Channel(context.Background(), "T", "C", false); !errors.Is(err, gateway.ErrDenied) {
			t.Fatal("foreign channel accepted")
		}
	}
}

func TestDirectoryFailureClassificationAndSanitization(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   error
	}{
		{200, `{"ok":false,"error":"invalid_auth"}`, gateway.ErrSlackAuth},
		{200, `{"ok":false,"error":"token_revoked"}`, gateway.ErrSlackAuth},
		{200, `{"ok":false,"error":"missing_scope","needed":"secret-marker"}`, gateway.ErrMetadataUnavailable},
		{200, `{"ok":false,"error":"not_in_channel"}`, gateway.ErrDenied},
		{200, `{"ok":false,"error":"channel_not_found"}`, gateway.ErrDenied},
		{200, `{"ok":false,"error":"secret-marker"}`, gateway.ErrMetadataUnavailable},
		{200, `secret-marker`, gateway.ErrMetadataUnavailable},
		{200, `{"channel":{}}`, gateway.ErrMetadataUnavailable},
		{200, `{"ok":true,"channel":{}}`, gateway.ErrMetadataUnavailable},
		{401, `secret-marker`, gateway.ErrSlackAuth}, {403, `secret-marker`, gateway.ErrSlackAuth},
		{429, `secret-marker`, gateway.ErrMetadataUnavailable}, {503, `secret-marker`, gateway.ErrMetadataUnavailable},
		{200, strings.Repeat("s", (1<<20)+1), gateway.ErrMetadataUnavailable},
	} {
		d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); w.Write([]byte(tc.body)) })
		_, err := d.Channel(context.Background(), "T", "C", false)
		if !errors.Is(err, tc.want) || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("unsafe failure: %v", err)
		}
	}
	d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := d.Channel(ctx, "T", "C", false); !errors.Is(err, gateway.ErrMetadataUnavailable) {
		t.Fatal(err)
	}
	redirect := testDirectory(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.invalid/secret-marker", 302)
	})
	if _, err := redirect.Channel(context.Background(), "T", "C", false); !errors.Is(err, gateway.ErrMetadataUnavailable) {
		t.Fatal("redirect followed")
	}
}
