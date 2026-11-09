package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// The SDK exposes optional security flags as bool, losing absent/null versus
// false. This narrow read-only projection preserves presence. Posting and
// Socket Mode continue to use the pinned slack-go SDK.
type Directory struct {
	client                    *http.Client
	baseURL, token, workspace string
}

func NewDirectory(token, workspace string) *Directory {
	return &Directory{client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, baseURL: "https://slack.com/api/", token: token, workspace: workspace}
}

func (d *Directory) Channel(ctx context.Context, workspace, id string, _ bool) (gateway.ChannelInfo, error) {
	if workspace != d.workspace || workspace == "" || id == "" {
		return gateway.ChannelInfo{}, gateway.ErrDenied
	}
	var response struct {
		OK      *bool  `json:"ok"`
		Error   string `json:"error"`
		Channel struct {
			ID              string `json:"id"`
			Team            string `json:"context_team_id"`
			User            string `json:"user"`
			Private         *bool  `json:"is_private"`
			Member          *bool  `json:"is_member"`
			Archived        *bool  `json:"is_archived"`
			External        *bool  `json:"is_ext_shared"`
			PendingExternal *bool  `json:"is_pending_ext_shared"`
			IM              *bool  `json:"is_im"`
			MPIM            *bool  `json:"is_mpim"`
		} `json:"channel"`
	}
	if err := d.read(ctx, "conversations.info", "channel", id, &response); err != nil {
		return gateway.ChannelInfo{}, err
	}
	if err := metadataResponseError(response.OK, response.Error); err != nil {
		return gateway.ChannelInfo{}, err
	}
	c := response.Channel
	if c.ID == "" {
		return gateway.ChannelInfo{}, gateway.ErrMetadataUnavailable
	}
	if c.ID != id || (c.Team != "" && c.Team != workspace) {
		return gateway.ChannelInfo{}, gateway.ErrDenied
	}
	if c.IM != nil && *c.IM {
		if c.User == "" {
			return gateway.ChannelInfo{}, gateway.ErrMetadataUnavailable
		}
		if (c.MPIM != nil && *c.MPIM) || (c.External != nil && *c.External) || (c.PendingExternal != nil && *c.PendingExternal) {
			return gateway.ChannelInfo{}, gateway.ErrDenied
		}
		return gateway.ChannelInfo{WorkspaceID: workspace, ID: id, IM: true, PeerID: c.User}, nil
	}
	if c.Private == nil || c.Member == nil || c.Archived == nil || c.External == nil || c.PendingExternal == nil || c.IM == nil || c.MPIM == nil {
		return gateway.ChannelInfo{}, gateway.ErrMetadataUnavailable
	}
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: id, Private: *c.Private, Member: *c.Member, Archived: *c.Archived,
		ExternalShared: *c.External, PendingExternalShared: *c.PendingExternal, IM: *c.IM, MPIM: *c.MPIM}, nil
}

func (d *Directory) User(ctx context.Context, workspace, id string) (gateway.UserInfo, error) {
	if workspace != d.workspace || workspace == "" || id == "" {
		return gateway.UserInfo{}, gateway.ErrDenied
	}
	var response struct {
		OK    *bool  `json:"ok"`
		Error string `json:"error"`
		User  struct {
			ID         string `json:"id"`
			Team       string `json:"team_id"`
			Deleted    *bool  `json:"deleted"`
			Bot        *bool  `json:"is_bot"`
			Guest      *bool  `json:"is_restricted"`
			Single     *bool  `json:"is_ultra_restricted"`
			External   *bool  `json:"is_external"`
			Stranger   *bool  `json:"is_stranger"`
			Enterprise struct {
				Teams []string `json:"teams"`
			} `json:"enterprise_user"`
		} `json:"user"`
	}
	if err := d.read(ctx, "users.info", "user", id, &response); err != nil {
		return gateway.UserInfo{}, err
	}
	if err := metadataResponseError(response.OK, response.Error); err != nil {
		return gateway.UserInfo{}, err
	}
	u := response.User
	if u.ID == "" || (u.Team == "" && len(u.Enterprise.Teams) == 0) {
		return gateway.UserInfo{}, gateway.ErrMetadataUnavailable
	}
	if u.ID != id || (u.Team != workspace && !slices.Contains(u.Enterprise.Teams, workspace)) ||
		(u.External != nil && *u.External) || (u.Stranger != nil && *u.Stranger) {
		return gateway.UserInfo{}, gateway.ErrDenied
	}
	if u.Deleted == nil || u.Bot == nil || u.Guest == nil || u.Single == nil {
		return gateway.UserInfo{}, gateway.ErrMetadataUnavailable
	}
	return gateway.UserInfo{WorkspaceID: workspace, ID: id, Deleted: *u.Deleted, Bot: *u.Bot, Guest: *u.Guest, SingleChannelGuest: *u.Single}, nil
}

func (d *Directory) read(ctx context.Context, method, key, id string, result any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	endpoint := d.baseURL + method + "?" + url.Values{key: {id}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return gateway.ErrMetadataUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+d.token)
	response, err := d.client.Do(request)
	if err != nil {
		return gateway.ErrMetadataUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return gateway.ErrSlackAuth
	}
	if response.StatusCode != http.StatusOK {
		return gateway.ErrMetadataUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, result) != nil {
		return gateway.ErrMetadataUnavailable
	}
	return nil
}

func metadataResponseError(ok *bool, code string) error {
	if ok != nil && *ok {
		return nil
	}
	switch strings.TrimSpace(code) {
	case "invalid_auth", "not_authed", "token_revoked", "account_inactive", "token_expired":
		return gateway.ErrSlackAuth
	case "channel_not_found", "not_in_channel", "user_not_found", "is_archived":
		return gateway.ErrDenied
	default:
		return gateway.ErrMetadataUnavailable
	}
}
