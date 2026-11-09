package slack

import (
	"encoding/json"
	"strings"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// Keep the wire projection explicit: a missing security field must not silently
// become false through an SDK bool's zero value. No raw payload is logged/stored.
type callback struct {
	Type           string `json:"type"`
	AppID          string `json:"api_app_id"`
	TeamID         string `json:"team_id"`
	EventID        string `json:"event_id"`
	ExternalShared *bool  `json:"is_ext_shared_channel"`
	Event          struct {
		Type        string            `json:"type"`
		Subtype     string            `json:"subtype"`
		User        string            `json:"user"`
		BotID       string            `json:"bot_id"`
		Text        string            `json:"text"`
		Channel     string            `json:"channel"`
		ChannelType string            `json:"channel_type"`
		TS          string            `json:"ts"`
		ThreadTS    string            `json:"thread_ts"`
		Edited      json.RawMessage   `json:"edited"`
		Files       []json.RawMessage `json:"files"`
		Attachments []json.RawMessage `json:"attachments"`
	} `json:"event"`
}

func normalize(payload []byte, route gateway.Route, botUserID string) (gateway.Message, string) {
	var c callback
	if json.Unmarshal(payload, &c) != nil {
		return gateway.Message{}, "malformed_event"
	}
	if c.Type != "event_callback" {
		return gateway.Message{}, "unsupported_event"
	}
	e := c.Event
	surface := gateway.SurfaceChannel
	if e.Type == "message" && e.ChannelType == "im" {
		surface = gateway.SurfaceDirectMessage
	} else if e.Type != "app_mention" || e.ChannelType == "im" || e.ChannelType == "mpim" {
		return gateway.Message{}, "unsupported_event"
	}
	m := gateway.Message{
		Surface:   surface,
		ThreadKey: gateway.ThreadKey{WorkspaceID: c.TeamID, ChannelID: e.Channel, RootTS: e.ThreadTS},
		AppID:     c.AppID, EventID: c.EventID, AuthorID: e.User, MessageTS: e.TS,
		Bot:            e.BotID != "" || e.User == botUserID,
		Edited:         len(e.Edited) > 0 && string(e.Edited) != "null",
		Deleted:        e.Subtype == "message_deleted",
		HasAttachments: len(e.Files) > 0 || len(e.Attachments) > 0,
	}
	if !route.Allows(m) {
		return gateway.Message{}, "route_denied"
	}
	if (c.ExternalShared == nil && surface == gateway.SurfaceChannel) || (c.ExternalShared != nil && *c.ExternalShared) {
		return gateway.Message{}, "external_channel_or_missing_metadata"
	}
	if m.Bot || m.Edited || e.Subtype != "" {
		return gateway.Message{}, "unsupported_message"
	}
	m.Mentioned = botUserID != "" && strings.Contains(e.Text, "<@"+botUserID+">")
	if botUserID == "" || (surface == gateway.SurfaceChannel && !m.Mentioned) {
		return gateway.Message{}, "no_bot_mention"
	}
	if m.RootTS == "" {
		m.RootTS = e.TS
	}
	if m.EventID == "" || m.AuthorID == "" || m.MessageTS == "" || m.RootTS == "" {
		return gateway.Message{}, "incomplete_event"
	}
	m.Text = strings.TrimSpace(strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">").Replace(strings.ReplaceAll(e.Text, "<@"+botUserID+">", "")))
	return m, ""
}
