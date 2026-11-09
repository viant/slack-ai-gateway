package slack

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	sdk "github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/viant/agently-slack-gateway/internal/config"
	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// Connect validates the bot credentials without logging SDK data. App identity
// is checked separately against the Socket Mode hello, before runtime readiness.
func Connect(ctx context.Context, tokens config.SlackTokens, route gateway.Route, logger *slog.Logger) (*Source, *Sender, error) {
	client := sdk.New(tokens.Bot, sdk.OptionAppLevelToken(tokens.App), sdk.OptionDebug(false),
		sdk.OptionHTTPClient(&http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}))
	userID, err := preflight(ctx, client, route)
	if err != nil {
		return nil, nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	source := &Source{session: sdkSession{socketmode.New(client, socketmode.OptionDebug(false))}, route: route, botUserID: userID, log: logger}
	return source, &Sender{api: client, route: route}, nil
}

func preflight(ctx context.Context, api *sdk.Client, route gateway.Route) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	auth, err := api.AuthTestContext(ctx)
	if err != nil {
		return "", fmt.Errorf("Slack bot authentication check failed")
	}
	if route.AppID == "" || (route.ChannelID == "" && route.Resolver == nil) || route.WorkspaceID == "" || auth.TeamID != route.WorkspaceID || auth.UserID == "" || auth.BotID == "" {
		return "", fmt.Errorf("Slack bot identity or workspace does not match configuration")
	}
	return auth.UserID, nil
}
