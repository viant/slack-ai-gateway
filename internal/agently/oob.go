package agently

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/viant/scy"
	"github.com/viant/scy/auth/authorizer"
	"github.com/viant/scy/auth/flow"
	"github.com/viant/scy/cred"
	_ "github.com/viant/scy/kms/blowfish" // compatibility with existing Scy local secrets
	"golang.org/x/oauth2"
)

type oauthConfigLoader func(context.Context, string) (*oauth2.Config, error)
type basicSecretLoader func(context.Context, string) (*cred.Basic, error)

func loadOAuthConfig(ctx context.Context, ref string) (result *oauth2.Config, err error) {
	// Some legacy cipher implementations panic on malformed ciphertext. Keep
	// library failures at this boundary; never expose secret-bearing panic data.
	defer func() {
		if recover() != nil {
			result, err = nil, errors.New("cannot decode OAuth configuration")
		}
	}()
	if !strings.Contains(ref, "|") {
		// Scy's typed credentials require a KMS key even for plaintext. A
		// methodless type permits plain/inline OAuth configuration explicitly.
		type plainConfig cred.Oauth2Config
		resource := scy.EncodedResource(ref).Decode(ctx, reflect.TypeOf(plainConfig{}))
		if encoded, ok := strings.CutPrefix(resource.URL, "inlined://base64/"); ok {
			resource.Data, err = base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return nil, errors.New("invalid inline OAuth configuration")
			}
		}
		secret, err := scy.New().Load(ctx, resource)
		if err != nil {
			return nil, err
		}
		config := (*cred.Oauth2Config)(secret.Target.(*plainConfig))
		if config.EncryptedClientSecret != "" {
			return nil, errors.New("encrypted OAuth configuration requires a key reference")
		}
		return &config.Config, nil
	}
	config := authorizer.OAuthConfig{ConfigURL: ref}
	err = authorizer.New().EnsureConfig(ctx, &config)
	return config.Config, err // caller redacts all library errors
}

func loadBasicSecret(ctx context.Context, ref string) (result *cred.Basic, err error) {
	defer func() {
		if recover() != nil {
			result, err = nil, errors.New("cannot decode OOB credentials")
		}
	}()
	// D1 deliberately accepts a local file only, never inline credentials or an
	// arbitrary remote destination. The env variable contains a reference.
	path, key, _ := strings.Cut(ref, "|")
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.New("invalid local secret reference")
	}
	local := path
	if u.Scheme == "file" && u.Host == "" && u.RawQuery == "" && u.Fragment == "" {
		local = u.Path
	}
	if !filepath.IsAbs(local) || (u.Scheme != "" && u.Scheme != "file") || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("OOB credentials require an absolute local file path or file URL")
	}
	if key == "" {
		type plainBasic cred.Basic
		secret, err := scy.New().Load(ctx, scy.NewResource(reflect.TypeOf(plainBasic{}), local, ""))
		if err != nil {
			return nil, err
		}
		basic := (*cred.Basic)(secret.Target.(*plainBasic))
		if basic.EncryptedPassword != "" {
			return nil, errors.New("encrypted OOB credentials require a key reference")
		}
		return basic, nil
	}
	secret, err := scy.New().Load(ctx, scy.NewResource(reflect.TypeOf(cred.Basic{}), local, key))
	if err != nil {
		return nil, err
	}
	basic, ok := secret.Target.(*cred.Basic)
	if !ok {
		return nil, errors.New("invalid credential format")
	}
	return basic, nil
}

type sessionPayload struct {
	AccessToken  string `json:"accessToken,omitempty"`
	IDToken      string `json:"idToken,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	ExpiresAt    string `json:"expiresAt,omitempty"`
}

func (sessionPayload) String() string   { return "[Agently credentials redacted]" }
func (sessionPayload) GoString() string { return "[Agently credentials redacted]" }

func secureEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "https" || (u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()))
}

// scy's OOB form helper lacks context/timeout. Keep its protocol and PKCE
// helpers, but bound the HTTP exchange locally and never follow its redirect.
func authorizeOOB(ctx context.Context, config *oauth2.Config, basic *cred.Basic, client *http.Client) (*sessionPayload, error) {
	if !secureEndpoint(config.Endpoint.AuthURL) || !secureEndpoint(config.Endpoint.TokenURL) {
		return nil, oobFailure("endpoint validation", 0)
	}
	random := make([]byte, 64)
	if _, err := rand.Read(random); err != nil {
		return nil, oobFailure("PKCE initialization", 0)
	}
	verifier, state := base64.RawURLEncoding.EncodeToString(random[:32]), base64.RawURLEncoding.EncodeToString(random[32:])
	const callback = "https://localhost/callback.html"
	opts := []flow.Option{flow.WithPKCE(true), flow.WithCodeVerifier(verifier), flow.WithState(state), flow.WithRedirectURI(callback)}
	authURL, err := flow.BuildAuthCodeURL(config, opts...)
	if err != nil {
		return nil, oobFailure("authorization URL construction", 0)
	}
	form := url.Values{"username": {basic.Username}, "password": {basic.Password}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, oobFailure("authorization request construction", 0)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(req)
	if err != nil {
		return nil, oobFailure("authorization request, HTTP status unavailable", 0)
	}
	response.Body.Close() // Location is all we need; never read/log the login page.
	if response.StatusCode/100 != 3 {
		return nil, oobFailure("authorization endpoint", response.StatusCode)
	}
	location, err := url.Parse(response.Header.Get("Location"))
	if err != nil || location.Scheme != "https" || location.Host != "localhost" || location.Path != "/callback.html" || location.User != nil || location.Fragment != "" {
		return nil, oobFailure("authorization redirect", response.StatusCode)
	}
	query := location.Query()
	if query.Get("error") != "" {
		return nil, oobFailure("authorization rejected", response.StatusCode)
	}
	if query.Get("code") == "" {
		return nil, oobFailure("authorization code missing", response.StatusCode)
	}
	if query.Get("state") != state {
		return nil, oobFailure("authorization state mismatch", response.StatusCode)
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
	token, err := flow.Exchange(ctx, config, query.Get("code"), opts...)
	if err != nil || token == nil {
		var responseErr *oauth2.RetrieveError
		if errors.As(err, &responseErr) && responseErr.Response != nil {
			return nil, oobFailure("token exchange", responseErr.Response.StatusCode)
		}
		return nil, oobFailure("token exchange, HTTP status unavailable", 0)
	}
	idToken, _ := token.Extra("id_token").(string)
	if token.AccessToken == "" && idToken == "" {
		return nil, oobFailure("token response missing credentials", 0)
	}
	payload := &sessionPayload{AccessToken: token.AccessToken, IDToken: idToken, RefreshToken: token.RefreshToken}
	if !token.Expiry.IsZero() {
		payload.ExpiresAt = token.Expiry.Format(time.RFC3339)
	}
	return payload, nil
}

// Stage must be an internal literal, never data from the IdP or its libraries.
// Raw errors, URLs, response bodies and headers can all contain credentials.
func oobFailure(stage string, status int) error {
	if status != 0 {
		return fmt.Errorf("Agently OOB authorization failed (%s, HTTP %d)", stage, status)
	}
	return fmt.Errorf("Agently OOB authorization failed (%s)", stage)
}
