package mysql

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
)

func TestMixedDSNUsesNativeConnectorSecret(t *testing.T) {
	dsn := fixtureDSN(t)
	parsed, err := driver.ParseDSN(dsn)
	storetest.Must(t, err)
	resource := scy.NewResource(cred.Basic{}, "file://"+filepath.ToSlash(filepath.Join(t.TempDir(), "mysql.json")), "blowfish://default")
	credentials := &cred.Basic{Username: parsed.User, Password: parsed.Passwd}
	storetest.Must(t, scy.New().Store(context.Background(), scy.NewSecret(credentials, resource)))
	parsed.User, parsed.Passwd = "${Username}", "${Password}"
	// The connector, not the gateway, loads Scy's typed credentials and
	// expands the DSN before opening the same isolated fixture database.
	s, err := Open(context.Background(), parsed.FormatDSN(), &scy.Resource{URL: resource.URL, Key: resource.Key})
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, s.Close()) })
	_, err = s.Status(context.Background(), 1)
	storetest.Must(t, err)
}

func TestSecretFailuresDoNotExposeResourceOrCiphertext(t *testing.T) {
	for _, resource := range []*scy.Resource{
		{URL: "file:///missing-sensitive-resource-marker", Key: "blowfish://default"},
		{URL: "file:///sensitive-resource-marker.enc", Key: "blowfish://default", Data: []byte("short")},
	} {
		_, err := Open(context.Background(), "${Username}:${Password}@tcp(127.0.0.1:1)/unused", resource)
		if err == nil || strings.Contains(err.Error(), "sensitive-resource-marker") || strings.Contains(err.Error(), "short") {
			t.Fatal("secret failure was accepted or exposed secret input")
		}
	}
}
