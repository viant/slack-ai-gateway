package cli

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestVersionFlagsAreOffline(t *testing.T) {
	previous := version
	t.Cleanup(func() { version = previous })
	if version != "dev" {
		t.Fatalf("default CLI version = %q, want dev", version)
	}
	for _, value := range []string{"dev", "v1.2.3"} {
		SetVersion(value)
		for _, flag := range []string{"-v", "--version"} {
			t.Run(value+"/"+flag, func(t *testing.T) {
				var out bytes.Buffer
				// No configured dependencies: version reporting must not reach storage,
				// secret loading, backend authentication or Slack connection setup.
				if err := execute(context.Background(), []string{flag}, &out, dependencies{}); err != nil {
					t.Fatal(err)
				}
				if got := out.String(); got != value+"\n" {
					t.Fatalf("version output = %q, want %q", got, value+"\n")
				}
			})
		}
	}
}

func TestVersionFlagsRequireStandaloneInvocation(t *testing.T) {
	for _, args := range [][]string{
		{"run", "--version"}, {"run", "-v"},
		{"--version", "run"}, {"-v", "run"},
		{"--version", "--config", "missing.yaml"},
	} {
		if err := execute(context.Background(), args, &bytes.Buffer{}, dependencies{}); err == nil {
			t.Fatalf("combined version invocation accepted: %v", args)
		}
	}
}

type versionFailureWriter struct{ err error }

func (w versionFailureWriter) Write([]byte) (int, error) { return 0, w.err }

func TestVersionPropagatesOutputFailure(t *testing.T) {
	want := errors.New("output unavailable")
	if err := execute(context.Background(), []string{"--version"}, versionFailureWriter{want}, dependencies{}); !errors.Is(err, want) {
		t.Fatalf("version output error = %v, want %v", err, want)
	}
}
