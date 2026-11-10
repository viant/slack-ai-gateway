package gateway_test

import (
	"html"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestRenderText(t *testing.T) {
	for _, input := range []string{"hello", "<!here> <@U123> & <https://example.invalid|link> @everyone", "&lt;literal&gt;", strings.Repeat("ą🙂<&>", 2000), strings.Repeat("x", 3999) + "&end", ""} {
		parts := gateway.RenderText(input)
		if len(parts) == 0 {
			t.Fatal("empty result")
		}
		for _, item := range parts {
			part := item.Text
			if !utf8.ValidString(part) || utf8.RuneCountInString(part) > 4000 || part == "" || strings.ContainsAny(part, "<>") {
				t.Fatal("unsafe or oversized part")
			}
			// Each part round-trips separately: an entity cannot cross its boundary.
			if strings.HasSuffix(part, "&") || strings.HasSuffix(part, "&am") || strings.HasSuffix(part, "&lt") {
				t.Fatal("split escape sequence")
			}
		}
		var decoded strings.Builder
		for _, item := range parts {
			part := item.Text
			decoded.WriteString(html.UnescapeString(part))
		}
		if input != "" && decoded.String() != input {
			t.Fatal("text changed or escaped twice")
		}
	}
}

func TestRenderNativeMarkdown(t *testing.T) {
	for _, text := range []string{
		"# Title\n\n**Bold** and [link](https://example.org).\n\n| A | B |\n| - | - |\n| 1 | 2 |",
		strings.Repeat("ą🙂", 6000),
		strings.Repeat("paragraph\n\n", 2500),
		strings.Repeat("a", 11000) + "\n\n" + strings.Repeat("b", 4000),
	} {
		parts := gateway.RenderMarkdown(text)
		var joined strings.Builder
		for _, p := range parts {
			if p.Format != gateway.ReplyMarkdown || !utf8.ValidString(p.Text) || utf8.RuneCountInString(p.Text) > 12000 {
				t.Fatal("wrong native Markdown part")
			}
			joined.WriteString(p.Text)
		}
		if joined.String() != text {
			t.Fatal("Markdown modified/escaped")
		}
		if utf8.RuneCountInString(text) <= 12000 && len(parts) != 1 {
			t.Fatal("short answer split")
		}
	}
	parts := gateway.RenderMarkdown(strings.Repeat("a", 11000) + "\n\n" + strings.Repeat("b", 4000))
	if !strings.HasSuffix(parts[0].Text, "\n\n") {
		t.Fatal("paragraph boundary not preferred")
	}
}

func TestMarkdownCodeFencesAcrossParts(t *testing.T) {
	for _, opening := range []string{"```go", "~~~~python", "````text"} {
		marker := strings.TrimRight(opening, "gopythnex")
		body := strings.Repeat("fmt.Println(\"ą🙂\")\n", 1600)
		parts := gateway.RenderMarkdown(opening + "\n" + body + marker + "\n")
		if len(parts) < 2 {
			t.Fatal("long code was not split")
		}
		var restored strings.Builder
		for i, p := range parts {
			if utf8.RuneCountInString(p.Text) > 12000 || !strings.HasPrefix(p.Text, opening+"\n") {
				t.Fatalf("invalid code part %d", i)
			}
			text := strings.TrimPrefix(p.Text, opening+"\n")
			if i == len(parts)-1 {
				text = strings.TrimSuffix(text, marker+"\n")
			} else {
				if !strings.HasSuffix(text, "\n"+marker) {
					t.Fatal("fence left open")
				}
				text = strings.TrimSuffix(text, "\n"+marker)
			}
			restored.WriteString(text)
		}
		if restored.String() != body {
			t.Fatal("code lost or reordered")
		}
	}
}

func TestMarkdownOversizedFenceFramingPreservesInput(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("`", 13000) + "\nbody\n",
		"```" + strings.Repeat("x", 13000) + "\nbody\n",
		strings.Repeat("~", 6000) + "\nbody\n",
	} {
		var restored strings.Builder
		for _, part := range gateway.RenderMarkdown(text) {
			if part.Text == "" || !utf8.ValidString(part.Text) || utf8.RuneCountInString(part.Text) > gateway.MaxMarkdownCharacters {
				t.Fatal("oversized fence caused an empty, invalid or oversized part")
			}
			restored.WriteString(part.Text)
		}
		if restored.String() != text {
			t.Fatal("oversized delimiter was rewritten")
		}
	}
}
