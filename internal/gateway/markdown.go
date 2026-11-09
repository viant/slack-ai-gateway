package gateway

import (
	"strings"
	"unicode/utf8"
)

const MaxMarkdownCharacters = 12000

type codeFence struct{ marker, opening string }

// RenderMarkdown preserves standard Markdown for Slack's native markdown block.
// Part limits count Unicode characters, including fences added at boundaries.
func RenderMarkdown(text string) []ReplyPart {
	if strings.TrimSpace(text) == "" {
		return RenderText(text)
	}
	remaining := []rune(text)
	var parts []ReplyPart
	var fence codeFence
	prefix := ""
	lineStart := true
	for len(remaining) > 0 {
		end := min(len(remaining), MaxMarkdownCharacters-utf8.RuneCountInString(prefix))
		if end < len(remaining) {
			end = markdownBoundary(remaining, end)
		}
		var next codeFence
		var suffix string
		for {
			next = advanceFence(string(remaining[:end]), fence, lineStart)
			suffix = ""
			if next.marker != "" {
				suffix = "\n" + next.marker
			}
			overflow := utf8.RuneCountInString(prefix) + end + utf8.RuneCountInString(suffix) - MaxMarkdownCharacters
			if overflow <= 0 {
				break
			}
			end -= overflow
		}
		part := prefix + string(remaining[:end]) + suffix
		parts = append(parts, ReplyPart{Text: part, Format: ReplyMarkdown})
		lineStart = remaining[end-1] == '\n'
		remaining = remaining[end:]
		fence = next
		prefix = ""
		if fence.marker != "" {
			prefix = fence.opening + "\n"
		}
	}
	return parts
}

func markdownBoundary(text []rune, end int) int {
	// Prefer complete paragraphs, then lines; a very long line splits by rune.
	for i := end - 1; i > 0; i-- {
		if text[i] == '\n' && text[i-1] == '\n' {
			return i + 1
		}
	}
	for i := end - 1; i >= 0; i-- {
		if text[i] == '\n' {
			return i + 1
		}
	}
	return end
}

func advanceFence(text string, state codeFence, lineStart bool) codeFence {
	for _, line := range strings.SplitAfter(text, "\n") {
		if lineStart {
			value := strings.TrimRight(line, "\r\n")
			trimmed := strings.TrimLeft(value, " ")
			if len(value)-len(trimmed) <= 3 && len(trimmed) >= 3 && (trimmed[0] == '`' || trimmed[0] == '~') {
				n := 0
				for n < len(trimmed) && trimmed[n] == trimmed[0] {
					n++
				}
				if n >= 3 {
					if state.marker == "" {
						// Backticks in a backtick info string do not open a CommonMark fence.
						if (trimmed[0] != '`' || !strings.Contains(trimmed[n:], "`")) && utf8.RuneCountInString(value)+n+2 < MaxMarkdownCharacters {
							// Framing itself must fit with at least one content character.
							// Pathological oversized delimiters/info stay as original text.
							state = codeFence{marker: trimmed[:n], opening: value}
						}
					} else if trimmed[0] == state.marker[0] && n >= len(state.marker) && strings.TrimSpace(trimmed[n:]) == "" {
						state = codeFence{}
					}
				}
			}
		}
		lineStart = strings.HasSuffix(line, "\n")
	}
	return state
}
