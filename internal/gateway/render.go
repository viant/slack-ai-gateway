package gateway

import "strings"

const MaxReplyCharacters = 4000

// RenderText produces persisted, escaped Slack plain-text parts. The sender must
// disable markdown and automatic parsing and must NOT escape these parts again.
// Each source rune is one indivisible unit, including its escaped representation.
func RenderText(text string) []ReplyPart {
	if strings.TrimSpace(text) == "" {
		text = "The query completed without a text response."
	}
	var parts []ReplyPart
	var part strings.Builder
	count := 0
	for _, r := range text {
		s, size := string(r), 1
		switch r {
		case '&':
			s, size = "&amp;", 5
		case '<':
			s, size = "&lt;", 4
		case '>':
			s, size = "&gt;", 4
		}
		if count+size > MaxReplyCharacters {
			parts = append(parts, ReplyPart{Text: part.String(), Format: ReplyPlain})
			part.Reset()
			count = 0
		}
		part.WriteString(s)
		count += size
	}
	if part.Len() > 0 {
		parts = append(parts, ReplyPart{Text: part.String(), Format: ReplyPlain})
	}
	return parts
}
