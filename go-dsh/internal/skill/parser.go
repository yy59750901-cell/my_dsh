package skill

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	MaxDocumentBytes    = 64 << 10
	MaxFrontmatterBytes = 8 << 10
	MaxSkills           = 64
)

var ErrFrontmatter = errors.New("unsupported or malformed skill frontmatter")
var skillName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

type Descriptor struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Source      string `json:"source"`
}

// parse 仅支持 name/description 字符串以及 description 的 |、> 块。
// 不使用完整 YAML，不解析标签、锚点、别名、合并键或可执行元数据；未知键拒绝。
func parse(data []byte) (Descriptor, string, error) {
	if len(data) > MaxDocumentBytes || !utf8.Valid(data) || strings.ContainsRune(string(data), 0) {
		return Descriptor{}, "", ErrFrontmatter
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	if !strings.HasPrefix(text, "---\n") {
		return Descriptor{}, "", ErrFrontmatter
	}
	lines := strings.Split(text, "\n")
	end := -1
	bytes := 4
	for i := 1; i < len(lines) && i <= 128; i++ {
		bytes += len(lines[i]) + 1
		if bytes > MaxFrontmatterBytes || len(lines[i]) > 4096 {
			return Descriptor{}, "", ErrFrontmatter
		}
		if lines[i] == "---" {
			end = i
			break
		}
	}
	if end < 0 {
		return Descriptor{}, "", ErrFrontmatter
	}
	values := make(map[string]string)
	for i := 1; i < end; i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.ContainsRune(line, '\t') || strings.HasPrefix(line, " ") {
			return Descriptor{}, "", ErrFrontmatter
		}
		key, raw, ok := strings.Cut(line, ":")
		if !ok || (key != "name" && key != "description") {
			return Descriptor{}, "", ErrFrontmatter
		}
		if _, exists := values[key]; exists {
			return Descriptor{}, "", ErrFrontmatter
		}
		raw = strings.TrimSpace(raw)
		if raw == "|" || raw == "|-" || raw == ">" || raw == ">-" {
			if key != "description" {
				return Descriptor{}, "", ErrFrontmatter
			}
			var block []string
			for i+1 < end {
				next := lines[i+1]
				if next != "" && !strings.HasPrefix(next, "  ") {
					break
				}
				if strings.ContainsRune(next, '\t') {
					return Descriptor{}, "", ErrFrontmatter
				}
				i++
				block = append(block, strings.TrimPrefix(next, "  "))
			}
			if len(block) == 0 {
				return Descriptor{}, "", ErrFrontmatter
			}
			separator := "\n"
			if raw[0] == '>' {
				separator = " "
			}
			values[key] = strings.TrimSpace(strings.Join(block, separator))
		} else {
			value, err := scalar(raw)
			if err != nil {
				return Descriptor{}, "", err
			}
			values[key] = value
		}
	}
	if !skillName.MatchString(values["name"]) || strings.TrimSpace(values["description"]) == "" || len(values["description"]) > 4096 {
		return Descriptor{}, "", ErrFrontmatter
	}
	return Descriptor{Name: values["name"], Description: values["description"]}, strings.Join(lines[end+1:], "\n"), nil
}

func scalar(raw string) (string, error) {
	if raw == "" {
		return "", ErrFrontmatter
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal([]byte(raw), &s); err != nil || strings.ContainsRune(s, 0) {
			return "", ErrFrontmatter
		}
		return s, nil
	}
	if raw[0] == '\'' {
		if len(raw) < 2 || raw[len(raw)-1] != '\'' {
			return "", ErrFrontmatter
		}
		inner := raw[1 : len(raw)-1]
		var out strings.Builder
		for i := 0; i < len(inner); i++ {
			if inner[i] == '\'' {
				if i+1 >= len(inner) || inner[i+1] != '\'' {
					return "", ErrFrontmatter
				}
				i++
			}
			out.WriteByte(inner[i])
		}
		return out.String(), nil
	}
	if strings.ContainsAny(raw[:1], "!&*[{>|@`#") || strings.ContainsAny(raw, "\r\t") {
		return "", ErrFrontmatter
	}
	return raw, nil
}
