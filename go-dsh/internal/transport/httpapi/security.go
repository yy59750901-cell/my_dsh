package httpapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// BearerAuthorized 比较固定长度摘要，避免令牌长度和前缀的时序比较。
func BearerAuthorized(values []string, token string) bool {
	if token == "" {
		return true
	}
	if len(values) != 1 {
		return false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	actual, want := sha256.Sum256([]byte(parts[1])), sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(actual[:], want[:]) == 1
}

func localAuthority(authority string) bool {
	if strings.ContainsAny(authority, "/@?#\\\t\r\n ") {
		return false
	}
	u, err := url.Parse("http://" + authority)
	if err != nil || u.Host != authority || u.Hostname() == "" {
		return false
	}
	host := u.Hostname()
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return false
		}
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return !strings.HasSuffix(authority, ":")
}

func sameOrigin(r *http.Request) bool {
	if !localAuthority(r.Host) {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	expected := scheme + "://" + r.Host
	origins := r.Header.Values("Origin")
	if len(origins) > 1 || (len(origins) == 1 && origins[0] != expected) {
		return false
	}
	if ref := r.Header.Get("Referer"); ref != "" {
		u, err := url.Parse(ref)
		if err != nil || u.User != nil || u.Scheme+"://"+u.Host != expected {
			return false
		}
	}
	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "none", "same-origin":
	default:
		return false
	}
	return true
}

var credentialText = regexp.MustCompile(`(?i)(bearer\s+[a-z0-9._~+/=-]+|\bsk-[a-z0-9_-]{8,}|(?:api[_-]?key|access[_-]?token|secret|password)\s*[=:]\s*[^\s,;"'&}]+)`)

func RedactText(value, token string) string {
	if token != "" {
		value = strings.ReplaceAll(value, token, "[REDACTED]")
	}
	return credentialText.ReplaceAllString(value, "[REDACTED]")
}

// RedactJSON 保留 JSON 数值精度，不序列化 Harness、Provider 或 Options。
func RedactJSON(raw []byte, token string) ([]byte, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case string:
			trimmed := strings.TrimSpace(x)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				var nested any
				decoder := json.NewDecoder(strings.NewReader(trimmed))
				decoder.UseNumber()
				if json.Valid([]byte(trimmed)) && decoder.Decode(&nested) == nil {
					original, _ := json.Marshal(nested)
					cleaned, err := json.Marshal(walk(nested))
					if err == nil && !bytes.Equal(original, cleaned) {
						return string(cleaned)
					}
				}
			}
			return RedactText(x, token)
		case []any:
			for i := range x {
				x[i] = walk(x[i])
			}
		case map[string]any:
			if x["role"] == "system" && x["source"] == "prompt" {
				x["content"] = []any{map[string]any{"type": "text", "text": "[REDACTED]"}}
			}
			for k, v := range x {
				normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
				switch normalized {
				case "authorization", "apikey", "token", "accesstoken", "refreshtoken", "secret", "clientsecret", "password", "cookie", "setcookie", "systemprompt":
					x[k] = "[REDACTED]"
				default:
					x[k] = walk(v)
				}
				if safe := RedactText(k, token); safe != k {
					x[safe] = x[k]
					delete(x, k)
				}
			}
		}
		return v
	}
	return json.Marshal(walk(value))
}
