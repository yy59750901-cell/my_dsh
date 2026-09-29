// Package profile 提供独立的配置快照与事务发布能力，不修改应用路由。
package profile

import (
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

const MaxJSONBytes = 1 << 20

var (
	ErrInvalid    = errors.New("invalid profile configuration")
	ErrCredential = errors.New("credential environment reference unavailable")
	idPattern     = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`)
	envPattern    = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)
)

// EnvRef 只保存变量名。解析出的凭据不写回 Bundle、Snapshot 或错误。
type EnvRef struct {
	Env string `json:"env"`
}

func (r EnvRef) Resolve(lookup func(string) (string, bool)) (string, error) {
	if !envPattern.MatchString(r.Env) || lookup == nil {
		return "", ErrCredential
	}
	value, ok := lookup(r.Env)
	if !ok || value == "" {
		return "", ErrCredential
	}
	return value, nil
}

type Model struct {
	ID            string  `json:"id"`
	Provider      string  `json:"provider"`
	Model         string  `json:"model"`
	Endpoint      string  `json:"endpoint,omitempty"`
	CredentialRef *EnvRef `json:"credential_ref,omitempty"`
}

type RemoteTool struct {
	ID            string  `json:"id"`
	Endpoint      string  `json:"endpoint"`
	CredentialRef *EnvRef `json:"credential_ref,omitempty"`
}

type MCPServer struct {
	ID                  string            `json:"id"`
	Transport           string            `json:"transport"`
	Endpoint            string            `json:"endpoint,omitempty"`
	Executable          string            `json:"executable,omitempty"`
	Args                []string          `json:"args,omitempty"`
	Env                 map[string]EnvRef `json:"env,omitempty"`
	CredentialRef       *EnvRef           `json:"credential_ref,omitempty"`
	TrustedOperatorOnly bool              `json:"trusted_operator_only,omitempty"`
}

type Profile struct {
	ID     string       `json:"id"`
	Models []Model      `json:"models,omitempty"`
	Tools  []RemoteTool `json:"tools,omitempty"`
	MCP    []MCPServer  `json:"mcp,omitempty"`
}

type Bundle struct {
	Profiles []Profile `json:"profiles"`
}

// Patch 中每个 Profile 按 ID 整段替换；不深合并，也不隐式删除。
// 已有 ID 保留原位置，新 ID 按 Patch 顺序追加。空数组可用于清空某个段。
type Patch struct {
	Profiles []Profile `json:"profiles"`
}

func ParseBundle(raw []byte) (Bundle, error) {
	var b Bundle
	if err := strictDecode(raw, &b); err != nil {
		return Bundle{}, err
	}
	if err := b.Validate(); err != nil {
		return Bundle{}, err
	}
	return b, nil
}

func ParsePatch(raw []byte) (Patch, error) {
	var p Patch
	if err := strictDecode(raw, &p); err != nil {
		return Patch{}, err
	}
	if err := (Bundle{Profiles: p.Profiles}).Validate(); err != nil {
		return Patch{}, err
	}
	return p, nil
}

func (b Bundle) Apply(p Patch) (Bundle, error) {
	if err := b.Validate(); err != nil {
		return Bundle{}, err
	}
	if err := (Bundle{Profiles: p.Profiles}).Validate(); err != nil {
		return Bundle{}, err
	}
	out := cloneBundle(b)
	index := make(map[string]int, len(out.Profiles))
	for i, v := range out.Profiles {
		index[v.ID] = i
	}
	for _, v := range cloneBundle(Bundle{Profiles: p.Profiles}).Profiles {
		if i, ok := index[v.ID]; ok {
			out.Profiles[i] = v
		} else {
			index[v.ID] = len(out.Profiles)
			out.Profiles = append(out.Profiles, v)
		}
	}
	if err := out.Validate(); err != nil {
		return Bundle{}, err
	}
	return out, nil
}

func validRef(r *EnvRef) bool { return r == nil || envPattern.MatchString(r.Env) }
func validEndpoint(s string, httpOnly bool) bool {
	u, err := url.Parse(s)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	if httpOnly {
		return u.Scheme == "http" || u.Scheme == "https"
	}
	return u.Scheme == "grpc" || u.Scheme == "grpcs"
}
func unique(seen map[string]bool, id string) bool {
	if !idPattern.MatchString(id) || seen[id] {
		return false
	}
	seen[id] = true
	return true
}

func (b Bundle) Validate() error {
	raw, err := json.Marshal(b)
	if err != nil || len(raw) > MaxJSONBytes || b.Profiles == nil {
		return ErrInvalid
	}
	profiles := make(map[string]bool)
	for _, p := range b.Profiles {
		if !unique(profiles, p.ID) {
			return ErrInvalid
		}
		models, tools, servers := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, m := range p.Models {
			if !unique(models, m.ID) || !idPattern.MatchString(m.Provider) || strings.TrimSpace(m.Model) == "" || !validRef(m.CredentialRef) || (m.Endpoint != "" && !validEndpoint(m.Endpoint, true)) {
				return ErrInvalid
			}
		}
		for _, t := range p.Tools {
			if !unique(tools, t.ID) || !validEndpoint(t.Endpoint, false) || !validRef(t.CredentialRef) {
				return ErrInvalid
			}
		}
		for _, s := range p.MCP {
			if !unique(servers, s.ID) || !validRef(s.CredentialRef) {
				return ErrInvalid
			}
			switch s.Transport {
			case "http":
				if !validEndpoint(s.Endpoint, true) || s.Executable != "" || len(s.Args) != 0 || len(s.Env) != 0 || s.TrustedOperatorOnly {
					return ErrInvalid
				}
			case "stdio":
				if !s.TrustedOperatorOnly || !filepath.IsAbs(s.Executable) || strings.ContainsRune(s.Executable, 0) || s.Endpoint != "" || s.CredentialRef != nil {
					return ErrInvalid
				}
				for _, arg := range s.Args {
					if strings.ContainsRune(arg, 0) {
						return ErrInvalid
					}
				}
				for k, v := range s.Env {
					if !envPattern.MatchString(k) || !envPattern.MatchString(v.Env) {
						return ErrInvalid
					}
				}
			default:
				return ErrInvalid
			}
		}
	}
	return nil
}

func cloneBundle(b Bundle) Bundle {
	// 仅用于已经校验过的配置；JSON 往返隔离所有切片、指针及 map。
	raw, _ := json.Marshal(b)
	var copy Bundle
	_ = json.Unmarshal(raw, &copy)
	return copy
}
