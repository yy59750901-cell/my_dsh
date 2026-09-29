package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/profile"
)

// loadProfileConfig 仅在启动时读取可信操作员的 DSH_PROFILE_* 环境配置，
// 不接受模型提供的路径，不监听文件，不接入 profile.Manager 热重载。
// 未指定文件时原样返回 cfg，且不进行文件 I/O；孤立的 ID/patch 设置报错。
// 先按 ID 整段应用 patch，再选 profile；未指定 ID 时必须恰好只有一个。
// 所选 profile 覆盖 cfg 的全部模型/MCP 字段，其余字段保留。显式存在的
// DSH_MODE/MODEL/BASE_URL/API_KEY/MCP_ENDPOINT/MCP_AUTHORIZATION 必须与
// profile 一致，否则拒绝冲突（显式空值也参与比较）。失败返回原 cfg。
// 凭据只解析进返回的运行时 Config，不写文件，也不把路径或凭据放进错误。
func loadProfileConfig(cfg Config) (Config, error) {
	file := os.Getenv("DSH_PROFILE_FILE")
	id := os.Getenv("DSH_PROFILE_ID")
	patchFile := os.Getenv("DSH_PROFILE_PATCH_FILE")
	if file == "" {
		if id != "" || patchFile != "" {
			return cfg, errors.New("DSH_PROFILE_ID and DSH_PROFILE_PATCH_FILE require DSH_PROFILE_FILE")
		}
		return cfg, nil
	}
	raw, err := readProfileConfigFile(file)
	if err != nil {
		return cfg, fmt.Errorf("DSH_PROFILE_FILE: %w", err)
	}
	bundle, err := profile.ParseBundle(raw)
	if err != nil {
		return cfg, fmt.Errorf("DSH_PROFILE_FILE: %w", err)
	}
	if patchFile != "" {
		raw, err = readProfileConfigFile(patchFile)
		if err != nil {
			return cfg, fmt.Errorf("DSH_PROFILE_PATCH_FILE: %w", err)
		}
		patch, err := profile.ParsePatch(raw)
		if err != nil {
			return cfg, fmt.Errorf("DSH_PROFILE_PATCH_FILE: %w", err)
		}
		bundle, err = bundle.Apply(patch)
		if err != nil {
			return cfg, fmt.Errorf("DSH_PROFILE_PATCH_FILE: %w", err)
		}
	}
	if id == "" {
		if len(bundle.Profiles) != 1 {
			return cfg, errors.New("DSH_PROFILE_ID is required unless the bundle has exactly one profile")
		}
		id = bundle.Profiles[0].ID
	}
	var selected *profile.Profile
	for i := range bundle.Profiles {
		if bundle.Profiles[i].ID == id {
			selected = &bundle.Profiles[i]
			break
		}
	}
	if selected == nil {
		return cfg, errors.New("DSH_PROFILE_ID does not select an existing profile")
	}
	if len(selected.Models) != 1 {
		return cfg, errors.New("startup profile requires exactly one model; multiple models are unsupported")
	}
	if len(selected.Tools) != 0 {
		return cfg, errors.New("startup profile remote tools are unsupported")
	}
	if len(selected.MCP) > 1 {
		return cfg, errors.New("startup profile supports at most one HTTP MCP server; multiple MCP servers are unsupported")
	}
	if len(selected.MCP) == 1 && selected.MCP[0].Transport != "http" {
		return cfg, errors.New("startup profile supports HTTP MCP only; stdio is unsupported")
	}
	candidate := cfg
	model := selected.Models[0]
	candidate.Mode, candidate.Model, candidate.BaseURL = model.Provider, model.Model, model.Endpoint
	candidate.APIKey, candidate.MCPEndpoint, candidate.MCPAuthorization = "", "", ""
	switch model.Provider {
	case "demo":
		if model.Model != "demo" || model.Endpoint != "" || model.CredentialRef != nil {
			return cfg, errors.New("startup demo profile requires model demo and no endpoint or credential reference")
		}
	case "openai":
		if model.Endpoint == "" || model.CredentialRef == nil {
			return cfg, errors.New("startup openai profile requires an endpoint and credential_ref")
		}
		candidate.APIKey, err = model.CredentialRef.Resolve(os.LookupEnv)
		if err != nil {
			return cfg, errors.New("startup model credential reference unavailable")
		}
		// 构造器只校验并创建内存对象，不发请求；复用真实 provider 的 URL/凭据约束。
		if _, err = llm.NewOpenAIProvider(llm.OpenAIOptions{BaseURL: candidate.BaseURL, APIKey: candidate.APIKey}); err != nil {
			return cfg, errors.New("startup openai profile endpoint or credential is invalid")
		}
	default:
		return cfg, errors.New("startup profile provider is unsupported; use demo or openai")
	}
	if len(selected.MCP) == 1 {
		server := selected.MCP[0]
		candidate.MCPEndpoint = server.Endpoint
		if server.CredentialRef != nil {
			candidate.MCPAuthorization, err = server.CredentialRef.Resolve(os.LookupEnv)
			if err != nil {
				return cfg, errors.New("startup MCP credential reference unavailable")
			}
			if strings.ContainsAny(candidate.MCPAuthorization, "\r\n\x00") {
				return cfg, errors.New("startup MCP authorization is invalid")
			}
		}
	}
	for _, field := range []struct{ key, value string }{
		{"DSH_MODE", candidate.Mode}, {"DSH_MODEL", candidate.Model},
		{"DSH_BASE_URL", candidate.BaseURL}, {"DSH_API_KEY", candidate.APIKey},
		{"DSH_MCP_ENDPOINT", candidate.MCPEndpoint}, {"DSH_MCP_AUTHORIZATION", candidate.MCPAuthorization},
	} {
		if value, exists := os.LookupEnv(field.key); exists && value != field.value {
			return cfg, fmt.Errorf("%s conflicts with the selected startup profile", field.key)
		}
	}
	return candidate, nil
}

// 每个文件最多读取 MaxJSONBytes+1 字节；只接受普通文件，不跟随符号链接。
// 非阻塞打开避免检查后被换成 FIFO 时挂住，再核对打开后的文件身份。
func readProfileConfigFile(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return nil, errors.New("profile config must be a readable regular file, not a symlink")
	}
	if before.Size() > profile.MaxJSONBytes {
		return nil, errors.New("profile config exceeds size limit")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("cannot read profile config")
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, errors.New("profile config file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, profile.MaxJSONBytes+1))
	if err != nil {
		return nil, errors.New("cannot read profile config")
	}
	if len(raw) > profile.MaxJSONBytes {
		return nil, errors.New("profile config exceeds size limit")
	}
	return raw, nil
}
