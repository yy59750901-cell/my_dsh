package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yy59750901/go-dsh/internal/profile"
)

func cleanStartupProfileEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DSH_PROFILE_FILE", "DSH_PROFILE_ID", "DSH_PROFILE_PATCH_FILE",
		"DSH_MODE", "DSH_MODEL", "DSH_BASE_URL", "DSH_API_KEY", "DSH_MCP_ENDPOINT", "DSH_MCP_AUTHORIZATION",
		"DSH_TEST_PROFILE_MODEL_KEY", "DSH_TEST_PROFILE_MCP_AUTH",
	} {
		// Setenv 负责测试后恢复原值；Unsetenv 区分未设置与显式空值。
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}
func startupProfileFile(t *testing.T, raw []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, raw, 0400); err != nil {
		t.Fatal(err)
	}
	return path
}
func setStartupBundle(t *testing.T, profiles ...profile.Profile) string {
	t.Helper()
	raw, err := json.Marshal(profile.Bundle{Profiles: profiles})
	if err != nil {
		t.Fatal(err)
	}
	path := startupProfileFile(t, raw)
	t.Setenv("DSH_PROFILE_FILE", path)
	return path
}
func demoStartupProfile(id string) profile.Profile {
	return profile.Profile{ID: id, Models: []profile.Model{{ID: "model", Provider: "demo", Model: "demo"}}}
}
func openAIStartupProfile() profile.Profile {
	return profile.Profile{ID: "chosen", Models: []profile.Model{{ID: "model", Provider: "openai", Model: "profile-model", Endpoint: "https://models.example.test/v1", CredentialRef: &profile.EnvRef{Env: "DSH_TEST_PROFILE_MODEL_KEY"}}}, MCP: []profile.MCPServer{{ID: "mcp", Transport: "http", Endpoint: "https://mcp.example.test/rpc", CredentialRef: &profile.EnvRef{Env: "DSH_TEST_PROFILE_MCP_AUTH"}}}}
}
func startupCredentials(t *testing.T) {
	t.Helper()
	t.Setenv("DSH_TEST_PROFILE_MODEL_KEY", "test-only-model-key")
	t.Setenv("DSH_TEST_PROFILE_MCP_AUTH", "Bearer test-only-mcp-key")
}

func TestProfileConfigDisabledAndOrphanSettings(t *testing.T) {
	cleanStartupProfileEnv(t)
	cfg := Config{Mode: "unchanged", Model: "unchanged", APIKey: "existing-key", SkillsPath: "/must-not-read", Workspace: "/must-not-create", MCPEndpoint: "existing-endpoint"}
	t.Setenv("DSH_MODE", "openai")
	got, err := loadProfileConfig(cfg)
	if err != nil || got != cfg {
		t.Fatal("默认未启用时配置被更改", got, err)
	}
	for _, key := range []string{"DSH_PROFILE_ID", "DSH_PROFILE_PATCH_FILE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "/must-not-read")
			got, err := loadProfileConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), "require DSH_PROFILE_FILE") || strings.Contains(err.Error(), "/must-not-read") || got != cfg {
				t.Fatal(got, err)
			}
		})
	}
}

func TestProfileConfigMapsSingleModelAndHTTPMCPWithoutWrites(t *testing.T) {
	cleanStartupProfileEnv(t)
	startupCredentials(t)
	path := setStartupBundle(t, openAIStartupProfile())
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg := Config{HTTPAddr: "127.0.0.1:18080", GRPCAddr: "127.0.0.1:19090", Mode: "demo", Model: "old", BaseURL: "old", APIKey: "old", Token: "keep-token", DBPath: filepath.Join(root, "no-db", "db"), PostgresDSN: "keep-dsn", Workspace: filepath.Join(root, "no-workspace"), SkillsPath: "keep-skills", SystemPrompt: "keep-system", MCPEndpoint: "old", MCPAuthorization: "old"}
	got, err := loadProfileConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := cfg
	want.Mode, want.Model, want.BaseURL, want.APIKey = "openai", "profile-model", "https://models.example.test/v1", "test-only-model-key"
	want.MCPEndpoint, want.MCPAuthorization = "https://mcp.example.test/rpc", "Bearer test-only-mcp-key"
	if got != want {
		t.Fatal("配置映射或非 profile 字段保留错误")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("配置文件被修改", err)
	}
	if strings.Contains(string(after), "test-only-model-key") || strings.Contains(string(after), "test-only-mcp-key") {
		t.Fatal("凭据被写入文件")
	}
	for _, path := range []string{cfg.Workspace, filepath.Dir(cfg.DBPath)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("加载配置产生了启动副作用", err)
		}
	}
	// 显式逐次加载可以解析新 ENV，但不会修改文件或安装自动热重载。
	t.Setenv("DSH_TEST_PROFILE_MODEL_KEY", "test-only-rotated-key")
	again, err := loadProfileConfig(cfg)
	if err != nil || again.APIKey != "test-only-rotated-key" || got.APIKey != "test-only-model-key" {
		t.Fatal("凭据缓存或配置别名错误", err)
	}
}

func TestProfileConfigSelectionAndWholeProfilePatch(t *testing.T) {
	cleanStartupProfileEnv(t)
	startupCredentials(t)
	other := demoStartupProfile("other")
	base := openAIStartupProfile()
	base.Tools = []profile.RemoteTool{{ID: "unsupported", Endpoint: "grpcs://tools.example.test"}}
	setStartupBundle(t, other, base)
	cfg := Config{Mode: "demo"}
	if _, err := loadProfileConfig(cfg); err == nil {
		t.Fatal("未选择多个 profile 中的一个")
	}
	t.Setenv("DSH_PROFILE_ID", "missing")
	if _, err := loadProfileConfig(cfg); err == nil {
		t.Fatal("不存在的 profile 被接受")
	}
	t.Setenv("DSH_PROFILE_ID", "other")
	got, err := loadProfileConfig(cfg)
	if err != nil || got.Mode != "demo" || got.Model != "demo" {
		t.Fatal(got, err)
	}
	t.Setenv("DSH_PROFILE_ID", "chosen")
	if _, err := loadProfileConfig(cfg); err == nil || !strings.Contains(err.Error(), "remote tools") {
		t.Fatal("所选不支持配置未拒绝", err)
	}
	replacement := openAIStartupProfile()
	replacement.Models[0].Model = "patched-model"
	replacement.MCP = nil
	raw, _ := json.Marshal(profile.Patch{Profiles: []profile.Profile{replacement, demoStartupProfile("added")}})
	patchFile := startupProfileFile(t, raw)
	t.Setenv("DSH_PROFILE_PATCH_FILE", patchFile)
	got, err = loadProfileConfig(cfg)
	if err != nil || got.Model != "patched-model" || got.MCPEndpoint != "" || got.MCPAuthorization != "" {
		t.Fatal("patch 未整段替换", got, err)
	}
	t.Setenv("DSH_PROFILE_ID", "added")
	got, err = loadProfileConfig(cfg)
	if err != nil || got.Model != "demo" {
		t.Fatal("未在 patch 后选择新增 profile", got, err)
	}
	after, err := os.ReadFile(patchFile)
	if err != nil || string(after) != string(raw) {
		t.Fatal("patch 文件被修改", err)
	}
}

func TestProfileConfigRejectsUnsupportedSelectedConfiguration(t *testing.T) {
	cases := []struct {
		name, want string
		change     func(*profile.Profile)
	}{
		{"no_model", "exactly one model", func(p *profile.Profile) { p.Models = nil }},
		{"multiple_models", "multiple models", func(p *profile.Profile) {
			p.Models = append(p.Models, profile.Model{ID: "second", Provider: "demo", Model: "demo"})
		}},
		{"multiple_mcp", "multiple MCP", func(p *profile.Profile) {
			p.MCP = []profile.MCPServer{{ID: "a", Transport: "http", Endpoint: "https://a.example.test"}, {ID: "b", Transport: "http", Endpoint: "https://b.example.test"}}
		}},
		{"stdio", "stdio is unsupported", func(p *profile.Profile) {
			p.MCP = []profile.MCPServer{{ID: "stdio", Transport: "stdio", Executable: "/never-execute", TrustedOperatorOnly: true}}
		}},
		{"remote_tool", "remote tools", func(p *profile.Profile) {
			p.Tools = []profile.RemoteTool{{ID: "remote", Endpoint: "grpcs://tools.example.test"}}
		}},
		{"provider", "provider is unsupported", func(p *profile.Profile) { p.Models[0].Provider = "unknown" }},
		{"demo_model", "demo profile", func(p *profile.Profile) { p.Models[0].Model = "other" }},
		{"demo_endpoint", "demo profile", func(p *profile.Profile) { p.Models[0].Endpoint = "https://example.test" }},
		{"demo_credential", "demo profile", func(p *profile.Profile) {
			p.Models[0].CredentialRef = &profile.EnvRef{Env: "DSH_TEST_PROFILE_MODEL_KEY"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cleanStartupProfileEnv(t)
			p := demoStartupProfile("chosen")
			test.change(&p)
			setStartupBundle(t, p)
			cfg := Config{Mode: "original", APIKey: "original-key"}
			got, err := loadProfileConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) || got != cfg {
				t.Fatal("未显式拒绝或返回部分配置", got, err)
			}
		})
	}
}

func TestProfileConfigRejectsInvalidAndMissingCredentials(t *testing.T) {
	for _, mode := range []string{"no_ref", "no_endpoint", "missing_model_env", "missing_mcp_env", "empty_model_env", "model_header", "mcp_header", "insecure_endpoint"} {
		t.Run(mode, func(t *testing.T) {
			cleanStartupProfileEnv(t)
			startupCredentials(t)
			p := openAIStartupProfile()
			switch mode {
			case "no_ref":
				p.Models[0].CredentialRef = nil
			case "no_endpoint":
				p.Models[0].Endpoint = ""
			case "missing_model_env":
				if err := os.Unsetenv("DSH_TEST_PROFILE_MODEL_KEY"); err != nil {
					t.Fatal(err)
				}
			case "missing_mcp_env":
				if err := os.Unsetenv("DSH_TEST_PROFILE_MCP_AUTH"); err != nil {
					t.Fatal(err)
				}
			case "empty_model_env":
				t.Setenv("DSH_TEST_PROFILE_MODEL_KEY", "")
			case "model_header":
				t.Setenv("DSH_TEST_PROFILE_MODEL_KEY", "test-only-secret\r\nInjected: yes")
			case "mcp_header":
				t.Setenv("DSH_TEST_PROFILE_MCP_AUTH", "test-only-secret\r\nInjected: yes")
			case "insecure_endpoint":
				p.Models[0].Endpoint = "http://remote.example.test"
			}
			setStartupBundle(t, p)
			cfg := Config{Mode: "original"}
			got, err := loadProfileConfig(cfg)
			if err == nil || got != cfg || strings.Contains(err.Error(), "test-only-") || strings.Contains(err.Error(), "Injected") {
				t.Fatal(got, err)
			}
		})
	}
}

func TestProfileConfigExplicitEnvironmentConflictPolicy(t *testing.T) {
	values := map[string]string{
		"DSH_MODE": "openai", "DSH_MODEL": "profile-model", "DSH_BASE_URL": "https://models.example.test/v1",
		"DSH_API_KEY": "test-only-model-key", "DSH_MCP_ENDPOINT": "https://mcp.example.test/rpc", "DSH_MCP_AUTHORIZATION": "Bearer test-only-mcp-key",
	}
	for key := range values {
		for _, value := range []string{"conflicting-value", ""} {
			t.Run(key+"/"+value, func(t *testing.T) {
				cleanStartupProfileEnv(t)
				startupCredentials(t)
				setStartupBundle(t, openAIStartupProfile())
				t.Setenv(key, value)
				cfg := Config{Mode: "original", Model: "original"}
				got, err := loadProfileConfig(cfg)
				if err == nil || !strings.Contains(err.Error(), key+" conflicts") || got != cfg || strings.Contains(err.Error(), "test-only-") || strings.Contains(err.Error(), "conflicting-value") {
					t.Fatal(got, err)
				}
			})
		}
	}
	t.Run("identical_values", func(t *testing.T) {
		cleanStartupProfileEnv(t)
		startupCredentials(t)
		setStartupBundle(t, openAIStartupProfile())
		for key, value := range values {
			t.Setenv(key, value)
		}
		got, err := loadProfileConfig(ConfigFromEnv())
		if err != nil || got.Mode != "openai" || got.Model != "profile-model" {
			t.Fatal(got, err)
		}
	})
	t.Run("omitted_mcp_not_silently_ignored", func(t *testing.T) {
		cleanStartupProfileEnv(t)
		setStartupBundle(t, demoStartupProfile("chosen"))
		t.Setenv("DSH_MCP_ENDPOINT", "https://operator.example.test")
		if _, err := loadProfileConfig(Config{}); err == nil || !strings.Contains(err.Error(), "DSH_MCP_ENDPOINT conflicts") {
			t.Fatal(err)
		}
	})
	t.Run("clears_old_fields_without_explicit_env", func(t *testing.T) {
		cleanStartupProfileEnv(t)
		setStartupBundle(t, demoStartupProfile("chosen"))
		got, err := loadProfileConfig(Config{Mode: "openai", Model: "old", BaseURL: "old", APIKey: "old", MCPEndpoint: "old", MCPAuthorization: "old"})
		if err != nil || got != (Config{Mode: "demo", Model: "demo"}) {
			t.Fatal(got, err)
		}
	})
}

func TestProfileConfigStrictJSONAndBoundedFiles(t *testing.T) {
	valid := `{"profiles":[{"id":"chosen","models":[{"id":"model","provider":"demo","model":"demo"}]}]}`
	for _, raw := range []string{
		"", `null`, `{"profiles":[]}`, `{"profiles":[],"profiles":[]}`, valid + ` {}`,
		`{"profiles":[{"id":"chosen","id":"duplicate"}]}`,
		`{"Profiles":[{"id":"chosen"}]}`,
		`{"profiles":[{"id":"chosen","credential":"test-only-marker"}]}`,
		`{"profiles":[{"id":"chosen"`, "{\"profiles\":[{\"id\":\"\xff\"}]}",
		strings.Repeat(" ", profile.MaxJSONBytes+1),
	} {
		t.Run("invalid", func(t *testing.T) {
			cleanStartupProfileEnv(t)
			path := startupProfileFile(t, []byte(raw))
			t.Setenv("DSH_PROFILE_FILE", path)
			cfg := Config{Mode: "original"}
			got, err := loadProfileConfig(cfg)
			if err == nil || got != cfg || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "test-only-marker") {
				t.Fatal(got, err)
			}
		})
	}
	t.Run("exact_limit", func(t *testing.T) {
		cleanStartupProfileEnv(t)
		raw := valid + strings.Repeat(" ", profile.MaxJSONBytes-len(valid))
		path := startupProfileFile(t, []byte(raw))
		t.Setenv("DSH_PROFILE_FILE", path)
		if _, err := loadProfileConfig(Config{}); err != nil {
			t.Fatal(err)
		}
	})
	for _, mode := range []string{"directory", "missing", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			cleanStartupProfileEnv(t)
			root := t.TempDir()
			path := root
			switch mode {
			case "missing":
				path = filepath.Join(root, "test-only-private-path")
			case "symlink":
				target := startupProfileFile(t, []byte(valid))
				path = filepath.Join(root, "link")
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("DSH_PROFILE_FILE", path)
			if _, err := loadProfileConfig(Config{}); err == nil || strings.Contains(err.Error(), path) {
				t.Fatal(err)
			}
		})
	}
	for _, raw := range []string{`{"profiles":[],"profiles":[]}`, `{"profiles":[{"id":"chosen","password":"test-only-marker"}]}`, `{"profiles":[`, strings.Repeat(" ", profile.MaxJSONBytes+1)} {
		t.Run("invalid_patch", func(t *testing.T) {
			cleanStartupProfileEnv(t)
			setStartupBundle(t, demoStartupProfile("chosen"))
			path := startupProfileFile(t, []byte(raw))
			t.Setenv("DSH_PROFILE_PATCH_FILE", path)
			if _, err := loadProfileConfig(Config{}); err == nil || !strings.Contains(err.Error(), "DSH_PROFILE_PATCH_FILE") || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "test-only-marker") {
				t.Fatal(err)
			}
		})
	}
}

func TestProfileConfigConcurrentLoads(t *testing.T) {
	cleanStartupProfileEnv(t)
	startupCredentials(t)
	setStartupBundle(t, openAIStartupProfile())
	cfg := Config{Mode: "demo", Token: "preserved"}
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := loadProfileConfig(cfg)
			if err != nil || got.Mode != "openai" || got.APIKey != "test-only-model-key" || got.Token != cfg.Token {
				t.Error("并发加载结果错误", err)
			}
		}()
	}
	wg.Wait()
}
