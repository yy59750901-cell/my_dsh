package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func cleanLocalConfigEnv(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "DSH_") {
			t.Setenv(key, "")
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func localConfigValues() map[string]string {
	return map[string]string{
		"mode": "openai", "model": "test-only-model", "api_key": "test-only-key",
		"base_url":  "https://models.example.test/api/v3/chat/completions",
		"http_addr": "127.0.0.1:0", "grpc_addr": "[::1]:0",
		"db_path": "state/dsh.db", "workspace": "workspace",
		"api_token": "test-only-token", "system_prompt": "本地测试",
	}
}

func writeLocalConfig(t *testing.T, directory string, values map[string]string) string {
	t.Helper()
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "config.local.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadLocalConfigRelativePathsAndAllFields(t *testing.T) {
	cleanLocalConfigEnv(t)
	root := t.TempDir()
	values := localConfigValues()
	values["skills_path"] = "skills"
	values["postgres_dsn"] = "test-only-dsn"
	values["mcp_endpoint"] = "https://mcp.example.test/rpc"
	values["mcp_authorization"] = "Bearer test-only-mcp"
	path := writeLocalConfig(t, root, values)
	t.Chdir(t.TempDir())
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != "openai" || cfg.Model != values["model"] || cfg.APIKey != values["api_key"] || cfg.BaseURL != "https://models.example.test/api/v3" || cfg.Token != values["api_token"] || cfg.SystemPrompt != values["system_prompt"] || cfg.HTTPAddr != values["http_addr"] || cfg.GRPCAddr != values["grpc_addr"] || cfg.PostgresDSN != values["postgres_dsn"] || cfg.SkillsPath != "skills" || cfg.MCPEndpoint != values["mcp_endpoint"] || cfg.MCPAuthorization != values["mcp_authorization"] {
		t.Fatal("字段映射错误")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DBPath != filepath.Join(resolvedRoot, "state/dsh.db") || cfg.Workspace != filepath.Join(resolvedRoot, "workspace") || cfg.localConfigPath != filepath.Join(resolvedRoot, "config.local.json") {
		t.Fatal("相对路径没有固定到配置文件目录")
	}
	if !cfg.configLoaded {
		t.Fatal("未记录配置快照")
	}
	if err = ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.Workspace, filepath.Dir(cfg.DBPath)} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("加载或校验产生文件系统副作用")
		}
	}
}

func TestLoadConfigDefaultAndMissing(t *testing.T) {
	cleanLocalConfigEnv(t)
	t.Chdir(t.TempDir())
	if cfg := ConfigFromEnv(); cfg.Mode != "openai" {
		t.Fatal("默认模式不是 openai")
	}
	if _, err := LoadConfig(""); err == nil {
		t.Fatal("默认缺少模型凭据时不能回退 demo")
	}
	// 即使 .env 存在，也不读取它。
	if err := os.WriteFile(".env", []byte("DSH_MODE=demo\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(""); err == nil {
		t.Fatal("隐式读取了 .env")
	}
	t.Setenv("DSH_MODE", "demo")
	cfg, err := LoadConfig("")
	if err != nil || cfg.Mode != "demo" || cfg.localConfigPath != "" {
		t.Fatal("ENV 显式 demo 回退失败", err)
	}
	if got, err := LoadConfig("missing-test-only-private-path"); err == nil || got != (Config{}) || strings.Contains(err.Error(), "missing-test-only-private-path") {
		t.Fatal("指定路径不存在必须报脱敏错误")
	}
	if err := os.Unsetenv("DSH_MODE"); err != nil {
		t.Fatal(err)
	}
	writeLocalConfig(t, ".", localConfigValues())
	if cfg, err = LoadConfig(""); err != nil || cfg.Mode != "openai" || cfg.localConfigPath == "" {
		t.Fatal("默认文件没有加载", err)
	}
}

func TestLoadConfigStrictJSON(t *testing.T) {
	cleanLocalConfigEnv(t)
	valid := `{"mode":"demo","db_path":"state/db","workspace":"workspace"}`
	cases := []string{
		"", `null`, `[]`, `"test-only-secret"`, `{"mode":null}`, `{"mode":true}`, `{"mode":1}`,
		`{"mode":{}}`, `{"mode":[]}`, `{"mode":"demo","mode":"demo"}`,
		`{"mode":"demo","\u006dode":"demo"}`, `{"Mode":"demo"}`,
		`{"test-only-secret":"test-only-key"}`, `{"api_key":"test-only-key",}`, valid + ` {}`,
		valid + ` null`, valid + ` test-only-secret`, `{"api_key":"test-only-key"`,
		"{\"api_key\":\"\xff\"}", "\xef\xbb\xbf" + valid,
	}
	for i, raw := range cases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private-test-only-path")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err == nil || cfg != (Config{}) {
				t.Fatal("接受了无效 JSON 或返回部分配置")
			}
			for _, forbidden := range []string{path, "test-only-secret", "test-only-key"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatal("错误泄漏了配置内容或路径")
				}
			}
		})
	}
	for _, delta := range []int{0, 1} {
		raw := valid + strings.Repeat(" ", maxLocalConfigBytes-len(valid)+delta)
		path := filepath.Join(t.TempDir(), "config")
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadConfig(path)
		if (delta == 0) != (err == nil) {
			t.Fatal("1 MiB 文件边界错误", err)
		}
	}
}

func TestLoadConfigFileSafety(t *testing.T) {
	cleanLocalConfigEnv(t)
	for _, mode := range []os.FileMode{0400, 0600, 0000, 0200, 0440, 0640, 0644, 0666, 0700, 0500, 0604, 0601, 0600 | os.ModeSetuid} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeLocalConfig(t, t.TempDir(), localConfigValues())
			if err := os.Chmod(path, mode); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode() != mode {
				t.Skip("文件系统未保留指定权限位")
			}
			_, err = LoadConfig(path)
			if (mode == 0400 || mode == 0600) != (err == nil) {
				t.Fatal("权限校验错误", err)
			}
		})
	}
	for _, kind := range []string{"directory", "symlink", "dangling", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.local.json")
			var err error
			switch kind {
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(writeLocalConfig(t, t.TempDir(), localConfigValues()), path)
			case "dangling":
				err = os.Symlink(filepath.Join(root, "missing"), path)
			case "fifo":
				err = syscall.Mkfifo(path, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Chdir(root)
			if _, err = LoadConfig(""); err == nil || strings.Contains(err.Error(), path) {
				t.Fatal("特殊文件被接受或错误泄漏路径")
			}
		})
	}
	// 检查前后 inode 不同必须拒绝，不能只检查文件名和权限。
	path := writeLocalConfig(t, t.TempDir(), localConfigValues())
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	other := writeLocalConfig(t, t.TempDir(), localConfigValues())
	if _, err = readLocalConfigFile(other, before); err == nil {
		t.Fatal("文件身份变化未拒绝")
	}
}

func TestLoadConfigEnvironmentConflicts(t *testing.T) {
	cleanLocalConfigEnv(t)
	values := localConfigValues()
	path := writeLocalConfig(t, t.TempDir(), values)
	for key, value := range map[string]string{"DSH_MODE": "openai", "DSH_MODEL": values["model"], "DSH_API_KEY": values["api_key"], "DSH_BASE_URL": values["base_url"]} {
		for _, envValue := range []string{"test-only-conflict", "", value} {
			t.Run(key+"/"+envValue, func(t *testing.T) {
				t.Setenv(key, envValue)
				_, err := LoadConfig(path)
				if envValue == value {
					if err != nil {
						t.Fatal("相同 ENV 不应冲突", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "test-only-") || strings.Contains(err.Error(), path) {
					t.Fatal("ENV 冲突未拒绝或没有脱敏")
				}
			})
		}
	}
	t.Run("equivalent_endpoint", func(t *testing.T) {
		t.Setenv("DSH_BASE_URL", "https://models.example.test/api/v3/")
		if _, err := LoadConfig(path); err != nil {
			t.Fatal(err)
		}
	})
	for _, key := range []string{"DSH_PROFILE_FILE", "DSH_PROFILE_ID", "DSH_PROFILE_PATCH_FILE", "DSH_PROFILE_FUTURE"} {
		for _, value := range []string{"", "test-only-never-read"} {
			t.Run(key+value, func(t *testing.T) {
				t.Setenv(key, value)
				if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "DSH_PROFILE_") || strings.Contains(err.Error(), "test-only-never-read") {
					t.Fatal("本地配置与 profile 同时设置未拒绝")
				}
			})
		}
	}
}

func TestLoadConfigLegacyProfileAndSnapshot(t *testing.T) {
	cleanLocalConfigEnv(t)
	t.Chdir(t.TempDir())
	setStartupBundle(t, demoStartupProfile("only"))
	cfg, err := LoadConfig("")
	if err != nil || cfg.Mode != "demo" {
		t.Fatal("无本地文件时 profile 回退失败", err)
	}
	// LoadConfig 已生成快照，Open 不应该重新读取被改坏的 profile。
	t.Setenv("DSH_PROFILE_FILE", "must-not-read-again")
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal("Open 意外重新加载 profile", err)
	}
	if err = a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeBaseURL(t *testing.T) {
	for _, suffix := range []string{"", "/", "/chat/completions", "/chat/completions/"} {
		got, err := normalizeBaseURL("https://models.example.test/api/v3" + suffix)
		if err != nil || got != "https://models.example.test/api/v3" {
			t.Fatal("完整接口路径归一化错误", err)
		}
	}
	for _, suffix := range []string{"/chat/completions/chat/completions", "/chat/completions//chat/completions/", "/chat/completions/other"} {
		if _, err := normalizeBaseURL("https://models.example.test/api/v3" + suffix); err == nil {
			t.Fatal("重复接口路径未拒绝")
		}
	}
}

func TestValidateConfigLocalOnly(t *testing.T) {
	cleanLocalConfigEnv(t)
	path := writeLocalConfig(t, t.TempDir(), localConfigValues())
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Config){
		"model":         func(c *Config) { c.Model = " " },
		"key":           func(c *Config) { c.APIKey = "" },
		"key_header":    func(c *Config) { c.APIKey = "test-only-secret\r\nheader" },
		"endpoint":      func(c *Config) { c.BaseURL = "" },
		"insecure":      func(c *Config) { c.BaseURL = "http://remote.example.test" },
		"query":         func(c *Config) { c.BaseURL += "?key=test-only-secret" },
		"userinfo":      func(c *Config) { c.BaseURL = "https://test-only-secret@models.example.test" },
		"bad_url":       func(c *Config) { c.BaseURL = "https://test-only-secret:%zz" },
		"duplicate":     func(c *Config) { c.BaseURL += "/chat/completions/chat/completions" },
		"mode":          func(c *Config) { c.Mode = "test-only-secret" },
		"demo_with_key": func(c *Config) { c.Mode = "demo" },
		"workspace":     func(c *Config) { c.Workspace = "" },
		"db":            func(c *Config) { c.DBPath = "" },
		"mcp":           func(c *Config) { c.MCPEndpoint = "https://test-only-secret@host" },
		"token":         func(c *Config) { c.Token = "test-only-secret\n" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			bad := cfg
			change(&bad)
			if err := ValidateConfig(bad); err == nil || strings.Contains(err.Error(), "test-only-") || strings.Contains(err.Error(), path) {
				t.Fatal("错误配置未拒绝或没有脱敏")
			}
		})
	}
	for _, addr := range []string{":80", "0.0.0.0:80", "localhost:80", "[::]:80", "127.0.0.1:http", "127.0.0.1:-1", "127.0.0.1:+80", "127.0.0.1:65536", "127.0.0.1:"} {
		for _, grpc := range []bool{false, true} {
			bad := cfg
			if grpc {
				bad.GRPCAddr = addr
			} else {
				bad.HTTPAddr = addr
			}
			if ValidateConfig(bad) == nil {
				t.Fatal("非法监听地址被接受")
			}
		}
	}
	// 校验 Config 快照不应该读取进程 ENV，更不能访问故意不可用的远端。
	t.Setenv("DSH_PROFILE_FILE", "must-not-read")
	cfg.PostgresDSN = "postgres://test-only-secret@db.example.test/must-not-connect"
	cfg.MCPEndpoint = "https://mcp.example.test/must-not-connect"
	if err = ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(cfg.Workspace); !os.IsNotExist(err) {
		t.Fatal("校验创建了 workspace")
	}
	if _, err = os.Stat(filepath.Dir(cfg.DBPath)); !os.IsNotExist(err) {
		t.Fatal("校验创建了数据库目录")
	}
}

func TestLocalConfigWorkspaceIsolation(t *testing.T) {
	cleanLocalConfigEnv(t)
	for _, kind := range []string{"same", "ancestor", "symlink", "case_alias"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			configDir := filepath.Join(root, "Private")
			if err := os.Mkdir(configDir, 0700); err != nil {
				t.Fatal(err)
			}
			values := localConfigValues()
			values["db_path"] = filepath.Join(root, "db")
			values["workspace"] = configDir
			switch kind {
			case "ancestor":
				values["workspace"] = root
			case "symlink":
				alias := filepath.Join(root, "alias")
				if err := os.Symlink(configDir, alias); err != nil {
					t.Fatal(err)
				}
				values["workspace"] = alias
			case "case_alias":
				alias := filepath.Join(root, "private")
				a, _ := os.Stat(configDir)
				b, err := os.Stat(alias)
				if os.IsNotExist(err) || err == nil && !os.SameFile(a, b) {
					t.Skip("大小写敏感文件系统")
				}
				if err != nil {
					t.Fatal(err)
				}
				values["workspace"] = alias
			}
			path := writeLocalConfig(t, configDir, values)
			if _, err := LoadConfig(path); err == nil || strings.Contains(err.Error(), path) {
				t.Fatal("配置文件位于 workspace 内仍被接受")
			}
			// 直接构造带来源的快照，验证 Open 在数据库和 MCP 活动之前也会拒绝。
			cfg := Config{Mode: "demo", Workspace: values["workspace"], DBPath: values["db_path"], localConfigPath: path, MCPEndpoint: "https://mcp.example.test/must-not-connect"}
			if _, err := Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "workspace") {
				t.Fatal("Open 未执行配置隔离")
			}
			if _, err := os.Stat(cfg.DBPath); !os.IsNotExist(err) {
				t.Fatal("隔离检查前已经创建数据库")
			}
		})
	}
}

func TestOpenRechecksIsolationAfterCreatingWorkspace(t *testing.T) {
	cleanLocalConfigEnv(t)
	root := t.TempDir()
	values := localConfigValues()
	values["mode"], values["model"], values["api_key"], values["base_url"] = "demo", "demo", "", ""
	// child 还不存在，但创建 child 后 child/.. 就是配置所在目录。
	path := writeLocalConfig(t, root, values)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Workspace = root + string(os.PathSeparator) + "child" + string(os.PathSeparator) + ".."
	if err = ValidateConfig(cfg); err != nil {
		t.Fatal("未创建目录应由 Open 进行最终检查", err)
	}
	if _, err = Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "workspace") {
		t.Fatal("Open 未在解析 workspace 后拒绝同目录")
	}
	if _, err = os.Stat(cfg.DBPath); !os.IsNotExist(err) {
		t.Fatal("隔离检查前创建了数据库")
	}
}

func TestOpenLocalConfigCreatesOnlyDedicatedWorkspace(t *testing.T) {
	cleanLocalConfigEnv(t)
	root := t.TempDir()
	values := localConfigValues()
	values["mode"], values["model"], values["api_key"], values["base_url"] = "demo", "demo", "", ""
	cfg, err := LoadConfig(writeLocalConfig(t, root, values))
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(cfg.Workspace); err != nil {
		t.Fatal("专用 workspace 未创建", err)
	}
	if _, err = os.Stat(cfg.DBPath); err != nil {
		t.Fatal("配置目录相对 DB 未创建", err)
	}
	t.Setenv("DSH_PROFILE_FILE", "must-not-read-or-override")
	if _, err = Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "DSH_PROFILE_") {
		t.Fatal("Open 允许 profile 覆盖本地配置")
	}
}

func TestOpenEmptyModeIsNotDemo(t *testing.T) {
	cleanLocalConfigEnv(t)
	cfg := config(t)
	cfg.Mode = ""
	if _, err := Open(context.Background(), cfg); err == nil {
		t.Fatal("Open 空模式意外启用 demo")
	}
	if _, err := os.Stat(cfg.Workspace); !os.IsNotExist(err) {
		t.Fatal("无效模型配置产生了副作用")
	}
}

func TestLocalConfigRequiresPathsAndNeverInheritsModelCredentials(t *testing.T) {
	cleanLocalConfigEnv(t)
	for _, field := range []string{"workspace", "db_path", "model", "api_key", "base_url"} {
		for _, value := range []string{"", " "} {
			t.Run(field+value, func(t *testing.T) {
				values := localConfigValues()
				values[field] = value
				path := writeLocalConfig(t, t.TempDir(), values)
				if _, err := LoadConfig(path); err == nil {
					t.Fatal("必填配置缺失仍被接受")
				}
			})
		}
	}
	values := localConfigValues()
	delete(values, "api_key")
	path := writeLocalConfig(t, t.TempDir(), values)
	t.Setenv("DSH_API_KEY", "test-only-must-not-inherit")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "DSH_API_KEY") {
		t.Fatal("文件隐式继承了 ENV 凭据")
	}
}

func TestLoadConfigEnvironmentOpenAI(t *testing.T) {
	cleanLocalConfigEnv(t)
	t.Chdir(t.TempDir())
	t.Setenv("DSH_MODEL", "test-only-env-model")
	t.Setenv("DSH_API_KEY", "test-only-env-key")
	t.Setenv("DSH_BASE_URL", "https://models.example.test/api/v3/chat/completions")
	cfg, err := LoadConfig("")
	if err != nil || cfg.Mode != "openai" || cfg.BaseURL != "https://models.example.test/api/v3" || cfg.localConfigPath != "" {
		t.Fatal("无本地文件时没有沿用 ENV openai", err)
	}
	if _, err = os.Stat(".workbuddy"); !os.IsNotExist(err) {
		t.Fatal("ENV 加载产生了文件系统副作用")
	}
}

func TestLoadLocalConfigConcurrent(t *testing.T) {
	cleanLocalConfigEnv(t)
	path := writeLocalConfig(t, t.TempDir(), localConfigValues())
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, err := LoadConfig(path)
			if err != nil || cfg.BaseURL != "https://models.example.test/api/v3" {
				t.Error("并发配置加载失败", err)
			}
		}()
	}
	wg.Wait()
}
