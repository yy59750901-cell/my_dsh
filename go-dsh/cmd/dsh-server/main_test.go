package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yy59750901/go-dsh/internal/app"
)

func cleanServerEnv(t *testing.T) {
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

func serverConfig(t *testing.T) (string, map[string]string) {
	t.Helper()
	root := t.TempDir()
	values := map[string]string{
		"mode": "openai", "model": "test-model", "api_key": "test-only-private-key",
		"base_url":  "https://models.example.test/api/v3/chat/completions",
		"http_addr": "127.0.0.1:8080", "grpc_addr": "127.0.0.1:9090",
		"workspace": filepath.Join(root, "must-not-create-workspace"), "db_path": filepath.Join(root, "must-not-create-db", "dsh.db"),
		"api_token": "test-only-private-token", "system_prompt": "test-only-private-prompt",
		"mcp_endpoint": "https://mcp.example.test/must-not-connect", "mcp_authorization": "Bearer test-only-private-mcp",
	}
	return filepath.Join(root, "config.local.json"), values
}

func saveServerConfig(t *testing.T, path string, values map[string]string) {
	t.Helper()
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckConfigOnlyPrintsSafeSummary(t *testing.T) {
	cleanServerEnv(t)
	path, values := serverConfig(t)
	// 不可连接的 DSN 只能被当成不透明配置，检查过程不能打开它。
	values["postgres_dsn"] = "postgres://test-only-private-dsn@db.example.test/must-not-connect"
	saveServerConfig(t, path, values)
	t.Chdir(t.TempDir())
	globalFlags := flag.CommandLine
	for range 2 {
		var output bytes.Buffer
		if err := runArgs([]string{"-config", path, "-check-config"}, &output); err != nil {
			t.Fatal(err)
		}
		want := "mode=\"openai\" model=\"test-model\" endpoint=\"https://models.example.test/api/v3\" http_listen=\"127.0.0.1:8080\" grpc_listen=\"127.0.0.1:9090\"\n"
		if output.String() != want {
			t.Fatal("检查输出不是预期的白名单摘要")
		}
		for _, key := range []string{"api_key", "api_token", "postgres_dsn", "workspace", "db_path", "system_prompt", "mcp_endpoint", "mcp_authorization"} {
			if strings.Contains(output.String(), values[key]) {
				t.Fatal("输出泄漏敏感字段")
			}
		}
	}
	if globalFlags != flag.CommandLine || flag.CommandLine.Lookup("config") != nil || flag.CommandLine.Lookup("check-config") != nil {
		t.Fatal("污染了全局 FlagSet")
	}
	for _, path := range []string{values["workspace"], filepath.Dir(values["db_path"])} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("check-config 产生了目录或数据库副作用")
		}
	}
}

func TestCheckConfigDefaultFileAndExplicitDemo(t *testing.T) {
	cleanServerEnv(t)
	path, values := serverConfig(t)
	values["mode"], values["model"], values["api_key"], values["base_url"] = "demo", "", "", ""
	saveServerConfig(t, path, values)
	t.Chdir(filepath.Dir(path))
	var output bytes.Buffer
	if err := runArgs([]string{"-check-config"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `mode="demo" model="demo" endpoint=""`) {
		t.Fatal("显式 demo 检查失败")
	}
	if _, err := os.Stat(values["workspace"]); !os.IsNotExist(err) {
		t.Fatal("check-config 创建了 workspace")
	}
}

func TestConfigFailureBeforeListenAndOpen(t *testing.T) {
	cleanServerEnv(t)
	for _, check := range []bool{false, true} {
		for _, kind := range []string{"missing_model", "missing_key", "bad_listener", "query", "workspace"} {
			t.Run(kind, func(t *testing.T) {
				path, values := serverConfig(t)
				switch kind {
				case "missing_model":
					delete(values, "model")
				case "missing_key":
					delete(values, "api_key")
				case "bad_listener":
					values["http_addr"] = "0.0.0.0:8080"
				case "query":
					values["base_url"] += "?key=test-only-private-query"
				case "workspace":
					values["workspace"] = filepath.Dir(path)
				}
				saveServerConfig(t, path, values)
				args := []string{"-config", path}
				if check {
					args = append(args, "-check-config")
				}
				var output bytes.Buffer
				err := runArgs(args, &output)
				if err == nil || output.Len() != 0 || strings.Contains(err.Error(), "test-only-private") || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "listen") {
					t.Fatal("没有在监听之前拒绝配置或没有脱敏")
				}
				if _, err := os.Stat(filepath.Dir(values["db_path"])); !os.IsNotExist(err) {
					t.Fatal("失败检查创建了数据库目录")
				}
			})
		}
	}
}

func TestRunArgsErrorsDoNotEchoArguments(t *testing.T) {
	cleanServerEnv(t)
	t.Chdir(t.TempDir())
	for _, args := range [][]string{
		{"-config", "test-only-private-path", "-check-config"},
		{"-test-only-private-unknown"}, {"-check-config=test-only-private-value"},
		{"test-only-private-positional"}, {"-config"},
	} {
		var output bytes.Buffer
		err := runArgs(args, &output)
		if err == nil || output.Len() != 0 || strings.Contains(err.Error(), "test-only-private") {
			t.Fatal("参数错误泄漏原始内容")
		}
	}
	var output bytes.Buffer
	if err := runArgs([]string{"-h"}, &output); err != nil || !strings.Contains(output.String(), "-check-config") {
		t.Fatal("帮助参数不可用", err)
	}
}

func TestSummaryRedactsQueryAndEscapedSecrets(t *testing.T) {
	cfg := app.Config{
		Mode: "openai", Model: "test-only-key\"quoted", APIKey: "test-only-key\"quoted",
		BaseURL:  "https://test-only-user:password@models.example.test/api/v3?secret=test-only-query#test-only-fragment",
		HTTPAddr: "127.0.0.1:8080", GRPCAddr: "127.0.0.1:9090",
	}
	var output bytes.Buffer
	if err := printConfigSummary(&output, cfg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "test-only-") || strings.ContainsAny(output.String(), "?#@") || !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatal("摘要未清除 query、userinfo、fragment 或重复秘密")
	}
}

func TestRunEntryDefaultsToOpenAIAndIgnoresTestFlags(t *testing.T) {
	cleanServerEnv(t)
	t.Chdir(t.TempDir())
	old := os.Args
	os.Args = []string{"dsh-server.test", "-test-only-private-flag"}
	t.Cleanup(func() { os.Args = old })
	if err := run(); err == nil || !strings.Contains(err.Error(), "openai") || strings.Contains(err.Error(), "test-only-private") {
		t.Fatal("run 测试入口未保留或默认启用了 demo")
	}
}
