// Package app composes the standalone, local, single-user harness.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	migrations "github.com/yy59750901/go-dsh/db"
	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/mcp"
	"github.com/yy59750901/go-dsh/internal/orchestration"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/skill"
	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/workspace"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Config struct {
	HTTPAddr         string
	GRPCAddr         string
	Mode             string
	Model            string
	BaseURL          string
	APIKey           string
	Token            string
	DBPath           string
	PostgresDSN      string
	Workspace        string
	SkillsPath       string
	SystemPrompt     string
	MCPEndpoint      string
	MCPAuthorization string

	localConfigPath string
	configLoaded    bool
}

// ConfigFromEnv reads only named service configuration, never the user's shell
// history, private credentials files or global MCP/Skill directories.
func ConfigFromEnv() Config {
	return Config{
		HTTPAddr: env("DSH_HTTP_ADDR", "127.0.0.1:8080"),
		GRPCAddr: env("DSH_GRPC_ADDR", "127.0.0.1:9090"),
		Mode:     env("DSH_MODE", "openai"), Model: os.Getenv("DSH_MODEL"),
		BaseURL: os.Getenv("DSH_BASE_URL"), APIKey: os.Getenv("DSH_API_KEY"), Token: os.Getenv("DSH_API_TOKEN"),
		DBPath: env("DSH_DB_PATH", ".workbuddy/dsh/dsh.db"), PostgresDSN: os.Getenv("DSH_POSTGRES_DSN"),
		Workspace: env("DSH_WORKSPACE", ".workbuddy/dsh/workspace"), SkillsPath: os.Getenv("DSH_SKILLS_PATH"),
		SystemPrompt: env("DSH_SYSTEM_PROMPT", defaultSystemPrompt),
		MCPEndpoint:  os.Getenv("DSH_MCP_ENDPOINT"), MCPAuthorization: os.Getenv("DSH_MCP_AUTHORIZATION"),
	}
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func ValidateListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return errors.New("invalid listen address")
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return errors.New("invalid listen port")
		}
	}
	if n, err := strconv.Atoi(port); err != nil || n < 0 || n > 65535 {
		return errors.New("invalid listen port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("this single-user build requires a numeric loopback listen address")
	}
	return nil
}

type App struct {
	Orchestration *orchestration.Manager
	Harness       *agent.Harness
	Tools         *tool.Registry
	Mode          string
	Model         string
	database      *sql.DB
	closers       []io.Closer
}

func Open(ctx context.Context, cfg Config) (_ *App, err error) {
	if cfg.localConfigPath != "" {
		if err = rejectLocalProfileEnv(); err != nil {
			return nil, err
		}
	} else if !cfg.configLoaded {
		cfg, err = loadProfileConfig(cfg)
		if err != nil {
			return nil, err
		}
	}
	cfg, err = normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	provider, err := configProvider(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Workspace == "" {
		return nil, errors.New("workspace is required")
	}
	workpath, err := filepath.Abs(cfg.Workspace)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(workpath, 0700); err != nil {
		return nil, errors.New("cannot create workspace")
	}
	workpath, err = filepath.EvalSymlinks(workpath)
	if err != nil {
		return nil, errors.New("cannot resolve workspace")
	}
	if err = validateLocalConfigIsolation(cfg.localConfigPath, workpath); err != nil {
		return nil, err
	}
	var dialector gorm.Dialector
	if cfg.PostgresDSN != "" {
		dialector = postgres.Open(cfg.PostgresDSN)
	} else {
		if cfg.DBPath == "" {
			return nil, errors.New("database path is required")
		}
		dbpath, err := filepath.Abs(cfg.DBPath)
		if err != nil {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Dir(dbpath), 0700); err != nil {
			return nil, errors.New("cannot create database directory")
		}
		dbdir, err := filepath.EvalSymlinks(filepath.Dir(dbpath))
		if err != nil {
			return nil, errors.New("cannot resolve database directory")
		}
		dbpath = filepath.Join(dbdir, filepath.Base(dbpath))
		if info, statErr := os.Lstat(dbpath); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
			return nil, errors.New("database must be a regular file, not a symlink")
		}
		inside, err := directoryWithin(workpath, dbdir)
		if err != nil {
			return nil, errors.New("cannot verify database directory isolation")
		}
		if inside {
			return nil, errors.New("database must remain outside the model-accessible workspace")
		}
		file, err := os.OpenFile(dbpath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, errors.New("cannot open database file")
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
		u := url.URL{Scheme: "file", Path: dbpath}
		u.RawQuery = "_busy_timeout=5000&_foreign_keys=on&_journal_mode=WAL"
		dialector = sqlite.Open(u.String())
	}
	database, err := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, errors.New("cannot open database; check local configuration")
	}
	sqlDB, err := database.DB()
	if err != nil {
		return nil, errors.New("cannot obtain database connection")
	}
	a := &App{Mode: cfg.Mode, Model: cfg.Model, database: sqlDB, Tools: tool.NewRegistry()}
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = a.Close(closeCtx)
		}
	}()
	if cfg.PostgresDSN == "" {
		sqlDB.SetMaxOpenConns(1)
	} else {
		sqlDB.SetMaxOpenConns(10)
	}
	if err = migrations.Migrate(ctx, database); err != nil {
		return nil, errors.New("database migration failed; no destructive migration was attempted")
	}
	w, err := workspace.Open(workpath)
	if err != nil {
		return nil, err
	}
	a.closers = append(a.closers, w)
	localTools := append([]tool.Tool{tool.NewEchoTool()}, w.Tools()...)
	if cfg.SkillsPath != "" {
		catalog, e := skill.NewCatalog(workpath, cfg.SkillsPath)
		if e != nil {
			return nil, fmt.Errorf("workspace skill catalog: %w", e)
		}
		a.closers = append(a.closers, catalog)
		localTools = append(localTools, catalog.Tools()...)
	}
	for _, t := range localTools {
		if err = a.Tools.Register(t); err != nil {
			return nil, err
		}
	}
	if cfg.MCPEndpoint != "" {
		client, e := mcp.NewStreamableHTTPClient(mcp.Config{Endpoint: cfg.MCPEndpoint, Authorization: cfg.MCPAuthorization})
		if e != nil {
			return nil, e
		}
		a.closers = append(a.closers, client)
		if err = client.Initialize(ctx); err != nil {
			return nil, errors.New("MCP initialization failed")
		}
		remote, e := client.Tools(ctx)
		if e != nil {
			return nil, fmt.Errorf("MCP tool definitions rejected: %w", e)
		}
		for _, t := range remote {
			if _, e = a.Tools.Resolve(t.Definition().Name); e == nil {
				return nil, errors.New("MCP tool name conflicts with a local tool")
			}
			if err = a.Tools.Register(t); err != nil {
				return nil, err
			}
		}
	}
	a.Harness, err = agent.NewHarness(gormrepo.NewEventStore(database), provider, a.Tools, agent.HarnessOptions{Model: cfg.Model, SystemPrompt: cfg.SystemPrompt})
	if err != nil {
		return nil, err
	}
	a.Orchestration, err = orchestration.New(database, a.Harness)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// directoryWithin compares filesystem identities, not spelling. This also
// catches case aliases on APFS/HFS and directory aliases on Windows.
func directoryWithin(root, directory string) (bool, error) {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return false, err
	}
	for {
		info, err := os.Stat(directory)
		if err != nil {
			return false, err
		}
		if os.SameFile(rootInfo, info) {
			return true, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return false, nil
		}
		directory = parent
	}
}

// Close drains Agent work before closing tools and storage. On a drain timeout,
// keep dependencies alive: closing a DB under an active writer is unsafe.
func (a *App) Close(ctx context.Context) error {
	if a.Orchestration != nil {
		if err := a.Orchestration.Close(ctx); err != nil {
			return err
		}
	}
	if a.Harness != nil {
		if err := a.Harness.Close(ctx); err != nil {
			return err
		}
	}
	var errs []error
	for i := len(a.closers) - 1; i >= 0; i-- {
		errs = append(errs, a.closers[i].Close())
	}
	if a.database != nil {
		errs = append(errs, a.database.Close())
	}
	return errors.Join(errs...)
}
