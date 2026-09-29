package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yy59750901/go-dsh/internal/app"
	"github.com/yy59750901/go-dsh/internal/transport/grpcapi"
	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
)

const shutdownTimeout = 40 * time.Second

func main() {
	if err := runArgs(os.Args[1:], os.Stdout); err != nil {
		slog.Error("dsh server stopped", "error", err)
		os.Exit(1)
	}
}

// run 保留不接收进程参数的测试入口，避免读取 go test 自身的 flags。
func run() error {
	return runArgs(nil, os.Stdout)
}

func runArgs(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("dsh-server", flag.ContinueOnError)
	// flag 的默认错误会回显参数及路径，统一改成固定错误。
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "本地配置路径，默认当前目录的 config.local.json")
	checkConfig := flags.Bool("check-config", false, "仅检查配置，不监听、不创建数据库、不恢复任务")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, err = fmt.Fprintln(output, "用法: dsh-server [-config path] [-check-config]")
			return err
		}
		return errors.New("命令行参数无效；使用 -config path 和 -check-config")
	}
	if flags.NArg() != 0 {
		return errors.New("不支持位置参数")
	}
	cfg, err := app.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	if err = app.ValidateConfig(cfg); err != nil {
		return err
	}
	if *checkConfig {
		return printConfigSummary(output, cfg)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Bind before migration/recovery: failure to bind must not begin model work.
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer grpcListener.Close()
	application, err := app.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := application.Close(closeCtx); err != nil {
			slog.Error("harness shutdown incomplete", "error", err)
		}
	}()
	gin.SetMode(gin.ReleaseMode)
	gateway := gin.New()
	gateway.Use(gin.Recovery())
	gateway.Any("/*path", gin.WrapH(httpapi.NewHandlerWithHarness(application.Harness, httpapi.Options{Token: cfg.Token, Mode: application.Mode, Model: application.Model, Tools: application.Tools, Orchestration: application.Orchestration})))
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr, Handler: gateway, ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	grpcServer := grpcapi.NewServerWithHarness(application.Harness, application.Tools, cfg.Token)
	errCh := make(chan error, 3)
	go func() {
		if err := application.Orchestration.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("orchestration stopped: %w", err)
		}
	}()
	go func() {
		if e := httpServer.Serve(httpListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve http: %w", e)
		}
	}()
	go func() {
		if e := grpcServer.Serve(grpcListener); e != nil {
			errCh <- fmt.Errorf("serve grpc: %w", e)
		}
	}()
	slog.Info("local single-user DSH ready", "http", "http://"+httpListener.Addr().String(), "grpc", grpcListener.Addr().String(), "mode", application.Mode, "model", application.Model)
	if application.Mode == "demo" {
		slog.Warn("DEMO MODE: deterministic local simulator, not a real language model")
	}
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	grpcStopped := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcStopped) }()
	httpErr := httpServer.Shutdown(shutdownCtx)
	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	return errors.Join(runErr, httpErr)
}

func printConfigSummary(output io.Writer, cfg app.Config) error {
	endpoint := ""
	if u, err := url.Parse(cfg.BaseURL); err == nil {
		u.User, u.RawQuery, u.Fragment, u.RawFragment, u.ForceQuery = nil, "", "", "", false
		endpoint = u.String()
	}
	fields := []string{cfg.Mode, cfg.Model, endpoint, cfg.HTTPAddr, cfg.GRPCAddr}
	// 仅白名单字段；先脱敏再转义，防止秘密包含引号时在格式化后漏掉匹配。
	for i := range fields {
		for _, secret := range []string{cfg.APIKey, cfg.Token, cfg.PostgresDSN, cfg.MCPAuthorization} {
			if secret != "" {
				fields[i] = strings.ReplaceAll(fields[i], secret, "[REDACTED]")
			}
		}
	}
	if _, err := fmt.Fprintf(output, "mode=%q model=%q endpoint=%q http_listen=%q grpc_listen=%q\n", fields[0], fields[1], fields[2], fields[3], fields[4]); err != nil {
		return errors.New("无法输出配置检查结果")
	}
	return nil
}
