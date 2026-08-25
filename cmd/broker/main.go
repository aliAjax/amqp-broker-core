package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/enterprise/amqp-broker-core/internal/broker"
	"github.com/enterprise/amqp-broker-core/internal/config"
	"github.com/enterprise/amqp-broker-core/internal/platform/grpcx"
	"github.com/enterprise/amqp-broker-core/internal/platform/httpx"
	"github.com/enterprise/amqp-broker-core/internal/worker"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "configs/broker.yaml", "path to YAML config")
	flag.Parse()
	if err := run(*configPath); err != nil {
		slog.Error("broker stopped", "error", err)
		os.Exit(1)
	}
}
func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	runtime, err := broker.Build(broker.Settings{NodeID: cfg.NodeID, AMQPAddress: cfg.AMQP.Address, MaxFrame: cfg.AMQP.MaxFrameSize, IdleTimeout: cfg.AMQP.IdleTimeout, MaxConnections: cfg.AMQP.MaxConnections, DataDir: cfg.Storage.DataDir, LeaseTTL: cfg.Workers.LeaseTTL, Shards: 32, Logger: logger, AuthToken: cfg.Auth.Token, AllowAnonymous: cfg.Auth.AllowAnonymous})
	if err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = runtime.Start(rootCtx); err != nil {
		return err
	}
	middleware := &httpx.Middleware{Token: cfg.Auth.Token, AllowAnonymous: cfg.Auth.AllowAnonymous, Timeout: 15 * time.Second, BodyLimit: cfg.Limits.BodyBytes, Rate: cfg.Limits.RequestsPerSecond, Logger: logger}
	api := &httpx.Server{Runtime: runtime}
	httpServer := &http.Server{Addr: cfg.HTTP.Address, Handler: api.Handler(middleware), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	grpcServer := &grpcx.Server{Address: cfg.GRPC.Address, Runtime: runtime, Token: cfg.Auth.Token, AllowAnonymous: cfg.Auth.AllowAnonymous}
	if err = grpcServer.Start(rootCtx); err != nil {
		return shutdownRuntime(runtime, cfg.Shutdown, err)
	}
	workers := worker.New(runtime, cfg.Workers.SweepInterval, logger)
	workers.Start(rootCtx)
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("broker listeners ready", "http", cfg.HTTP.Address, "grpc", cfg.GRPC.Address, "amqp", cfg.AMQP.Address, "node", cfg.NodeID)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	select {
	case <-rootCtx.Done():
	case err = <-serveErr:
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
	defer cancel()
	var joined []error
	if e := httpServer.Shutdown(shutdownCtx); e != nil {
		joined = append(joined, e)
	}
	if e := grpcServer.Close(shutdownCtx); e != nil {
		joined = append(joined, e)
	}
	if e := workers.Stop(shutdownCtx); e != nil {
		joined = append(joined, e)
	}
	if e := runtime.Close(shutdownCtx); e != nil {
		joined = append(joined, e)
	}
	logger.Info("broker shutdown complete")
	return errors.Join(joined...)
}
func shutdownRuntime(r *broker.Runtime, timeout time.Duration, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return errors.Join(cause, r.Close(ctx))
}
