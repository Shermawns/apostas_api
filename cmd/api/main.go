package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"apostas_api/internal/controller"
	"apostas_api/internal/infra/config"
	"apostas_api/internal/infra/oidc"
	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/infra/queue"
	"apostas_api/internal/usecases"
	"apostas_api/internal/worker"
)

func main() {
	_ = godotenv.Load()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration validation failed", "error", err)
		os.Exit(1)
	}
	fx.New(appOptions(cfg)...).Run()
}

func appOptions(cfg config.Config) []fx.Option {
	return []fx.Option{
		fx.StartTimeout(90 * time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.WithLogger(func() fxevent.Logger { return &fxevent.SlogLogger{Logger: slog.Default()} }),
		fx.Module("configuration", fx.Supply(cfg)),
		fx.Module("persistence", fx.Provide(openPool, postgres.NewStore,
			func(s *postgres.Store) usecases.WalletStore { return s },
			func(s *postgres.Store) usecases.WagerStore { return s },
			func(s *postgres.Store) usecases.ReadStore { return s },
		)),
		fx.Module("messaging", fx.Provide(queue.NewClient, worker.NewInputConsumer, worker.NewOutboxPublisher, worker.NewReferenceResolver, worker.NewManager)),
		fx.Module("application", fx.Provide(usecases.NewWallets, usecases.NewWager, usecases.NewReader, oidc.NewAuth, controller.NewHandler)),
		fx.Invoke(worker.Register, startHTTP),
	}
}

func openPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

func startHTTP(lc fx.Lifecycle, cfg config.Config, handler *controller.Handler, auth *oidc.Auth) {
	server := &http.Server{Addr: cfg.Addr, Handler: auth.Middleware(handler.Routes()), ReadHeaderTimeout: 5 * time.Second}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for {
				if err := auth.Ready(ctx); err == nil {
					break
				} else if ctx.Err() != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
			listener, err := net.Listen("tcp", cfg.Addr)
			if err != nil {
				return err
			}
			go func() {
				if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("http server stopped", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error { return server.Shutdown(ctx) },
	})
}
