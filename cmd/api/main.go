package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"go.uber.org/fx"

	"apostas_api/internal/controller"
	"apostas_api/internal/infra/config"
	"apostas_api/internal/infra/oidc"
	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/usecases"
)

func main() {
	_ = godotenv.Load()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	fx.New(
		fx.Module("configuration", fx.Provide(config.Load)),
		fx.Module("persistence", fx.Provide(openPool, postgres.NewStore,
			func(s *postgres.Store) usecases.WalletStore { return s },
			func(s *postgres.Store) usecases.WagerStore { return s },
		)),
		fx.Module("application", fx.Provide(usecases.NewWallets, usecases.NewWager, oidc.NewAuth, controller.NewHandler)),
		fx.Invoke(startHTTP),
	).Run()
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
		OnStart: func(context.Context) error {
			go func() {
				if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("http server stopped", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error { return server.Shutdown(ctx) },
	})
}
