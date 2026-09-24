package main

import (
	"context"
	"os"
	"testing"
	"time"

	"apostas_api/internal/infra/config"
	"apostas_api/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func TestFxLifecycleWithRealDependencies(t *testing.T) {
	if os.Getenv("TEST_FX") != "1" {
		t.Skip("TEST_FX=1 is required")
	}
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	cfg := config.Config{
		Addr: "127.0.0.1:0", DatabaseURL: databaseURL,
		IssuerURL: "http://localhost:8081/realms/apostas", JWKSURL: "http://localhost:8081/realms/apostas/protocol/openid-connect/certs", Audience: "apostas-api",
		ShutdownTimeout: 5 * time.Second, AWSRegion: "us-east-1", AWSEndpoint: "http://localhost:4566", AWSAccessKey: "test", AWSSecretKey: "test",
		InputQueueURL:  "http://localhost:4566/000000000000/wager-transactions.fifo",
		OutputQueueURL: "http://localhost:4566/000000000000/wager-events.fifo",
		DLQURL:         "http://localhost:4566/000000000000/wager-transactions-dlq.fifo",
	}
	var pool *pgxpool.Pool
	var manager *worker.Manager
	options := append(appOptions(cfg), fx.Populate(&pool, &manager))
	app := fx.New(options...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.Done():
	default:
		t.Fatal("workers did not finish before shutdown returned")
	}
	if err := pool.Ping(ctx); err == nil {
		t.Fatal("PostgreSQL pool remained open after shutdown")
	}
}
