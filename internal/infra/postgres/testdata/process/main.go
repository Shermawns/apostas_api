package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 5 {
		return fmt.Errorf("expected wallet, player, external ID and key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		return err
	}
	cfg.MaxConns = 1
	schema := os.Getenv("TEST_SCHEMA")
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	walletID, err := uuid.Parse(os.Args[1])
	if err != nil {
		return err
	}
	playerID, err := uuid.Parse(os.Args[2])
	if err != nil {
		return err
	}
	money, err := model.ParseMoney("80.00", "BRL")
	if err != nil {
		return err
	}
	fmt.Println("READY")
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		return err
	}
	result, err := usecases.NewWager(postgres.NewStore(pool)).Process(ctx, usecases.Operation{
		ProviderID: "provider", ExternalTransactionID: os.Args[3], IdempotencyKey: os.Args[4],
		WalletID: walletID, PlayerID: playerID, RoundID: "round", GameID: "game", Kind: model.Bet, Money: money,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
