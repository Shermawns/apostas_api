package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentWagersAcrossIndependentProcesses(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema := "concurrency_" + uuid.NewString()[0:8]
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")

	pools := []*pgxpool.Pool{openProcessPool(t, ctx, databaseURL, schema), openProcessPool(t, ctx, databaseURL, schema), openProcessPool(t, ctx, databaseURL, schema)}
	for _, pool := range pools {
		defer pool.Close()
	}
	if err := createConcurrencySchema(ctx, pools[0]); err != nil {
		t.Fatal(err)
	}

	money, _ := model.ParseMoney("100.00", "BRL")
	wallet, err := model.NewWallet(uuid.New(), uuid.New(), money, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	stores := make([]*postgres.Store, len(pools))
	wagers := make([]*usecases.Wager, len(pools))
	for i, pool := range pools {
		stores[i] = postgres.NewStore(pool)
		wagers[i] = usecases.NewWager(stores[i])
	}
	if err := stores[0].CreateWallet(ctx, wallet); err != nil {
		t.Fatal(err)
	}

	stake, _ := model.ParseMoney("80.00", "BRL")
	operations := []usecases.Operation{
		operation(wallet, "bet-a", "key-a", stake),
		operation(wallet, "bet-b", "key-b", stake),
		operation(wallet, "bet-a", "key-a", stake),
	}
	start := make(chan struct{})
	results := make(chan usecases.Result, len(operations))
	errs := make(chan error, len(operations))
	var group sync.WaitGroup
	for i := range operations {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			result, err := wagers[i].Process(ctx, operations[i])
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	group.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatal(err)
	}

	processed := map[uuid.UUID]struct{}{}
	rejected := map[uuid.UUID]struct{}{}
	for result := range results {
		switch result.Status {
		case model.Processed:
			processed[result.TransactionID] = struct{}{}
		case model.Rejected:
			if result.FailureCode != "INSUFFICIENT_FUNDS" {
				t.Fatalf("unexpected rejection %s", result.FailureCode)
			}
			rejected[result.TransactionID] = struct{}{}
		default:
			t.Fatalf("unexpected status %s", result.Status)
		}
	}
	if len(processed) != 1 || len(rejected) != 1 {
		t.Fatalf("processed=%d rejected=%d", len(processed), len(rejected))
	}
	stored, err := stores[0].GetWallet(ctx, wallet.ID())
	if err != nil || stored.Balance().Minor() != 2000 {
		t.Fatalf("wallet=%+v err=%v", stored, err)
	}
	page, err := stores[0].ListLedger(ctx, wallet.ID(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	debits := 0
	for _, entry := range page.Entries {
		if entry.Direction == "DEBIT" {
			debits++
		}
	}
	if debits != 1 {
		t.Fatalf("debits=%d", debits)
	}
	for i := 0; i < 2; i++ {
		result, err := wagers[i].Process(ctx, operations[i])
		if err != nil || !result.IdempotentReplay {
			t.Fatalf("replay result=%+v err=%v", result, err)
		}
	}
	assertIndependentWalletsAdvance(ctx, t, stores, wagers, money, stake)
	assertFiftyDuplicateRequests(ctx, t, stores, wagers)
}

func assertFiftyDuplicateRequests(ctx context.Context, t *testing.T, stores []*postgres.Store, wagers []*usecases.Wager) {
	t.Helper()
	initial, _ := model.ParseMoney("100.00", "BRL")
	stake, _ := model.ParseMoney("25.00", "BRL")
	wallet, err := model.NewWallet(uuid.New(), uuid.New(), initial, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := stores[0].CreateWallet(ctx, wallet); err != nil {
		t.Fatal(err)
	}
	op := operation(wallet, "fifty-replay", "fifty-key", stake)
	start := make(chan struct{})
	errs := make(chan error, 50)
	var group sync.WaitGroup
	for i := 0; i < 50; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			_, err := wagers[i%len(wagers)].Process(ctx, op)
			if err != nil {
				errs <- err
			}
		}(i)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	stored, err := stores[0].GetWallet(ctx, wallet.ID())
	if err != nil || stored.Balance().Minor() != 7500 {
		t.Fatalf("wallet=%+v err=%v", stored, err)
	}
	page, err := stores[0].ListLedger(ctx, wallet.ID(), nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	debits := 0
	for _, entry := range page.Entries {
		if entry.Direction == "DEBIT" {
			debits++
		}
	}
	if debits != 1 {
		t.Fatalf("debits=%d", debits)
	}
}

func assertIndependentWalletsAdvance(ctx context.Context, t *testing.T, stores []*postgres.Store, wagers []*usecases.Wager, initial, stake model.Money) {
	t.Helper()
	wallets := make([]model.Wallet, 2)
	for i := range wallets {
		wallet, err := model.NewWallet(uuid.New(), uuid.New(), initial, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := stores[i].CreateWallet(ctx, wallet); err != nil {
			t.Fatal(err)
		}
		wallets[i] = wallet
	}
	start := make(chan struct{})
	errs := make(chan error, len(wallets))
	var group sync.WaitGroup
	for i := range wallets {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			result, err := wagers[i].Process(ctx, operation(wallets[i], "parallel-"+string(rune('a'+i)), "parallel-key-"+string(rune('a'+i)), stake))
			if err != nil || result.Status != model.Processed {
				errs <- fmt.Errorf("wallet %d result=%+v err=%w", i, result, err)
			}
		}(i)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func createConcurrencySchema(ctx context.Context, pool *pgxpool.Pool) error {
	migrations := []string{
		filepath.Join("..", "..", "..", "migrations", "000001_init.up.sql"),
		filepath.Join("..", "..", "..", "migrations", "000002_wallet_integrity.up.sql"),
		filepath.Join("..", "..", "..", "migrations", "000003_audit_integrity.up.sql"),
	}
	for _, path := range migrations {
		sql, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return err
		}
	}
	return nil
}

func openProcessPool(t *testing.T, ctx context.Context, databaseURL, schema string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func operation(wallet model.Wallet, externalID, key string, money model.Money) usecases.Operation {
	return usecases.Operation{ProviderID: "provider", ExternalTransactionID: externalID, IdempotencyKey: key, PlayerID: wallet.PlayerID(), WalletID: wallet.ID(), RoundID: "round", GameID: "game", Kind: model.Bet, Money: money}
}
