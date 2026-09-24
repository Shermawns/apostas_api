package postgres_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestThreeIndependentProcesses(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	schema := "processes_" + uuid.NewString()[:8]
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	pool := openProcessPool(t, ctx, databaseURL, schema)
	defer pool.Close()
	if err := createConcurrencySchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	initial, _ := model.ParseMoney("100.00", "BRL")
	wallet, err := model.NewWallet(uuid.New(), uuid.New(), initial, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(pool)
	if err := store.CreateWallet(ctx, wallet); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "wager-process")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/process")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v: %s", err, output)
	}
	type child struct {
		cmd    *exec.Cmd
		input  io.WriteCloser
		output *bufio.Reader
	}
	children := make([]child, 3)
	for i, pair := range [][2]string{{"bet-a", "key-a"}, {"bet-b", "key-b"}, {"bet-a", "key-a"}} {
		command := exec.CommandContext(ctx, binary, wallet.ID().String(), wallet.PlayerID().String(), pair[0], pair[1])
		command.Env = append(os.Environ(), "TEST_SCHEMA="+schema)
		input, err := command.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		output, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(output)
		ready, err := reader.ReadString('\n')
		if err != nil || strings.TrimSpace(ready) != "READY" {
			t.Fatalf("child ready=%q err=%v", ready, err)
		}
		children[i] = child{cmd: command, input: input, output: reader}
	}
	for _, child := range children {
		_, _ = io.WriteString(child.input, "GO\n")
	}
	processed, rejected := map[uuid.UUID]struct{}{}, map[uuid.UUID]struct{}{}
	for _, child := range children {
		var result usecases.Result
		if err := json.NewDecoder(child.output).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if err := child.cmd.Wait(); err != nil {
			t.Fatal(err)
		}
		switch result.Status {
		case model.Processed:
			processed[result.TransactionID] = struct{}{}
		case model.Rejected:
			if result.FailureCode != "INSUFFICIENT_FUNDS" {
				t.Fatalf("failure=%s", result.FailureCode)
			}
			rejected[result.TransactionID] = struct{}{}
		default:
			t.Fatalf("status=%s", result.Status)
		}
	}
	if len(processed) != 1 || len(rejected) != 1 {
		t.Fatalf("processed=%d rejected=%d", len(processed), len(rejected))
	}
	stored, err := store.GetWallet(ctx, wallet.ID())
	if err != nil || stored.Balance().Minor() != 2000 {
		t.Fatalf("balance=%v err=%v", stored.Balance().Minor(), err)
	}
	page, err := store.ListLedger(ctx, wallet.ID(), nil, 10)
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
