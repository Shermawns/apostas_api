package integration

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRestartPreservesIdempotencyAndPendingReference(t *testing.T) {
	if os.Getenv("TEST_RESTART") != "1" {
		t.Skip("TEST_RESTART=1 is required")
	}
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "apostas-api")
	if os.PathSeparator == '\\' {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/api")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build API: %v: %s", err, output)
	}
	port := freePort(t)
	base := "http://127.0.0.1:" + port
	start := func() (*exec.Cmd, *bytes.Buffer) {
		return startAPI(t, ctx, root, binary, port, databaseURL)
	}
	api, logs := start()
	stop := func() {
		if api.Process != nil {
			_ = api.Process.Kill()
			_ = api.Wait()
		}
	}
	defer stop()
	waitForAPI(t, ctx, base, logs)

	client := &http.Client{Timeout: 5 * time.Second}
	walletToken := token(t, client, "http://localhost:8081", "wallet-service", "wallet-service-local-only")
	providerToken := token(t, client, "http://localhost:8081", "provider-a", "provider-a-local-only")
	playerID := uuid.New()
	opening := map[string]any{"playerId": playerID, "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}
	var wallet walletResponse
	if status := request(t, client, http.MethodPost, base+"/wallets", walletToken, opening, &wallet); status != http.StatusCreated {
		t.Fatalf("open wallet status=%d", status)
	}
	externalID := uuid.NewString()
	op := map[string]any{"providerId": "provider-a", "externalTransactionId": externalID, "playerId": playerID, "walletId": wallet.ID, "roundId": "restart-round", "gameId": "restart-game", "kind": "BET", "money": map[string]string{"amount": "20.00", "currency": "BRL"}}
	var first usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, op, &first, map[string]string{"Idempotency-Key": "provider-a:" + externalID}); status != http.StatusOK || first.Status != model.Processed {
		t.Fatalf("initial wager status=%d result=%+v", status, first)
	}
	pendingExternalID := uuid.NewString()
	pending := map[string]any{"providerId": "provider-a", "externalTransactionId": pendingExternalID, "playerId": playerID, "walletId": wallet.ID, "roundId": "restart-round", "gameId": "restart-game", "kind": "REFUND", "money": map[string]string{"amount": "20.00", "currency": "BRL"}, "referenceExternalTransactionId": "not-yet-arrived"}
	var pendingResult usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, pending, &pendingResult, map[string]string{"Idempotency-Key": "provider-a:" + pendingExternalID}); status != http.StatusAccepted || pendingResult.Status != model.PendingReference {
		t.Fatalf("pending wager status=%d result=%+v", status, pendingResult)
	}

	stop()
	api, logs = start()
	waitForAPI(t, ctx, base, logs)
	var replay usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, op, &replay, map[string]string{"Idempotency-Key": "provider-a:" + externalID}); status != http.StatusOK || !replay.IdempotentReplay || replay.TransactionID != first.TransactionID {
		t.Fatalf("replay after restart status=%d result=%+v", status, replay)
	}
	var status model.Status
	if err := pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id=$1`, pendingResult.TransactionID).Scan(&status); err != nil || status != model.PendingReference {
		t.Fatalf("pending transaction after restart status=%s err=%v", status, err)
	}
	var reconciliation usecases.Reconciliation
	if code := request(t, client, http.MethodPost, base+"/wallets/"+wallet.ID.String()+"/reconciliation", walletToken, nil, &reconciliation); code != http.StatusOK || !reconciliation.Consistent {
		t.Fatalf("reconciliation after restart status=%d result=%+v", code, reconciliation)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

func startAPI(t *testing.T, ctx context.Context, root, binary, port, databaseURL string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	command := exec.CommandContext(ctx, binary)
	command.Dir = root
	command.Stdout, command.Stderr = logs, logs
	command.Env = append(os.Environ(),
		"APP_ADDR=127.0.0.1:"+port,
		"DATABASE_URL="+databaseURL,
		"AWS_REGION=us-east-1", "AWS_ENDPOINT_URL=http://localhost:4566", "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test",
		"AWS_SQS_INPUT_QUEUE_URL=http://localhost:4566/000000000000/wager-transactions.fifo",
		"AWS_SQS_OUTPUT_QUEUE_URL=http://localhost:4566/000000000000/wager-events.fifo",
		"AWS_SQS_DLQ_URL=http://localhost:4566/000000000000/wager-transactions-dlq.fifo",
		"OIDC_ISSUER_URL=http://localhost:8081/realms/apostas", "OIDC_JWKS_URL=http://localhost:8081/realms/apostas/protocol/openid-connect/certs", "OIDC_AUDIENCE=apostas-api",
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	return command, logs
}

func waitForAPI(t *testing.T, ctx context.Context, base string, logs *bytes.Buffer) {
	t.Helper()
	for {
		response, err := http.Get(base + "/health/ready")
		if err == nil && response.StatusCode == http.StatusOK {
			response.Body.Close()
			return
		}
		if response != nil {
			response.Body.Close()
		}
		if ctx.Err() != nil {
			t.Fatalf("API did not become ready: %v\n%s", err, logs.String())
		}
		time.Sleep(150 * time.Millisecond)
	}
}
