package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type walletResponse struct {
	ID       uuid.UUID   `json:"id"`
	PlayerID uuid.UUID   `json:"playerId"`
	Balance  model.Money `json:"balance"`
	Version  int64       `json:"version"`
}

type errorResponse struct {
	Code string `json:"code"`
}

func TestAuthenticatedHTTPAndSQS(t *testing.T) {
	if os.Getenv("TEST_E2E") != "1" {
		t.Skip("TEST_E2E=1 is required")
	}
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://localhost:8080"
	idp := "http://localhost:8081"
	localstack := "http://localhost:4566"
	walletToken := token(t, client, idp, "wallet-service", "wallet-service-local-only")
	providerToken := token(t, client, idp, "provider-a", "provider-a-local-only")
	otherToken := token(t, client, idp, "provider-b", "provider-b-local-only")

	playerID := uuid.New()
	opening := map[string]any{"playerId": playerID, "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}
	if status := request(t, client, http.MethodPost, base+"/wallets", "", opening, nil); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated wallet status=%d", status)
	}
	if status := request(t, client, http.MethodPost, base+"/wallets", "not-a-jwt", opening, nil); status != http.StatusUnauthorized {
		t.Fatalf("invalid-token wallet status=%d", status)
	}
	expiredToken := expiredProviderToken(t, client, idp)
	if status := request(t, client, http.MethodPost, base+"/wallets", expiredToken, opening, nil); status != http.StatusUnauthorized {
		t.Fatalf("expired-token wallet status=%d", status)
	}
	if status := request(t, client, http.MethodPost, base+"/wallets", providerToken, opening, nil); status != http.StatusForbidden {
		t.Fatalf("provider wallet status=%d", status)
	}
	var wallet walletResponse
	if status := request(t, client, http.MethodPost, base+"/wallets", walletToken, opening, &wallet); status != http.StatusCreated {
		t.Fatalf("open wallet status=%d", status)
	}
	if wallet.Balance.Minor() != 10000 || wallet.Version != 1 {
		t.Fatalf("opened wallet=%+v", wallet)
	}

	externalID := uuid.NewString()
	operation := map[string]any{
		"providerId": "provider-a", "externalTransactionId": externalID,
		"playerId": playerID, "walletId": wallet.ID, "roundId": "round-1", "gameId": "game-1",
		"kind": "BET", "money": map[string]string{"amount": "25.00", "currency": "BRL"},
	}
	key := "provider-a:" + externalID
	headers := map[string]string{"Idempotency-Key": key}
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", otherToken, operation, nil, headers); status != http.StatusForbidden {
		t.Fatalf("cross-provider wager status=%d", status)
	}
	var first usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, operation, &first, headers); status != http.StatusOK {
		t.Fatalf("bet status=%d", status)
	}
	if first.Status != model.Processed || first.Balance.Minor() != 7500 {
		t.Fatalf("bet result=%+v", first)
	}
	var replay usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, operation, &replay, headers); status != http.StatusOK || !replay.IdempotentReplay || replay.Balance.Minor() != 7500 {
		t.Fatalf("replay status=%d result=%+v", status, replay)
	}
	if status := request(t, client, http.MethodGet, base+"/wagering/transactions/"+first.TransactionID.String(), otherToken, nil, nil); status != http.StatusNotFound {
		t.Fatalf("foreign transaction status=%d", status)
	}
	if status := request(t, client, http.MethodGet, base+"/providers/provider-a/wagering/transactions/"+externalID, otherToken, nil, nil); status != http.StatusForbidden {
		t.Fatalf("foreign provider route status=%d", status)
	}
	if status := request(t, client, http.MethodGet, base+"/wallets/"+wallet.ID.String(), providerToken, nil, nil); status != http.StatusForbidden {
		t.Fatalf("provider wallet read status=%d", status)
	}
	if status := request(t, client, http.MethodPost, base+"/wallets", walletToken, opening, nil); status != http.StatusConflict {
		t.Fatalf("duplicate wallet status=%d", status)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"), awsconfig.WithBaseEndpoint(localstack), awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	queues := sqs.NewFromConfig(awsCfg)
	inputURL := localstack + "/000000000000/wager-transactions.fifo"
	dlqURL := localstack + "/000000000000/wager-transactions-dlq.fifo"
	body, err := json.Marshal(map[string]any{"messageId": "e2e-" + uuid.NewString(), "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": map[string]any{
		"providerId": "provider-a", "externalTransactionId": externalID, "idempotencyKey": key,
		"playerId": playerID, "walletId": wallet.ID, "roundId": "round-1", "gameId": "game-1", "kind": "BET",
		"money": map[string]string{"amount": "25.00", "currency": "BRL"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	sendFIFO(t, ctx, queues, inputURL, wallet.ID.String(), string(body))
	waitFor(t, ctx, func() (bool, error) {
		var completed bool
		err := pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE message_id=$1`, extractMessageID(t, body)).Scan(&completed)
		return completed, err
	})
	duplicatesBefore := metricValue(t, client, base, walletToken, "apostas_wager_duplicates_total")
	sendFIFO(t, ctx, queues, inputURL, wallet.ID.String(), string(body))
	waitFor(t, ctx, func() (bool, error) {
		return metricValue(t, client, base, walletToken, "apostas_wager_duplicates_total") > duplicatesBefore, nil
	})
	var afterSQS walletResponse
	if status := request(t, client, http.MethodGet, base+"/wallets/"+wallet.ID.String(), walletToken, nil, &afterSQS); status != http.StatusOK || afterSQS.Balance.Minor() != 7500 {
		t.Fatalf("HTTP/SQS duplicate status=%d wallet=%+v", status, afterSQS)
	}

	concurrentID := uuid.NewString()
	concurrentKey := "provider-a:" + concurrentID
	concurrentOperation := map[string]any{
		"providerId": "provider-a", "externalTransactionId": concurrentID,
		"playerId": playerID, "walletId": wallet.ID, "roundId": "round-1", "gameId": "game-1",
		"kind": "BET", "money": map[string]string{"amount": "10.00", "currency": "BRL"},
	}
	concurrentMessageID := "e2e-" + uuid.NewString()
	concurrentData := map[string]any{}
	for key, value := range concurrentOperation {
		concurrentData[key] = value
	}
	concurrentData["idempotencyKey"] = concurrentKey
	concurrentBody, err := json.Marshal(map[string]any{"messageId": concurrentMessageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": concurrentData})
	if err != nil {
		t.Fatal(err)
	}
	sendFIFO(t, ctx, queues, inputURL, wallet.ID.String(), string(concurrentBody))
	var concurrentResult usecases.Result
	if status := request(t, client, http.MethodPost, base+"/wagering/transactions", providerToken, concurrentOperation, &concurrentResult, map[string]string{"Idempotency-Key": concurrentKey}); status != http.StatusOK || concurrentResult.Status != model.Processed {
		t.Fatalf("concurrent HTTP/SQS status=%d result=%+v", status, concurrentResult)
	}
	waitFor(t, ctx, func() (bool, error) {
		var completed bool
		err := pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE message_id=$1`, concurrentMessageID).Scan(&completed)
		return completed, err
	})
	var concurrentLedgerCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id=$1`, concurrentResult.TransactionID).Scan(&concurrentLedgerCount); err != nil || concurrentLedgerCount != 1 {
		t.Fatalf("concurrent ledger count=%d err=%v", concurrentLedgerCount, err)
	}
	if status := request(t, client, http.MethodGet, base+"/wallets/"+wallet.ID.String(), walletToken, nil, &afterSQS); status != http.StatusOK || afterSQS.Balance.Minor() != 6500 {
		t.Fatalf("concurrent HTTP/SQS balance status=%d wallet=%+v", status, afterSQS)
	}

	malformed := "invalid-e2e-" + uuid.NewString()
	sendFIFO(t, ctx, queues, inputURL, wallet.ID.String(), malformed)
	waitFor(t, ctx, func() (bool, error) {
		received, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(dlqURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false, err
		}
		found := false
		for _, message := range received.Messages {
			if aws.ToString(message.Body) == malformed {
				found = true
			}
			if message.ReceiptHandle != nil {
				_, _ = queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(dlqURL), ReceiptHandle: message.ReceiptHandle})
			}
		}
		return found, nil
	})
	var eventCount int
	waitFor(t, ctx, func() (bool, error) {
		err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'correlationId'=$1 AND published_at IS NOT NULL`, first.TransactionID.String()).Scan(&eventCount)
		return eventCount == 2, err
	})
	var reconciliation usecases.Reconciliation
	if status := request(t, client, http.MethodPost, base+"/wallets/"+wallet.ID.String()+"/reconciliation", walletToken, nil, &reconciliation); status != http.StatusOK || !reconciliation.Consistent || reconciliation.CheckedEntries != 3 {
		t.Fatalf("reconciliation status=%d result=%+v", status, reconciliation)
	}
}

func token(t *testing.T, client *http.Client, base, id, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}}
	response, err := client.PostForm(base+"/realms/apostas/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("token for %s: status=%d", id, response.StatusCode)
	}
	var value struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value.AccessToken
}

func expiredProviderToken(t *testing.T, client *http.Client, idp string) string {
	t.Helper()
	adminUser := os.Getenv("KEYCLOAK_ADMIN")
	if adminUser == "" {
		adminUser = "admin"
	}
	adminPassword := os.Getenv("KEYCLOAK_ADMIN_PASSWORD")
	if adminPassword == "" {
		adminPassword = "admin-local-only"
	}
	form := url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {adminUser}, "password": {adminPassword}}
	response, err := client.PostForm(idp+"/realms/master/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Keycloak admin token status=%d", response.StatusCode)
	}
	var admin struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&admin); err != nil {
		t.Fatal(err)
	}
	endpoint := idp + "/admin/realms/apostas"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+admin.AccessToken)
	response, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Keycloak realm read status=%d", response.StatusCode)
	}
	settings := map[string]any{}
	if err := json.NewDecoder(response.Body).Decode(&settings); err != nil {
		t.Fatal(err)
	}
	original := settings["accessTokenLifespan"]
	settings["accessTokenLifespan"] = 1
	updateRealm(t, client, endpoint, admin.AccessToken, settings)
	t.Cleanup(func() {
		settings["accessTokenLifespan"] = original
		updateRealm(t, client, endpoint, admin.AccessToken, settings)
	})
	value := token(t, client, idp, "provider-a", "provider-a-local-only")
	time.Sleep(2 * time.Second)
	return value
}

func updateRealm(t *testing.T, client *http.Client, endpoint, bearer string, settings map[string]any) {
	t.Helper()
	body, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("Keycloak realm update status=%d", response.StatusCode)
	}
}

func request(t *testing.T, client *http.Client, method, endpoint, bearer string, body any, output any, headers ...map[string]string) int {
	t.Helper()
	var data io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, endpoint, data)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for _, group := range headers {
		for key, value := range group {
			req.Header.Set(key, value)
		}
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			t.Fatal(err)
		}
	} else {
		_, _ = io.Copy(io.Discard, response.Body)
	}
	return response.StatusCode
}

func sendFIFO(t *testing.T, ctx context.Context, client *sqs.Client, queueURL, group, body string) {
	t.Helper()
	dedup := uuid.NewString()
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: &body, MessageGroupId: &group, MessageDeduplicationId: &dedup}); err != nil {
		t.Fatal(err)
	}
}

func extractMessageID(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.MessageID
}

func waitFor(t *testing.T, ctx context.Context, condition func() (bool, error)) {
	t.Helper()
	for {
		okay, err := condition()
		if err == nil && okay {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for external state: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func metricValue(t *testing.T, client *http.Client, base, bearer, name string) uint64 {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, name)), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("metric %s missing", name)
	return 0
}
