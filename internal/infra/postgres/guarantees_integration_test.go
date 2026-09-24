package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestDatabaseGuarantees(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema := "guarantees_" + uuid.NewString()[:8]
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	first := openProcessPool(t, ctx, databaseURL, schema)
	second := openProcessPool(t, ctx, databaseURL, schema)
	defer first.Close()
	defer second.Close()
	if err := createConcurrencySchema(ctx, first); err != nil {
		t.Fatal(err)
	}

	initial, _ := model.ParseMoney("100.00", "BRL")
	wallet, err := model.NewWallet(uuid.New(), uuid.New(), initial, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	storeA, storeB := postgres.NewStore(first), postgres.NewStore(second)
	if err := storeA.CreateWallet(ctx, wallet); err != nil {
		t.Fatal(err)
	}
	zeroBalance, _ := model.Zero("BRL")
	zeroWallet, err := model.NewWallet(uuid.New(), uuid.New(), zeroBalance, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := storeA.CreateWallet(ctx, zeroWallet); err != nil {
		t.Fatal(err)
	}
	var zeroFinancialRecords int
	if err := first.QueryRow(ctx, `SELECT (SELECT count(*) FROM wager_transactions WHERE wallet_id=$1)
		+ (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1)
		+ (SELECT count(*) FROM outbox_events WHERE aggregate_id=$1)`, zeroWallet.ID()).Scan(&zeroFinancialRecords); err != nil || zeroFinancialRecords != 0 {
		t.Fatalf("zero opening financial records=%d err=%v", zeroFinancialRecords, err)
	}

	if _, err := first.Exec(ctx, `UPDATE wallets SET balance_minor=11000,version=2 WHERE id=$1`, wallet.ID()); err == nil {
		t.Fatal("direct balance update committed without ledger")
	}
	if _, err := first.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor=1 WHERE wallet_id=$1`, wallet.ID()); err == nil {
		t.Fatal("ledger update committed")
	}
	if _, err := first.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id=$1`, wallet.ID()); err == nil {
		t.Fatal("ledger deletion committed")
	}
	if _, err := first.Exec(ctx, `UPDATE wager_transactions SET amount_minor=1 WHERE wallet_id=$1 AND kind='OPENING'`, wallet.ID()); err == nil {
		t.Fatal("opening transaction changed after processing")
	}

	claimedA, err := storeA.ClaimOutbox(ctx, 1)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("first claim=%d err=%v", len(claimedA), err)
	}
	claimedB, err := storeB.ClaimOutbox(ctx, 1)
	if err != nil || len(claimedB) != 1 || claimedA[0].ID == claimedB[0].ID {
		t.Fatalf("second claim=%d err=%v", len(claimedB), err)
	}
	if _, err := second.Exec(ctx, `UPDATE outbox_events SET claimed_until=now()-interval '1 second' WHERE id=$1`, claimedA[0].ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := storeB.ClaimOutbox(ctx, 1)
	if err != nil || len(recovered) != 1 || recovered[0].ID != claimedA[0].ID {
		t.Fatalf("recovery=%v err=%v", recovered, err)
	}
	if err := storeB.MarkOutboxPublished(ctx, recovered[0].ID); err != nil {
		t.Fatal(err)
	}

	stake, _ := model.ParseMoney("20.00", "BRL")
	op := operation(wallet, "inbox-bet", "inbox-key", stake)
	wager := usecases.NewWager(storeA)
	inbox := usecases.InboxMessage{ConsumerName: "test-consumer", MessageID: "message-1", PayloadHash: strings.Repeat("a", 64)}
	original, err := wager.ProcessInbox(ctx, inbox, op)
	if err != nil || original.Status != model.Processed {
		t.Fatalf("first inbox=%+v err=%v", original, err)
	}
	replay, err := usecases.NewWager(storeB).ProcessInbox(ctx, inbox, op)
	if err != nil || !replay.IdempotentReplay || replay.TransactionID != original.TransactionID {
		t.Fatalf("inbox replay=%+v err=%v", replay, err)
	}
	conflictingOperation := op
	conflictingOperation.RoundID = "different-round"
	if _, err := wager.Process(ctx, conflictingOperation); !errors.Is(err, usecases.ErrConflict) {
		t.Fatalf("idempotency key payload conflict=%v", err)
	}
	failedOperation := operation(wallet, "permanent-infrastructure-failure", "failure-key", stake)
	failed, err := wager.Fail(ctx, failedOperation, "PERMANENT_INFRASTRUCTURE_FAILURE")
	if err != nil || failed.Status != model.Failed || failed.FailureCode != "PERMANENT_INFRASTRUCTURE_FAILURE" {
		t.Fatalf("persist failed transaction=%+v err=%v", failed, err)
	}
	failedReplay, err := usecases.NewWager(storeB).Fail(ctx, failedOperation, "PERMANENT_INFRASTRUCTURE_FAILURE")
	if err != nil || !failedReplay.IdempotentReplay || failedReplay.TransactionID != failed.TransactionID || failedReplay.Status != model.Failed {
		t.Fatalf("failed replay=%+v err=%v", failedReplay, err)
	}
	var failedLedgerEntries int
	if err := first.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id=$1`, failed.TransactionID).Scan(&failedLedgerEntries); err != nil || failedLedgerEntries != 0 {
		t.Fatalf("failed transaction ledger entries=%d err=%v", failedLedgerEntries, err)
	}
	inbox.PayloadHash = strings.Repeat("b", 64)
	if _, err := wager.ProcessInbox(ctx, inbox, op); !errors.Is(err, usecases.ErrConflict) {
		t.Fatalf("hash conflict=%v", err)
	}
	var completed bool
	if err := first.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE consumer_name='test-consumer' AND message_id='message-1'`).Scan(&completed); err != nil || !completed {
		t.Fatalf("inbox completion=%v err=%v", completed, err)
	}
	stored, err := storeA.GetWallet(ctx, wallet.ID())
	if err != nil || stored.Balance().Minor() != 8000 {
		t.Fatalf("wallet=%+v err=%v", stored, err)
	}
	reconciliation, err := storeA.ReconcileWallet(ctx, wallet.ID())
	if err != nil || !reconciliation.Consistent || reconciliation.CheckedEntries != 2 {
		t.Fatalf("reconciliation=%+v err=%v", reconciliation, err)
	}

	refundAmount, _ := model.ParseMoney("10.00", "BRL")
	refund := operation(wallet, "refund-before-bet", "refund-key", refundAmount)
	refund.Kind = model.Refund
	refund.ReferenceExternalTransactionID = "later-bet"
	pending, err := wager.Process(ctx, refund)
	if err != nil || pending.Status != model.PendingReference {
		t.Fatalf("pending refund=%+v err=%v", pending, err)
	}
	laterBet := operation(wallet, "later-bet", "later-key", refundAmount)
	if result, err := wager.Process(ctx, laterBet); err != nil || result.Status != model.Processed {
		t.Fatalf("late bet=%+v err=%v", result, err)
	}
	if _, err := first.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1`, pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	processed, err := storeB.ResolvePendingReference(ctx)
	if err != nil || !processed {
		t.Fatalf("resolve=%t err=%v", processed, err)
	}
	resolved, err := storeA.GetTransaction(ctx, pending.TransactionID)
	if err != nil || resolved.Status != model.Processed || resolved.ReferenceTransactionID == nil {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}

	missing := operation(wallet, "missing-refund", "missing-key", refundAmount)
	missing.Kind = model.Refund
	missing.ReferenceExternalTransactionID = "never-arrives"
	expiring, err := wager.Process(ctx, missing)
	if err != nil || expiring.Status != model.PendingReference {
		t.Fatalf("expiring=%+v err=%v", expiring, err)
	}
	if _, err := first.Exec(ctx, `UPDATE wager_transactions SET attempts=9,next_attempt_at=now() WHERE id=$1`, expiring.TransactionID); err != nil {
		t.Fatal(err)
	}
	if handled, err := storeB.ResolvePendingReference(ctx); err != nil || !handled {
		t.Fatalf("expiry handled=%t err=%v", handled, err)
	}
	expired, err := storeA.GetTransaction(ctx, expiring.TransactionID)
	if err != nil || expired.Status != model.Rejected || expired.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Fatalf("expired=%+v err=%v", expired, err)
	}
	final, err := storeA.ReconcileWallet(ctx, wallet.ID())
	if err != nil || !final.Consistent {
		t.Fatalf("final reconciliation=%+v err=%v", final, err)
	}
}
