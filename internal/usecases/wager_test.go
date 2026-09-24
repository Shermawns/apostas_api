package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

type captureStore struct {
	hash      string
	inbox     InboxMessage
	usedInbox bool
}

func (s *captureStore) Execute(_ context.Context, _ Operation, hash string, _ func(*model.Wallet, *Reference, time.Time) Decision) (Result, error) {
	s.hash = hash
	return Result{}, nil
}

func (s *captureStore) ExecuteInbox(_ context.Context, inbox InboxMessage, _ Operation, hash string, _ func(*model.Wallet, *Reference, time.Time) Decision) (Result, error) {
	s.usedInbox = true
	s.inbox = inbox
	s.hash = hash
	return Result{}, nil
}

func TestHashExcludesIdempotencyKey(t *testing.T) {
	money, _ := model.ParseMoney("1.00", "BRL")
	op := Operation{ProviderID: "provider-a", ExternalTransactionID: "ext-1", PlayerID: uuid.New(), WalletID: uuid.New(), RoundID: "round", GameID: "game", Kind: model.Bet, Money: money, IdempotencyKey: "first"}
	store := &captureStore{}
	u := NewWager(store)
	if _, err := u.Process(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	first := store.hash
	op.IdempotencyKey = "second"
	if _, err := u.Process(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if store.hash != first {
		t.Fatal("transport key affected payload hash")
	}
	op.RoundID = "different"
	if _, err := u.Process(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if store.hash == first {
		t.Fatal("business field did not affect payload hash")
	}
}

func TestCanonicalOperationUsesSortedBusinessKeys(t *testing.T) {
	money, _ := model.ParseMoney("1.5", "BRL")
	op := Operation{ProviderID: "provider", ExternalTransactionID: "external", PlayerID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), WalletID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), RoundID: "round", GameID: "game", Kind: model.Bet, Money: money}
	payload, err := canonicalOperation(op)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"externalTransactionId":"external","gameId":"game","kind":"BET","money":{"amount":"1.50","currency":"BRL"},"playerId":"00000000-0000-0000-0000-000000000001","providerId":"provider","referenceExternalTransactionId":"","roundId":"round","walletId":"00000000-0000-0000-0000-000000000002"}`
	if string(payload) != expected {
		t.Fatalf("canonical payload=%s", payload)
	}
}

func TestProcessInboxUsesAtomicStoreOperation(t *testing.T) {
	money, _ := model.ParseMoney("1.00", "BRL")
	op := Operation{ProviderID: "provider-a", ExternalTransactionID: "ext-1", IdempotencyKey: "key-1", PlayerID: uuid.New(), WalletID: uuid.New(), RoundID: "round", GameID: "game", Kind: model.Bet, Money: money}
	store := &captureStore{}
	inbox := InboxMessage{ConsumerName: "consumer", MessageID: "message", PayloadHash: "hash"}
	if _, err := NewWager(store).ProcessInbox(context.Background(), inbox, op); err != nil {
		t.Fatal(err)
	}
	if !store.usedInbox || store.inbox != inbox {
		t.Fatal("inbox delivery was not sent to the atomic store operation")
	}
}

func TestWinReferenceMustBeCompatibleBet(t *testing.T) {
	money, _ := model.ParseMoney("10.00", "BRL")
	walletID := uuid.New()
	playerID := uuid.New()
	wallet, err := model.NewWallet(walletID, playerID, money, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{ProviderID: "provider", PlayerID: playerID, WalletID: walletID, RoundID: "round", Kind: model.Win, Money: money, ReferenceExternalTransactionID: "bet-1"}
	if result := Decide(op, &wallet, nil, time.Now().UTC()); result.Status != model.PendingReference {
		t.Fatalf("missing reference status=%s", result.Status)
	}
	ref := &Reference{ID: uuid.New(), Kind: model.Bet, Status: model.Processed, WalletID: walletID, PlayerID: playerID, RoundID: "round", Money: money}
	if result := Decide(op, &wallet, ref, time.Now().UTC()); result.Status != model.Processed {
		t.Fatalf("compatible reference status=%s", result.Status)
	}
	ref.Kind = model.Win
	if result := Decide(op, &wallet, ref, time.Now().UTC()); result.Status != model.Rejected || result.FailureCode != "INVALID_REFERENCE" {
		t.Fatalf("incompatible reference result=%+v", result)
	}
}

func TestDecisionRulesForExternalKinds(t *testing.T) {
	initial, _ := model.ParseMoney("100.00", "BRL")
	stake, _ := model.ParseMoney("20.00", "BRL")
	zero, _ := model.Zero("BRL")
	walletID, playerID := uuid.New(), uuid.New()
	base := Operation{ProviderID: "provider", PlayerID: playerID, WalletID: walletID, RoundID: "round", Money: stake}
	betReference := &Reference{ID: uuid.New(), Kind: model.Bet, Status: model.Processed, WalletID: walletID, PlayerID: playerID, RoundID: "round", Money: stake}
	winReference := &Reference{ID: uuid.New(), Kind: model.Win, Status: model.Processed, WalletID: walletID, PlayerID: playerID, RoundID: "round", Money: stake}
	for _, test := range []struct {
		name      string
		kind      model.Kind
		money     model.Money
		ref       *Reference
		balance   int64
		status    model.Status
		failure   string
		direction string
	}{
		{"bet", model.Bet, stake, nil, 8000, model.Processed, "", "DEBIT"},
		{"win", model.Win, stake, nil, 12000, model.Processed, "", "CREDIT"},
		{"loss", model.Loss, zero, nil, 10000, model.Processed, "", ""},
		{"refund", model.Refund, stake, betReference, 12000, model.Processed, "", "CREDIT"},
		{"rollback bet", model.Rollback, stake, betReference, 12000, model.Processed, "", "CREDIT"},
		{"rollback win", model.Rollback, stake, winReference, 8000, model.Processed, "", "DEBIT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wallet, err := model.NewWallet(walletID, playerID, initial, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			op := base
			op.Kind, op.Money = test.kind, test.money
			decision := Decide(op, &wallet, test.ref, time.Now().UTC())
			if decision.Status != test.status || decision.FailureCode != test.failure || decision.Direction != test.direction || wallet.Balance().Minor() != test.balance {
				t.Fatalf("decision=%+v balance=%d", decision, wallet.Balance().Minor())
			}
			if test.kind == model.Loss && wallet.Version() != 1 {
				t.Fatalf("LOSS version=%d", wallet.Version())
			}
		})
	}
	low, _ := model.ParseMoney("5.00", "BRL")
	wallet, _ := model.NewWallet(walletID, playerID, low, time.Now().UTC())
	op := base
	op.Kind = model.Rollback
	decision := Decide(op, &wallet, winReference, time.Now().UTC())
	if decision.FailureCode != "REVERSAL_INSUFFICIENT_FUNDS" || wallet.Balance().Minor() != 500 {
		t.Fatalf("reversal result=%+v", decision)
	}
	betReference.AlreadyReversed = true
	op.Kind = model.Refund
	decision = Decide(op, &wallet, betReference, time.Now().UTC())
	if decision.FailureCode != "INVALID_REFERENCE" {
		t.Fatalf("duplicate reversal result=%+v", decision)
	}
}

func TestZeroPolicyRejectsExternalOpening(t *testing.T) {
	zero, _ := model.Zero("BRL")
	positive, _ := model.ParseMoney("1.00", "BRL")
	for _, kind := range []model.Kind{model.Bet, model.Win, model.Refund, model.Rollback} {
		if err := model.ValidateOperation(kind, zero, "reference"); !errors.Is(err, model.ErrInvalidTransaction) {
			t.Fatalf("%s zero err=%v", kind, err)
		}
	}
	if err := model.ValidateOperation(model.Kind("OPENING"), positive, ""); !errors.Is(err, model.ErrInvalidTransaction) {
		t.Fatalf("external opening err=%v", err)
	}
}
