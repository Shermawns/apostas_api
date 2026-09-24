package usecases

import (
	"context"
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
