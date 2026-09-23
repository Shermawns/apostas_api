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
