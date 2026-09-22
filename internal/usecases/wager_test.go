package usecases

import (
	"context"
	"testing"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

type captureStore struct{ hash string }

func (s *captureStore) Execute(_ context.Context, _ Operation, hash string, _ func(*model.Wallet, *Reference, time.Time) Decision) (Result, error) {
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
