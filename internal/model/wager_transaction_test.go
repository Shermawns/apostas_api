package model

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWagerTransactionCreationRehydrationAndTransitions(t *testing.T) {
	now := time.Now().UTC()
	money, _ := ParseMoney("25.00", "BRL")
	result, _ := ParseMoney("75.00", "BRL")
	data := WagerTransactionData{
		ID: uuid.New(), ExternalTransactionID: "bet-1", ProviderID: "provider-a", IdempotencyKey: "key-1",
		PayloadHash: strings.Repeat("a", 64), WalletID: uuid.New(), PlayerID: uuid.New(),
		RoundID: "round-1", GameID: "game-1", Kind: Bet, Money: money, CreatedAt: now,
	}
	transaction, err := NewWagerTransaction(data)
	if err != nil || transaction.Status() != Pending {
		t.Fatalf("create=%+v err=%v", transaction, err)
	}
	if err := transaction.Transition(Processed, "", result, uuid.Nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Transition(Rejected, "INSUFFICIENT_FUNDS", result, uuid.Nil, now.Add(2*time.Second)); !errors.Is(err, ErrTerminalTransaction) {
		t.Fatalf("terminal transition=%v", err)
	}
	data.Status, data.ResultBalance, data.UpdatedAt = Processed, result, now.Add(time.Second)
	rehydrated, err := RehydrateWagerTransaction(data)
	if err != nil || rehydrated.Status() != Processed || rehydrated.ResultBalance() != result {
		t.Fatalf("rehydrate=%+v err=%v", rehydrated, err)
	}
	data.PayloadHash = "invalid"
	if _, err := RehydrateWagerTransaction(data); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("invalid rehydration=%v", err)
	}

	opening, err := NewOpeningTransaction(uuid.New(), uuid.New(), uuid.New(), money, now)
	if err != nil || opening.Kind() != Opening || opening.Status() != Processed {
		t.Fatalf("opening=%+v err=%v", opening, err)
	}
	if _, err := NewOpeningTransaction(uuid.New(), uuid.New(), uuid.New(), Money{}, now); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("invalid opening=%v", err)
	}
}
