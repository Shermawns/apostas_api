package model

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMoney(t *testing.T) {
	for _, amount := range []string{"", "NaN", "Infinity", "1e2", "-1.00", "1.234", "01.00", "92233720368547758.08"} {
		if _, err := ParseMoney(amount, "BRL"); err == nil {
			t.Errorf("accepted %q", amount)
		}
	}
	a, err := ParseMoney("25.5", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if a.Amount() != "25.50" {
		t.Fatalf("amount=%s", a.Amount())
	}
	b, err := ParseMoney("0.01", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	c, err := a.Sub(b)
	if err != nil || c.Amount() != "25.49" {
		t.Fatalf("subtract: %v %v", c, err)
	}
	encoded, err := json.Marshal(c)
	if err != nil || string(encoded) != `{"amount":"25.49","currency":"BRL"}` {
		t.Fatalf("json=%s err=%v", encoded, err)
	}
	usd, _ := ParseMoney("1.00", "USD")
	if _, err := a.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency err=%v", err)
	}
	max, _ := MoneyFromMinor(math.MaxInt64, "BRL")
	if _, err := max.Add(b); !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow err=%v", err)
	}
	min, _ := MoneyFromMinor(math.MinInt64, "BRL")
	if _, err := min.Negate(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("negate err=%v", err)
	}
}

func TestWalletInvariants(t *testing.T) {
	initial, _ := ParseMoney("100.00", "BRL")
	w, err := NewWallet(uuid.New(), uuid.New(), initial, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	large, _ := ParseMoney("100.01", "BRL")
	if _, err := w.Debit(large, time.Now()); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("debit err=%v", err)
	}
	if w.Version() != 1 || w.Balance().Amount() != "100.00" {
		t.Fatal("wallet changed after rejected debit")
	}
	small, _ := ParseMoney("80.00", "BRL")
	if _, err := w.Debit(small, time.Now()); err != nil {
		t.Fatal(err)
	}
	if w.Version() != 2 || w.Balance().Amount() != "20.00" {
		t.Fatal("wrong wallet state")
	}
}

func TestTransactionStateAndKinds(t *testing.T) {
	zero, _ := ParseMoney("0.00", "BRL")
	positive, _ := ParseMoney("1.00", "BRL")
	if err := ValidateOperation(Loss, positive, ""); err == nil {
		t.Fatal("accepted positive loss")
	}
	if err := ValidateOperation(Loss, zero, ""); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOperation(Refund, positive, ""); err == nil {
		t.Fatal("accepted refund without reference")
	}
	s := NewTransactionState()
	if err := s.Transition(PendingReference); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(Processed); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(Rejected); !errors.Is(err, ErrTerminalTransaction) {
		t.Fatalf("terminal err=%v", err)
	}
}
