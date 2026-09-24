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
	if _, err := ParseMoney("1.00", "ZZZ"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("invalid currency err=%v", err)
	}
	zero, err := Zero("BRL")
	if err != nil || !zero.IsZero() || zero.Currency() != "BRL" {
		t.Fatalf("zero=%v err=%v", zero, err)
	}
	a, err := ParseMoney("25.5", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	amount, err := a.Amount()
	if err != nil || amount != "25.50" {
		t.Fatalf("amount=%s err=%v", amount, err)
	}
	b, err := ParseMoney("0.01", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	c, err := a.Sub(b)
	amount, amountErr := c.Amount()
	if err != nil || amountErr != nil || amount != "25.49" {
		t.Fatalf("subtract: %v %v", c, err)
	}
	encoded, err := json.Marshal(c)
	if err != nil || string(encoded) != `{"amount":"25.49","currency":"BRL"}` {
		t.Fatalf("json=%s err=%v", encoded, err)
	}
	usd, _ := ParseMoney("1.00", "USD")
	if comparison, err := a.Compare(a); err != nil || comparison != 0 {
		t.Fatalf("equal comparison=%d err=%v", comparison, err)
	}
	if comparison, err := a.Compare(b); err != nil || comparison <= 0 {
		t.Fatalf("greater comparison=%d err=%v", comparison, err)
	}
	if _, err := a.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("comparison currency err=%v", err)
	}
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
	if difference, err := min.Sub(min); err != nil || !difference.IsZero() {
		t.Fatalf("minimum minus itself=%v err=%v", difference, err)
	}
	if _, err := min.Sub(b); !errors.Is(err, ErrOverflow) {
		t.Fatalf("subtraction overflow err=%v", err)
	}
	if _, err := min.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("subtraction currency err=%v", err)
	}
	var strict Money
	if err := json.Unmarshal([]byte(`{"amount":"1.00","currency":"BRL","ignored":1}`), &strict); err == nil {
		t.Fatal("money accepted an unknown JSON field")
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
	amount, _ := w.Balance().Amount()
	if w.Version() != 1 || amount != "100.00" {
		t.Fatal("wallet changed after rejected debit")
	}
	small, _ := ParseMoney("80.00", "BRL")
	if _, err := w.Debit(small, time.Now()); err != nil {
		t.Fatal(err)
	}
	amount, _ = w.Balance().Amount()
	if w.Version() != 2 || amount != "20.00" {
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
	var uninitialized TransactionState
	if err := uninitialized.Transition(Processed); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("uninitialized transition err=%v", err)
	}
}

func TestDomainValuesRejectInvalidState(t *testing.T) {
	var money Money
	if _, err := money.Amount(); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("invalid money formatting err=%v", err)
	}
	balance, _ := ParseMoney("1.00", "BRL")
	created := time.Now().UTC()
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), balance, 1, created, created.Add(-time.Second)); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("invalid wallet chronology err=%v", err)
	}
	var wallet Wallet
	if _, err := wallet.Credit(balance, time.Now().UTC()); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("uninitialized wallet transition err=%v", err)
	}
}
