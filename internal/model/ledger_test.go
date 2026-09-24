package model

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWalletLedgerEntryInvariants(t *testing.T) {
	before, _ := ParseMoney("10.00", "BRL")
	money, _ := ParseMoney("2.00", "BRL")
	after, _ := ParseMoney("12.00", "BRL")
	if _, err := NewWalletLedgerEntry(uuid.New(), uuid.New(), uuid.New(), Credit, money, before, after, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWalletLedgerEntry(uuid.New(), uuid.New(), uuid.New(), Debit, money, before, after, time.Now().UTC()); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Fatalf("invalid debit err=%v", err)
	}
}
