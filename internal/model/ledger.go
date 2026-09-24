package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidLedgerEntry = errors.New("invalid ledger entry")

type LedgerDirection string

const (
	Debit  LedgerDirection = "DEBIT"
	Credit LedgerDirection = "CREDIT"
)

type WalletLedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     LedgerDirection
	money         Money
	before        Money
	after         Money
	createdAt     time.Time
}

func NewWalletLedgerEntry(id, walletID, transactionID uuid.UUID, direction LedgerDirection, money, before, after Money, createdAt time.Time) (WalletLedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil || !money.IsPositive() || !before.Valid() || !after.Valid() || before.Minor() < 0 || after.Minor() < 0 || createdAt.IsZero() {
		return WalletLedgerEntry{}, ErrInvalidLedgerEntry
	}
	if before.Currency() != money.Currency() || after.Currency() != money.Currency() {
		return WalletLedgerEntry{}, ErrCurrencyMismatch
	}
	var expected Money
	var err error
	switch direction {
	case Credit:
		expected, err = before.Add(money)
	case Debit:
		expected, err = before.Sub(money)
	default:
		return WalletLedgerEntry{}, ErrInvalidLedgerEntry
	}
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if expected != after {
		return WalletLedgerEntry{}, ErrInvalidLedgerEntry
	}
	return WalletLedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: direction, money: money, before: before, after: after, createdAt: createdAt}, nil
}

func (e WalletLedgerEntry) ID() uuid.UUID              { return e.id }
func (e WalletLedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e WalletLedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e WalletLedgerEntry) Direction() LedgerDirection { return e.direction }
func (e WalletLedgerEntry) Money() Money               { return e.money }
func (e WalletLedgerEntry) BalanceBefore() Money       { return e.before }
func (e WalletLedgerEntry) BalanceAfter() Money        { return e.after }
func (e WalletLedgerEntry) CreatedAt() time.Time       { return e.createdAt }
