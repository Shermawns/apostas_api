package model

import (
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
)

var ErrInsufficientFunds = errors.New("insufficient funds")
var ErrInvalidWallet = errors.New("invalid wallet")

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(id, playerID uuid.UUID, initial Money, now time.Time) (Wallet, error) {
	wallet := Wallet{id: id, playerID: playerID, balance: initial, version: 1, createdAt: now, updatedAt: now}
	if !wallet.Valid() {
		return Wallet{}, ErrInvalidWallet
	}
	return wallet, nil
}

func RehydrateWallet(id, playerID uuid.UUID, balance Money, version int64, created, updated time.Time) (Wallet, error) {
	wallet := Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: created, updatedAt: updated}
	if !wallet.Valid() {
		return Wallet{}, ErrInvalidWallet
	}
	return wallet, nil
}

func (w Wallet) ID() uuid.UUID        { return w.id }
func (w Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w Wallet) Balance() Money       { return w.balance }
func (w Wallet) Version() int64       { return w.version }
func (w Wallet) CreatedAt() time.Time { return w.createdAt }
func (w Wallet) UpdatedAt() time.Time { return w.updatedAt }
func (w Wallet) Valid() bool {
	return w.id != uuid.Nil && w.playerID != uuid.Nil && w.balance.Valid() && w.balance.Minor() >= 0 && w.version >= 1 && !w.createdAt.IsZero() && !w.updatedAt.IsZero() && !w.updatedAt.Before(w.createdAt)
}

func (w *Wallet) Credit(amount Money, now time.Time) (Money, error) {
	if !w.Valid() || now.Before(w.updatedAt) {
		return Money{}, ErrInvalidWallet
	}
	if !amount.IsPositive() || now.IsZero() {
		return Money{}, ErrInvalidMoney
	}
	if w.version == math.MaxInt64 {
		return Money{}, ErrOverflow
	}
	if w.balance.Currency() != amount.Currency() {
		return Money{}, ErrCurrencyMismatch
	}
	next, err := w.balance.Add(amount)
	if err != nil {
		return Money{}, err
	}
	before := w.balance
	w.balance, w.version, w.updatedAt = next, w.version+1, now
	return before, nil
}

func (w *Wallet) Debit(amount Money, now time.Time) (Money, error) {
	if !w.Valid() || now.Before(w.updatedAt) {
		return Money{}, ErrInvalidWallet
	}
	if !amount.IsPositive() || now.IsZero() {
		return Money{}, ErrInvalidMoney
	}
	if w.version == math.MaxInt64 {
		return Money{}, ErrOverflow
	}
	if w.balance.Currency() != amount.Currency() {
		return Money{}, ErrCurrencyMismatch
	}
	if w.balance.Minor() < amount.Minor() {
		return Money{}, ErrInsufficientFunds
	}
	next, err := w.balance.Sub(amount)
	if err != nil {
		return Money{}, err
	}
	before := w.balance
	w.balance, w.version, w.updatedAt = next, w.version+1, now
	return before, nil
}
