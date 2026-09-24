package model

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

type WagerTransactionData struct {
	ID                             uuid.UUID
	ExternalTransactionID          string
	ProviderID                     string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	Status                         Status
	FailureCode                    string
	ResultBalance                  Money
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

type WagerTransaction struct {
	data WagerTransactionData
}

func NewWagerTransaction(data WagerTransactionData) (WagerTransaction, error) {
	data.Status = Pending
	data.FailureCode = ""
	data.ReferenceTransactionID = uuid.Nil
	data.ResultBalance = Money{}
	data.UpdatedAt = data.CreatedAt
	if err := validateWagerTransaction(data); err != nil {
		return WagerTransaction{}, err
	}
	return WagerTransaction{data: data}, nil
}

func NewOpeningTransaction(id, walletID, playerID uuid.UUID, money Money, now time.Time) (WagerTransaction, error) {
	data := WagerTransactionData{ID: id, WalletID: walletID, PlayerID: playerID, Kind: Opening, Money: money, Status: Processed, ResultBalance: money, CreatedAt: now, UpdatedAt: now}
	if err := validateWagerTransaction(data); err != nil {
		return WagerTransaction{}, err
	}
	return WagerTransaction{data: data}, nil
}

func RehydrateWagerTransaction(data WagerTransactionData) (WagerTransaction, error) {
	if err := validateWagerTransaction(data); err != nil {
		return WagerTransaction{}, err
	}
	return WagerTransaction{data: data}, nil
}

func (t WagerTransaction) ID() uuid.UUID                     { return t.data.ID }
func (t WagerTransaction) Status() Status                    { return t.data.Status }
func (t WagerTransaction) Kind() Kind                        { return t.data.Kind }
func (t WagerTransaction) Money() Money                      { return t.data.Money }
func (t WagerTransaction) FailureCode() string               { return t.data.FailureCode }
func (t WagerTransaction) ResultBalance() Money              { return t.data.ResultBalance }
func (t WagerTransaction) ReferenceTransactionID() uuid.UUID { return t.data.ReferenceTransactionID }
func (t WagerTransaction) CreatedAt() time.Time              { return t.data.CreatedAt }
func (t WagerTransaction) UpdatedAt() time.Time              { return t.data.UpdatedAt }

func (t *WagerTransaction) Transition(status Status, failureCode string, resultBalance Money, referenceID uuid.UUID, now time.Time) error {
	if t == nil || validateWagerTransaction(t.data) != nil {
		return ErrInvalidTransaction
	}
	state, err := RehydrateTransactionState(t.data.Status)
	if err != nil {
		return err
	}
	if err := state.Transition(status); err != nil {
		return err
	}
	if now.IsZero() || now.Before(t.data.UpdatedAt) || !resultBalance.Valid() || resultBalance.Minor() < 0 || resultBalance.Currency() != t.data.Money.Currency() {
		return ErrInvalidTransaction
	}
	if (status == Rejected || status == Failed) != (failureCode != "") || (status == Processed || status == PendingReference) && failureCode != "" {
		return ErrInvalidTransaction
	}
	if referenceID != uuid.Nil && status != Processed {
		return ErrInvalidTransaction
	}
	next := t.data
	next.Status = status
	next.FailureCode = failureCode
	next.ResultBalance = resultBalance
	next.ReferenceTransactionID = referenceID
	next.UpdatedAt = now
	if err := validateWagerTransaction(next); err != nil {
		return err
	}
	t.data = next
	return nil
}

func validateWagerTransaction(data WagerTransactionData) error {
	if data.ID == uuid.Nil || data.WalletID == uuid.Nil || data.PlayerID == uuid.Nil || !data.Money.Valid() || data.Money.Minor() < 0 || data.CreatedAt.IsZero() || data.UpdatedAt.IsZero() || data.UpdatedAt.Before(data.CreatedAt) {
		return ErrInvalidTransaction
	}
	if _, err := RehydrateTransactionState(data.Status); err != nil {
		return err
	}
	if data.ResultBalance.Valid() && (data.ResultBalance.Currency() != data.Money.Currency() || data.ResultBalance.Minor() < 0) {
		return ErrInvalidTransaction
	}
	if data.Status != Pending && !data.ResultBalance.Valid() {
		return ErrInvalidTransaction
	}
	if data.Status == Rejected || data.Status == Failed {
		if data.FailureCode == "" {
			return ErrInvalidTransaction
		}
	} else if data.FailureCode != "" {
		return ErrInvalidTransaction
	}
	if data.Kind == Opening {
		if !data.Money.IsPositive() || data.Status != Processed || data.ResultBalance != data.Money || data.ProviderID != "" || data.ExternalTransactionID != "" || data.IdempotencyKey != "" || data.PayloadHash != "" || data.RoundID != "" || data.GameID != "" || data.ReferenceExternalTransactionID != "" || data.ReferenceTransactionID != uuid.Nil {
			return ErrInvalidTransaction
		}
		return nil
	}
	if strings.TrimSpace(data.ProviderID) == "" || strings.TrimSpace(data.ExternalTransactionID) == "" || strings.TrimSpace(data.IdempotencyKey) == "" || len(data.PayloadHash) != 64 || strings.TrimSpace(data.RoundID) == "" || strings.TrimSpace(data.GameID) == "" {
		return ErrInvalidTransaction
	}
	return ValidateOperation(data.Kind, data.Money, data.ReferenceExternalTransactionID)
}
