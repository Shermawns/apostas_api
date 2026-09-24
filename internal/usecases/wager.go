package usecases

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

var ErrInvalidInput = errors.New("invalid input")
var ErrConflict = errors.New("conflict")
var ErrNotFound = errors.New("not found")

type Operation struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"-"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           model.Kind  `json:"kind"`
	Money                          model.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

type Reference struct {
	ID              uuid.UUID
	Kind            model.Kind
	Status          model.Status
	WalletID        uuid.UUID
	PlayerID        uuid.UUID
	RoundID         string
	Money           model.Money
	AlreadyReversed bool
}

type Decision struct {
	Status      model.Status
	FailureCode string
	Direction   string
	Before      model.Money
	ReferenceID uuid.UUID
}

type Result struct {
	TransactionID    uuid.UUID    `json:"transactionId"`
	Status           model.Status `json:"status"`
	Balance          model.Money  `json:"balance"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

type WagerStore interface {
	Execute(context.Context, Operation, string, func(*model.Wallet, *Reference, time.Time) Decision) (Result, error)
	ExecuteInbox(context.Context, InboxMessage, Operation, string, func(*model.Wallet, *Reference, time.Time) Decision) (Result, error)
}

type InboxMessage struct {
	ConsumerName string
	MessageID    string
	PayloadHash  string
}

type Wager struct{ store WagerStore }

func NewWager(store WagerStore) *Wager { return &Wager{store: store} }

func (u *Wager) Process(ctx context.Context, op Operation) (Result, error) {
	return u.process(ctx, op, nil)
}

func (u *Wager) ProcessInbox(ctx context.Context, inbox InboxMessage, op Operation) (Result, error) {
	return u.process(ctx, op, &inbox)
}

func (u *Wager) Fail(ctx context.Context, op Operation, failureCode string) (Result, error) {
	if strings.TrimSpace(failureCode) == "" {
		return Result{}, ErrInvalidInput
	}
	if strings.TrimSpace(op.ProviderID) == "" || strings.TrimSpace(op.ExternalTransactionID) == "" || strings.TrimSpace(op.IdempotencyKey) == "" || op.PlayerID == uuid.Nil || op.WalletID == uuid.Nil {
		return Result{}, ErrInvalidInput
	}
	if err := model.ValidateOperation(op.Kind, op.Money, op.ReferenceExternalTransactionID); err != nil {
		return Result{}, ErrInvalidInput
	}
	canonical, err := canonicalOperation(op)
	if err != nil {
		return Result{}, err
	}
	sum := sha256.Sum256(canonical)
	return u.store.Execute(ctx, op, hex.EncodeToString(sum[:]), func(wallet *model.Wallet, _ *Reference, _ time.Time) Decision {
		return Decision{Status: model.Failed, FailureCode: failureCode}
	})
}

func (u *Wager) process(ctx context.Context, op Operation, inbox *InboxMessage) (Result, error) {
	if strings.TrimSpace(op.ProviderID) == "" || strings.TrimSpace(op.ExternalTransactionID) == "" || strings.TrimSpace(op.IdempotencyKey) == "" || strings.TrimSpace(op.RoundID) == "" || strings.TrimSpace(op.GameID) == "" || op.PlayerID == uuid.Nil || op.WalletID == uuid.Nil {
		return Result{}, ErrInvalidInput
	}
	if err := model.ValidateOperation(op.Kind, op.Money, op.ReferenceExternalTransactionID); err != nil {
		return Result{}, ErrInvalidInput
	}
	canonical, err := canonicalOperation(op)
	if err != nil {
		return Result{}, err
	}
	sum := sha256.Sum256(canonical)
	decision := func(wallet *model.Wallet, ref *Reference, now time.Time) Decision {
		return Decide(op, wallet, ref, now)
	}
	if inbox != nil {
		return u.store.ExecuteInbox(ctx, *inbox, op, hex.EncodeToString(sum[:]), decision)
	}
	return u.store.Execute(ctx, op, hex.EncodeToString(sum[:]), decision)
}

func canonicalOperation(op Operation) ([]byte, error) {
	return json.Marshal(map[string]any{
		"externalTransactionId":          op.ExternalTransactionID,
		"gameId":                         op.GameID,
		"kind":                           op.Kind,
		"money":                          op.Money,
		"playerId":                       op.PlayerID,
		"providerId":                     op.ProviderID,
		"referenceExternalTransactionId": op.ReferenceExternalTransactionID,
		"roundId":                        op.RoundID,
		"walletId":                       op.WalletID,
	})
}

func Decide(op Operation, wallet *model.Wallet, ref *Reference, now time.Time) Decision {
	if wallet.PlayerID() != op.PlayerID || wallet.Balance().Currency() != op.Money.Currency() {
		return Decision{Status: model.Rejected, FailureCode: "WALLET_MISMATCH"}
	}
	if op.Kind == model.Loss {
		return Decision{Status: model.Processed}
	}
	if op.Kind == model.Win && op.ReferenceExternalTransactionID != "" {
		if ref == nil || ref.Status == model.Pending || ref.Status == model.PendingReference {
			return Decision{Status: model.PendingReference}
		}
		if ref.Status != model.Processed || ref.Kind != model.Bet || ref.PlayerID != op.PlayerID || ref.WalletID != op.WalletID || ref.RoundID != op.RoundID || ref.Money.Currency() != op.Money.Currency() {
			return Decision{Status: model.Rejected, FailureCode: "INVALID_REFERENCE"}
		}
	}
	if op.Kind == model.Refund || op.Kind == model.Rollback {
		if ref == nil || ref.Status == model.Pending || ref.Status == model.PendingReference {
			return Decision{Status: model.PendingReference}
		}
		if ref.Status != model.Processed || ref.PlayerID != op.PlayerID || ref.WalletID != op.WalletID || ref.RoundID != op.RoundID || ref.Money != op.Money || ref.AlreadyReversed {
			return Decision{Status: model.Rejected, FailureCode: "INVALID_REFERENCE"}
		}
		if op.Kind == model.Refund && ref.Kind != model.Bet {
			return Decision{Status: model.Rejected, FailureCode: "INVALID_REFERENCE"}
		}
		if op.Kind == model.Rollback && ref.Kind != model.Bet && ref.Kind != model.Win && ref.Kind != model.Refund {
			return Decision{Status: model.Rejected, FailureCode: "INVALID_REFERENCE"}
		}
	}
	debit := op.Kind == model.Bet || (op.Kind == model.Rollback && ref != nil && (ref.Kind == model.Win || ref.Kind == model.Refund))
	var before model.Money
	var err error
	if debit {
		before, err = wallet.Debit(op.Money, now)
	} else {
		before, err = wallet.Credit(op.Money, now)
	}
	if errors.Is(err, model.ErrInsufficientFunds) {
		code := "INSUFFICIENT_FUNDS"
		if op.Kind == model.Rollback {
			code = "REVERSAL_INSUFFICIENT_FUNDS"
		}
		return Decision{Status: model.Rejected, FailureCode: code}
	}
	if err != nil {
		return Decision{Status: model.Rejected, FailureCode: "AMOUNT_OVERFLOW"}
	}
	d := Decision{Status: model.Processed, Direction: "CREDIT", Before: before}
	if debit {
		d.Direction = "DEBIT"
	}
	if ref != nil {
		d.ReferenceID = ref.ID
	}
	return d
}
