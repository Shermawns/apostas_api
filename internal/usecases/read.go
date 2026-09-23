package usecases

import (
	"context"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

type LedgerCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type LedgerEntry struct {
	ID            uuid.UUID   `json:"id"`
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         model.Money `json:"money"`
	BalanceBefore model.Money `json:"balanceBefore"`
	BalanceAfter  model.Money `json:"balanceAfter"`
	CreatedAt     time.Time   `json:"createdAt"`
}

type LedgerPage struct {
	Entries    []LedgerEntry
	NextCursor *LedgerCursor
}

type Transaction struct {
	ID                             uuid.UUID    `json:"transactionId"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	ProviderID                     string       `json:"providerId,omitempty"`
	WalletID                       uuid.UUID    `json:"walletId"`
	PlayerID                       uuid.UUID    `json:"playerId"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	Kind                           model.Kind   `json:"kind"`
	Money                          model.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID   `json:"referenceTransactionId,omitempty"`
	Status                         model.Status `json:"status"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	ResultBalance                  *model.Money `json:"resultBalance,omitempty"`
	CreatedAt                      time.Time    `json:"createdAt"`
	UpdatedAt                      time.Time    `json:"updatedAt"`
}

type Reconciliation struct {
	WalletID          uuid.UUID   `json:"walletId"`
	StoredBalance     model.Money `json:"storedBalance"`
	CalculatedBalance model.Money `json:"calculatedBalance"`
	Difference        model.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

type ReadStore interface {
	ListLedger(context.Context, uuid.UUID, *LedgerCursor, int) (LedgerPage, error)
	GetTransaction(context.Context, uuid.UUID) (Transaction, error)
	GetTransactionForProvider(context.Context, uuid.UUID, string) (Transaction, error)
	GetTransactionByExternal(context.Context, string, string) (Transaction, error)
	ReconcileWallet(context.Context, uuid.UUID) (Reconciliation, error)
}

type Reader struct{ store ReadStore }

func NewReader(store ReadStore) *Reader { return &Reader{store: store} }

func (u *Reader) Ledger(ctx context.Context, walletID uuid.UUID, cursor *LedgerCursor, limit int) (LedgerPage, error) {
	return u.store.ListLedger(ctx, walletID, cursor, limit)
}

func (u *Reader) Transaction(ctx context.Context, id uuid.UUID) (Transaction, error) {
	return u.store.GetTransaction(ctx, id)
}

func (u *Reader) TransactionForProvider(ctx context.Context, id uuid.UUID, providerID string) (Transaction, error) {
	return u.store.GetTransactionForProvider(ctx, id, providerID)
}

func (u *Reader) TransactionByExternal(ctx context.Context, providerID, externalID string) (Transaction, error) {
	return u.store.GetTransactionByExternal(ctx, providerID, externalID)
}

func (u *Reader) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	return u.store.ReconcileWallet(ctx, walletID)
}
