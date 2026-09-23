package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func (s *Store) CreateWallet(ctx context.Context, wallet model.Wallet) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version,created_at,updated_at) VALUES($1,$2,$3,$4,1,$5,$5)`, wallet.ID(), wallet.PlayerID(), wallet.Balance().Currency(), wallet.Balance().Minor(), wallet.CreatedAt())
	if err != nil {
		return classify(err)
	}
	if wallet.Balance().IsPositive() {
		transactionID := uuid.New()
		_, err = tx.Exec(ctx, `INSERT INTO wager_transactions(id,wallet_id,player_id,kind,amount_minor,currency,status,result_balance_minor) VALUES($1,$2,$3,'OPENING',$4,$5,'PROCESSED',$4)`, transactionID, wallet.ID(), wallet.PlayerID(), wallet.Balance().Minor(), wallet.Balance().Currency())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount_minor,balance_before_minor,balance_after_minor) VALUES($1,$2,$3,'CREDIT',$4,0,$4)`, uuid.New(), wallet.ID(), transactionID, wallet.Balance().Minor())
		if err != nil {
			return err
		}
		if err := addEvent(ctx, tx, wallet.ID(), "WagerTransactionProcessed", map[string]any{"transactionId": transactionID, "kind": "OPENING"}); err != nil {
			return err
		}
		if err := addEvent(ctx, tx, wallet.ID(), "WalletBalanceChanged", map[string]any{"walletId": wallet.ID(), "transactionId": transactionID, "direction": "CREDIT", "money": wallet.Balance(), "balanceBefore": zero(wallet.Balance().Currency()), "balanceAfter": wallet.Balance(), "walletVersion": 1}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) GetWallet(ctx context.Context, id uuid.UUID) (model.Wallet, error) {
	var player uuid.UUID
	var currency string
	var minor, version int64
	var created, updated time.Time
	err := s.Pool.QueryRow(ctx, `SELECT player_id,currency,balance_minor,version,created_at,updated_at FROM wallets WHERE id=$1`, id).Scan(&player, &currency, &minor, &version, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Wallet{}, usecases.ErrNotFound
	}
	if err != nil {
		return model.Wallet{}, err
	}
	money, err := model.MoneyFromMinor(minor, currency)
	if err != nil {
		return model.Wallet{}, err
	}
	return model.RehydrateWallet(id, player, money, version, created, updated)
}

func (s *Store) ListLedger(ctx context.Context, walletID uuid.UUID, cursor *usecases.LedgerCursor, limit int) (usecases.LedgerPage, error) {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wallets WHERE id=$1)`, walletID).Scan(&exists); err != nil {
		return usecases.LedgerPage{}, err
	}
	if !exists {
		return usecases.LedgerPage{}, usecases.ErrNotFound
	}

	query := `SELECT l.id,l.wallet_id,l.transaction_id,l.direction,l.amount_minor,l.balance_before_minor,l.balance_after_minor,l.created_at,w.currency
		FROM wallet_ledger_entries l JOIN wallets w ON w.id=l.wallet_id WHERE l.wallet_id=$1 ORDER BY l.created_at,l.id LIMIT $2`
	args := []any{walletID, limit + 1}
	if cursor != nil {
		query = `SELECT l.id,l.wallet_id,l.transaction_id,l.direction,l.amount_minor,l.balance_before_minor,l.balance_after_minor,l.created_at,w.currency
			FROM wallet_ledger_entries l JOIN wallets w ON w.id=l.wallet_id WHERE l.wallet_id=$1 AND (l.created_at,l.id) > ($2,$3) ORDER BY l.created_at,l.id LIMIT $4`
		args = []any{walletID, cursor.CreatedAt, cursor.ID, limit + 1}
	}
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return usecases.LedgerPage{}, err
	}
	defer rows.Close()

	entries := make([]usecases.LedgerEntry, 0, limit)
	for rows.Next() {
		var entry usecases.LedgerEntry
		var amount, before, after int64
		var currency string
		if err := rows.Scan(&entry.ID, &entry.WalletID, &entry.TransactionID, &entry.Direction, &amount, &before, &after, &entry.CreatedAt, &currency); err != nil {
			return usecases.LedgerPage{}, err
		}
		var moneyErr error
		entry.Money, moneyErr = model.MoneyFromMinor(amount, currency)
		if moneyErr == nil {
			entry.BalanceBefore, moneyErr = model.MoneyFromMinor(before, currency)
		}
		if moneyErr == nil {
			entry.BalanceAfter, moneyErr = model.MoneyFromMinor(after, currency)
		}
		if moneyErr != nil {
			return usecases.LedgerPage{}, moneyErr
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return usecases.LedgerPage{}, err
	}
	page := usecases.LedgerPage{Entries: entries}
	if len(entries) > limit {
		page.Entries = entries[:limit]
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &usecases.LedgerCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

func (s *Store) GetTransaction(ctx context.Context, id uuid.UUID) (usecases.Transaction, error) {
	row := s.Pool.QueryRow(ctx, transactionQuery+` WHERE id=$1`, id)
	return scanTransaction(row)
}

func (s *Store) GetTransactionForProvider(ctx context.Context, id uuid.UUID, providerID string) (usecases.Transaction, error) {
	row := s.Pool.QueryRow(ctx, transactionQuery+` WHERE id=$1 AND provider_id=$2`, id, providerID)
	return scanTransaction(row)
}

func (s *Store) GetTransactionByExternal(ctx context.Context, providerID, externalID string) (usecases.Transaction, error) {
	row := s.Pool.QueryRow(ctx, transactionQuery+` WHERE provider_id=$1 AND external_transaction_id=$2`, providerID, externalID)
	return scanTransaction(row)
}

func (s *Store) ReconcileWallet(ctx context.Context, walletID uuid.UUID) (usecases.Reconciliation, error) {
	var currency string
	var stored, calculated int64
	var entries int64
	err := s.Pool.QueryRow(ctx, `SELECT w.currency,w.balance_minor,
		COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount_minor ELSE -l.amount_minor END),0)::bigint,
		COUNT(l.id)
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id
		WHERE w.id=$1 GROUP BY w.id,w.currency,w.balance_minor`, walletID).Scan(&currency, &stored, &calculated, &entries)
	if errors.Is(err, pgx.ErrNoRows) {
		return usecases.Reconciliation{}, usecases.ErrNotFound
	}
	if err != nil {
		return usecases.Reconciliation{}, err
	}
	storedMoney, err := model.MoneyFromMinor(stored, currency)
	if err != nil {
		return usecases.Reconciliation{}, err
	}
	calculatedMoney, err := model.MoneyFromMinor(calculated, currency)
	if err != nil {
		return usecases.Reconciliation{}, err
	}
	difference, err := storedMoney.Sub(calculatedMoney)
	if err != nil {
		return usecases.Reconciliation{}, err
	}
	return usecases.Reconciliation{WalletID: walletID, StoredBalance: storedMoney, CalculatedBalance: calculatedMoney, Difference: difference, Consistent: difference.IsZero(), CheckedEntries: entries}, nil
}

const transactionQuery = `SELECT id,external_transaction_id,provider_id,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,
	reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance_minor,created_at,updated_at FROM wager_transactions`

func scanTransaction(row pgx.Row) (usecases.Transaction, error) {
	var transaction usecases.Transaction
	var externalID, providerID, roundID, gameID, referenceExternalID, failureCode *string
	var referenceID *uuid.UUID
	var amount int64
	var currency string
	var resultMinor *int64
	err := row.Scan(&transaction.ID, &externalID, &providerID, &transaction.WalletID, &transaction.PlayerID, &roundID, &gameID, &transaction.Kind, &amount, &currency, &referenceExternalID, &referenceID, &transaction.Status, &failureCode, &resultMinor, &transaction.CreatedAt, &transaction.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return usecases.Transaction{}, usecases.ErrNotFound
	}
	if err != nil {
		return usecases.Transaction{}, err
	}
	var moneyErr error
	transaction.Money, moneyErr = model.MoneyFromMinor(amount, currency)
	if moneyErr != nil {
		return usecases.Transaction{}, moneyErr
	}
	if resultMinor != nil {
		resultBalance, err := model.MoneyFromMinor(*resultMinor, currency)
		if err != nil {
			return usecases.Transaction{}, err
		}
		transaction.ResultBalance = &resultBalance
	}
	transaction.ExternalTransactionID = stringValue(externalID)
	transaction.ProviderID = stringValue(providerID)
	transaction.RoundID = stringValue(roundID)
	transaction.GameID = stringValue(gameID)
	transaction.ReferenceExternalTransactionID = stringValue(referenceExternalID)
	transaction.ReferenceTransactionID = referenceID
	transaction.FailureCode = stringValue(failureCode)
	return transaction, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Store) Execute(ctx context.Context, op usecases.Operation, hash string, decide func(*model.Wallet, *usecases.Reference, time.Time) usecases.Decision) (usecases.Result, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return usecases.Result{}, err
	}
	defer tx.Rollback(ctx)
	// The wallet row serializes writers across all API and worker processes.
	var player uuid.UUID
	var currency string
	var minor, version int64
	var created, updated time.Time
	err = tx.QueryRow(ctx, `SELECT player_id,currency,balance_minor,version,created_at,updated_at FROM wallets WHERE id=$1 FOR UPDATE`, op.WalletID).Scan(&player, &currency, &minor, &version, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return usecases.Result{}, usecases.ErrNotFound
	}
	if err != nil {
		return usecases.Result{}, err
	}
	walletMoney, err := model.MoneyFromMinor(minor, currency)
	if err != nil {
		return usecases.Result{}, err
	}
	wallet, err := model.RehydrateWallet(op.WalletID, player, walletMoney, version, created, updated)
	if err != nil {
		return usecases.Result{}, err
	}
	var existingID uuid.UUID
	var existingHash, existingKey string
	var existingStatus model.Status
	var existingBalance int64
	var existingFailure *string
	err = tx.QueryRow(ctx, `SELECT id,payload_hash,idempotency_key,status,result_balance_minor,failure_code FROM wager_transactions WHERE provider_id=$1 AND (idempotency_key=$2 OR external_transaction_id=$3) LIMIT 1`, op.ProviderID, op.IdempotencyKey, op.ExternalTransactionID).Scan(&existingID, &existingHash, &existingKey, &existingStatus, &existingBalance, &existingFailure)
	if err == nil {
		if existingHash != hash || existingKey != op.IdempotencyKey {
			return usecases.Result{}, usecases.ErrConflict
		}
		balance, _ := model.MoneyFromMinor(existingBalance, currency)
		result := usecases.Result{TransactionID: existingID, Status: existingStatus, Balance: balance, IdempotentReplay: true}
		if existingFailure != nil {
			result.FailureCode = *existingFailure
		}
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return usecases.Result{}, err
	}
	var ref *usecases.Reference
	if op.ReferenceExternalTransactionID != "" {
		var r usecases.Reference
		var amount int64
		var refCurrency string
		err = tx.QueryRow(ctx, `SELECT id,kind,status,wallet_id,player_id,round_id,amount_minor,currency FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, op.ProviderID, op.ReferenceExternalTransactionID).Scan(&r.ID, &r.Kind, &r.Status, &r.WalletID, &r.PlayerID, &r.RoundID, &amount, &refCurrency)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return usecases.Result{}, err
		}
		if err == nil {
			r.Money, err = model.MoneyFromMinor(amount, refCurrency)
			if err != nil {
				return usecases.Result{}, err
			}
			err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wager_transactions WHERE reference_transaction_id=$1 AND status='PROCESSED' AND kind IN ('REFUND','ROLLBACK'))`, r.ID).Scan(&r.AlreadyReversed)
			if err != nil {
				return usecases.Result{}, err
			}
			ref = &r
		}
	}
	now := time.Now().UTC()
	d := decide(&wallet, ref, now)
	transactionID := uuid.New()
	var referenceID *uuid.UUID
	if d.ReferenceID != uuid.Nil {
		referenceID = &d.ReferenceID
	}
	var failure *string
	if d.FailureCode != "" {
		failure = &d.FailureCode
	}
	_, err = tx.Exec(ctx, `INSERT INTO wager_transactions(id,external_transaction_id,provider_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance_minor,next_attempt_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, transactionID, op.ExternalTransactionID, op.ProviderID, op.IdempotencyKey, hash, op.WalletID, op.PlayerID, op.RoundID, op.GameID, op.Kind, op.Money.Minor(), op.Money.Currency(), nullable(op.ReferenceExternalTransactionID), referenceID, d.Status, failure, wallet.Balance().Minor(), nextAttempt(d.Status, now))
	if err != nil {
		return usecases.Result{}, classify(err)
	}
	if d.Direction != "" {
		_, err = tx.Exec(ctx, `UPDATE wallets SET balance_minor=$2,version=$3,updated_at=$4 WHERE id=$1`, op.WalletID, wallet.Balance().Minor(), wallet.Version(), now)
		if err != nil {
			return usecases.Result{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount_minor,balance_before_minor,balance_after_minor) VALUES($1,$2,$3,$4,$5,$6,$7)`, uuid.New(), op.WalletID, transactionID, d.Direction, op.Money.Minor(), d.Before.Minor(), wallet.Balance().Minor())
		if err != nil {
			return usecases.Result{}, err
		}
		if err := addEvent(ctx, tx, op.WalletID, "WalletBalanceChanged", map[string]any{"walletId": op.WalletID, "transactionId": transactionID, "direction": d.Direction, "money": op.Money, "balanceBefore": d.Before, "balanceAfter": wallet.Balance(), "walletVersion": wallet.Version()}); err != nil {
			return usecases.Result{}, err
		}
	}
	event := "WagerTransactionProcessed"
	if d.Status == model.Rejected {
		event = "WagerTransactionRejected"
	}
	if d.Status == model.PendingReference {
		event = "WagerTransactionPendingReference"
	}
	if err := addEvent(ctx, tx, op.WalletID, event, map[string]any{"transactionId": transactionID, "providerId": op.ProviderID, "kind": op.Kind, "status": d.Status, "failureCode": d.FailureCode}); err != nil {
		return usecases.Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return usecases.Result{}, err
	}
	return usecases.Result{TransactionID: transactionID, Status: d.Status, Balance: wallet.Balance(), FailureCode: d.FailureCode}, nil
}

func addEvent(ctx context.Context, tx pgx.Tx, aggregate uuid.UUID, eventType string, data any) error {
	id := uuid.New()
	payload, err := json.Marshal(map[string]any{"eventId": id, "eventType": eventType, "aggregateId": aggregate, "occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "version": 1, "data": data})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,aggregate_id,event_type,payload) VALUES($1,$2,$3,$4)`, id, aggregate, eventType, payload)
	return err
}

func zero(currency string) model.Money { m, _ := model.MoneyFromMinor(0, currency); return m }
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nextAttempt(status model.Status, now time.Time) any {
	if status == model.PendingReference {
		return now.Add(time.Second)
	}
	return nil
}
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return usecases.ErrConflict
	}
	return fmt.Errorf("postgres: %w", err)
}
