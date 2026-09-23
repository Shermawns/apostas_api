package controller

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"apostas_api/internal/infra/oidc"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
)

type Handler struct {
	wallets *usecases.Wallets
	wager   *usecases.Wager
	reader  *usecases.Reader
	pool    *pgxpool.Pool
}

func NewHandler(wallets *usecases.Wallets, wager *usecases.Wager, reader *usecases.Reader, pool *pgxpool.Pool) *Handler {
	return &Handler{wallets: wallets, wager: wager, reader: reader, pool: pool}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "live"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := h.pool.Ping(r.Context()); err != nil {
			writeError(w, 503, "DATABASE_UNAVAILABLE")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /wallets", h.openWallet)
	mux.HandleFunc("GET /wallets/{walletId}", h.getWallet)
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.getLedger)
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.reconcileWallet)
	mux.HandleFunc("POST /wagering/transactions", h.processWager)
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.getTransaction)
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.getProviderTransaction)
	return mux
}

func (h *Handler) openWallet(w http.ResponseWriter, r *http.Request) {
	if !internal(r) {
		writeError(w, 403, "FORBIDDEN")
		return
	}
	var body struct {
		PlayerID       uuid.UUID   `json:"playerId"`
		InitialBalance model.Money `json:"initialBalance"`
	}
	if !decode(r, &body) {
		writeError(w, 400, "INVALID_INPUT")
		return
	}
	wallet, err := h.wallets.Open(r.Context(), body.PlayerID, body.InitialBalance)
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	writeJSON(w, 201, walletResponse(wallet))
}

func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request) {
	if !internal(r) {
		writeError(w, 403, "FORBIDDEN")
		return
	}
	id, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeError(w, 400, "INVALID_INPUT")
		return
	}
	wallet, err := h.wallets.Get(r.Context(), id)
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	writeJSON(w, 200, walletResponse(wallet))
}

func (h *Handler) processWager(w http.ResponseWriter, r *http.Request) {
	p, ok := oidc.FromContext(r.Context())
	if !ok {
		writeError(w, 401, "UNAUTHORIZED")
		return
	}
	var op usecases.Operation
	if !decode(r, &op) {
		writeError(w, 400, "INVALID_INPUT")
		return
	}
	if p.ClientID != op.ProviderID || p.ClientID == "wallet-service" {
		writeError(w, 403, "FORBIDDEN")
		return
	}
	op.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	result, err := h.wager.Process(r.Context(), op)
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	status := 200
	if result.Status == model.PendingReference {
		status = 202
	}
	if result.Status == model.Rejected {
		status = 422
	}
	writeJSON(w, status, result)
}

func (h *Handler) getLedger(w http.ResponseWriter, r *http.Request) {
	if !internal(r) {
		writeError(w, http.StatusForbidden, "FORBIDDEN")
		return
	}
	walletID, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT")
		return
	}
	limit, err := ledgerLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT")
		return
	}
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT")
		return
	}
	page, err := h.reader.Ledger(r.Context(), walletID, cursor, limit)
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	var nextCursor *string
	if page.NextCursor != nil {
		value, err := encodeCursor(*page.NextCursor)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "TEMPORARY_UNAVAILABLE")
			return
		}
		nextCursor = &value
	}
	writeJSON(w, http.StatusOK, struct {
		Entries    []usecases.LedgerEntry `json:"entries"`
		NextCursor *string                `json:"nextCursor,omitempty"`
	}{Entries: page.Entries, NextCursor: nextCursor})
}

func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("transactionId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT")
		return
	}
	var transaction usecases.Transaction
	if internal(r) {
		transaction, err = h.reader.Transaction(r.Context(), id)
	} else {
		principal, ok := oidc.FromContext(r.Context())
		if !ok || principal.ClientID == "wallet-service" {
			writeError(w, http.StatusForbidden, "FORBIDDEN")
			return
		}
		transaction, err = h.reader.TransactionForProvider(r.Context(), id, principal.ClientID)
	}
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, transaction)
}

func (h *Handler) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	if !canReadTransaction(r, providerID) {
		writeError(w, http.StatusForbidden, "FORBIDDEN")
		return
	}
	transaction, err := h.reader.TransactionByExternal(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, transaction)
}

func (h *Handler) reconcileWallet(w http.ResponseWriter, r *http.Request) {
	if !internal(r) {
		writeError(w, http.StatusForbidden, "FORBIDDEN")
		return
	}
	walletID, err := uuid.Parse(r.PathValue("walletId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT")
		return
	}
	reconciliation, err := h.reader.Reconcile(r.Context(), walletID)
	if err != nil {
		writeUsecaseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliation)
}

func canReadTransaction(r *http.Request, providerID string) bool {
	if internal(r) {
		return true
	}
	p, ok := oidc.FromContext(r.Context())
	return ok && providerID != "" && p.ClientID == providerID
}

func ledgerLimit(r *http.Request) (int, error) {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return 50, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 100 {
		return 0, errors.New("invalid ledger limit")
	}
	return limit, nil
}

func encodeCursor(cursor usecases.LedgerCursor) (string, error) {
	payload, err := json.Marshal(struct {
		CreatedAt time.Time `json:"createdAt"`
		ID        uuid.UUID `json:"id"`
	}{CreatedAt: cursor.CreatedAt, ID: cursor.ID})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(value string) (*usecases.LedgerCursor, error) {
	if value == "" {
		return nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	var cursor struct {
		CreatedAt time.Time `json:"createdAt"`
		ID        uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.CreatedAt.IsZero() || cursor.ID == uuid.Nil {
		return nil, errors.New("invalid ledger cursor")
	}
	return &usecases.LedgerCursor{CreatedAt: cursor.CreatedAt, ID: cursor.ID}, nil
}

func internal(r *http.Request) bool {
	p, ok := oidc.FromContext(r.Context())
	return ok && p.ClientID == "wallet-service"
}
func decode(r *http.Request, out any) bool {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	return d.Decode(out) == nil
}
func walletResponse(w model.Wallet) any {
	return map[string]any{"id": w.ID(), "playerId": w.PlayerID(), "balance": w.Balance(), "version": w.Version()}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"code": code})
}
func writeUsecaseError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecases.ErrInvalidInput), errors.Is(err, model.ErrInvalidWallet), errors.Is(err, model.ErrInvalidMoney):
		writeError(w, 400, "INVALID_INPUT")
	case errors.Is(err, usecases.ErrConflict):
		writeError(w, 409, "CONFLICT")
	case errors.Is(err, usecases.ErrNotFound):
		writeError(w, 404, "NOT_FOUND")
	default:
		writeError(w, 503, "TEMPORARY_UNAVAILABLE")
	}
}
