package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"apostas_api/internal/infra/oidc"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
)

type Handler struct {
	wallets *usecases.Wallets
	wager   *usecases.Wager
	pool    *pgxpool.Pool
}

func NewHandler(wallets *usecases.Wallets, wager *usecases.Wager, pool *pgxpool.Pool) *Handler {
	return &Handler{wallets, wager, pool}
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
	mux.HandleFunc("POST /wagering/transactions", h.processWager)
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
