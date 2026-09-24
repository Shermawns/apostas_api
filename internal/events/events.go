package events

import (
	"encoding/json"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

type Event struct {
	id          uuid.UUID
	aggregateID uuid.UUID
	eventType   string
	payload     []byte
}

func (e Event) ID() uuid.UUID          { return e.id }
func (e Event) AggregateID() uuid.UUID { return e.aggregateID }
func (e Event) Type() string           { return e.eventType }
func (e Event) Payload() []byte        { return append([]byte(nil), e.payload...) }

type WalletBalanceChanged struct {
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         model.Money `json:"money"`
	BalanceBefore model.Money `json:"balanceBefore"`
	BalanceAfter  model.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

type WagerTransactionProcessed struct {
	TransactionID uuid.UUID    `json:"transactionId"`
	ProviderID    string       `json:"providerId,omitempty"`
	Kind          model.Kind   `json:"kind"`
	Status        model.Status `json:"status"`
}

type WagerTransactionRejected struct {
	TransactionID uuid.UUID    `json:"transactionId"`
	ProviderID    string       `json:"providerId"`
	Kind          model.Kind   `json:"kind"`
	Status        model.Status `json:"status"`
	FailureCode   string       `json:"failureCode"`
}

type WagerTransactionPendingReference struct {
	TransactionID uuid.UUID    `json:"transactionId"`
	ProviderID    string       `json:"providerId"`
	Kind          model.Kind   `json:"kind"`
	Status        model.Status `json:"status"`
}

func NewWalletBalanceChanged(data WalletBalanceChanged, causationID string) (Event, error) {
	return build(data.WalletID, data.TransactionID, causationID, "WalletBalanceChanged", data)
}

func NewWagerTransactionProcessed(aggregateID, transactionID uuid.UUID, providerID string, kind model.Kind, causationID string) (Event, error) {
	return build(aggregateID, transactionID, causationID, "WagerTransactionProcessed", WagerTransactionProcessed{TransactionID: transactionID, ProviderID: providerID, Kind: kind, Status: model.Processed})
}

func NewWagerTransactionRejected(aggregateID, transactionID uuid.UUID, providerID string, kind model.Kind, failureCode, causationID string) (Event, error) {
	return build(aggregateID, transactionID, causationID, "WagerTransactionRejected", WagerTransactionRejected{TransactionID: transactionID, ProviderID: providerID, Kind: kind, Status: model.Rejected, FailureCode: failureCode})
}

func NewWagerTransactionPendingReference(aggregateID, transactionID uuid.UUID, providerID string, kind model.Kind, causationID string) (Event, error) {
	return build(aggregateID, transactionID, causationID, "WagerTransactionPendingReference", WagerTransactionPendingReference{TransactionID: transactionID, ProviderID: providerID, Kind: kind, Status: model.PendingReference})
}

func build(aggregateID, correlationID uuid.UUID, causationID, eventType string, data any) (Event, error) {
	id := uuid.New()
	payload, err := json.Marshal(struct {
		EventID       uuid.UUID `json:"eventId"`
		EventType     string    `json:"eventType"`
		AggregateID   uuid.UUID `json:"aggregateId"`
		CorrelationID uuid.UUID `json:"correlationId"`
		CausationID   string    `json:"causationId,omitempty"`
		OccurredAt    time.Time `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          any       `json:"data"`
	}{id, eventType, aggregateID, correlationID, causationID, time.Now().UTC(), 1, data})
	if err != nil {
		return Event{}, err
	}
	return Event{id: id, aggregateID: aggregateID, eventType: eventType, payload: payload}, nil
}
