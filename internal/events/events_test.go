package events

import (
	"encoding/json"
	"testing"
	"time"

	"apostas_api/internal/model"
	"github.com/google/uuid"
)

func TestConstructorsFixEnvelopeAndSnapshot(t *testing.T) {
	walletID, transactionID := uuid.New(), uuid.New()
	money, _ := model.ParseMoney("25.00", "BRL")
	before, _ := model.ParseMoney("100.00", "BRL")
	after, _ := model.ParseMoney("75.00", "BRL")
	data := WalletBalanceChanged{WalletID: walletID, TransactionID: transactionID, Direction: "DEBIT", Money: money, BalanceBefore: before, BalanceAfter: after, WalletVersion: 2}
	event, err := NewWalletBalanceChanged(data, "sqs-message-1")
	if err != nil {
		t.Fatal(err)
	}
	data.Direction = "CREDIT"
	payload := event.Payload()
	payload[0] = 'x'
	var envelope struct {
		EventID       uuid.UUID `json:"eventId"`
		EventType     string    `json:"eventType"`
		AggregateID   uuid.UUID `json:"aggregateId"`
		CorrelationID uuid.UUID `json:"correlationId"`
		CausationID   string    `json:"causationId"`
		OccurredAt    time.Time `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          struct {
			Direction string `json:"direction"`
		} `json:"data"`
	}
	if err := json.Unmarshal(event.Payload(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.EventID != event.ID() || envelope.EventType != "WalletBalanceChanged" || envelope.AggregateID != walletID || envelope.CorrelationID != transactionID || envelope.CausationID != "sqs-message-1" || envelope.Version != 1 || envelope.OccurredAt.Location() != time.UTC || envelope.Data.Direction != "DEBIT" {
		t.Fatalf("unexpected event envelope: %+v", envelope)
	}
	var raw struct {
		Data struct {
			Money model.Money `json:"money"`
		} `json:"data"`
	}
	if err := json.Unmarshal(event.Payload(), &raw); err != nil || raw.Data.Money != money {
		t.Fatalf("money event data=%+v err=%v", raw, err)
	}

	processed, _ := NewWagerTransactionProcessed(walletID, transactionID, "provider-a", model.Bet, "")
	rejected, _ := NewWagerTransactionRejected(walletID, transactionID, "provider-a", model.Bet, "INSUFFICIENT_FUNDS", "")
	pending, _ := NewWagerTransactionPendingReference(walletID, transactionID, "provider-a", model.Refund, "")
	for _, pair := range []struct {
		event    Event
		typeName string
	}{{processed, "WagerTransactionProcessed"}, {rejected, "WagerTransactionRejected"}, {pending, "WagerTransactionPendingReference"}} {
		if pair.event.Type() != pair.typeName {
			t.Fatalf("type=%s want=%s", pair.event.Type(), pair.typeName)
		}
	}
}
