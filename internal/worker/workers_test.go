package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/infra/queue"
	"apostas_api/internal/model"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
)

func TestInputConsumerLeavesCommittedMessageForReplayWhenInterruptedBeforeAcknowledgement(t *testing.T) {
	message := wagerRequestMessage(t)
	processor := &replayProcessor{transactionID: uuid.New()}
	input := &fakeInputQueue{messages: []queue.Message{message}}
	consumer := &InputConsumer{
		queue: input,
		wager: processor,
		afterCommit: func(queue.Message) error {
			return errFaultInjected
		},
	}

	consumer.Run(context.Background())

	if input.deleteCalls != 0 {
		t.Fatalf("DeleteMessage calls = %d, want 0 after interruption", input.deleteCalls)
	}
	if processor.calls != 1 || processor.replays != 0 {
		t.Fatalf("first processing = calls %d, replays %d; want 1, 0", processor.calls, processor.replays)
	}

	// A replacement worker receives the same SQS delivery after its visibility timeout.
	if err := (&InputConsumer{queue: input, wager: processor}).handle(context.Background(), message); err != nil {
		t.Fatalf("replay committed inbox message: %v", err)
	}
	if processor.calls != 2 || processor.replays != 1 {
		t.Fatalf("replay = calls %d, replays %d; want 2, 1", processor.calls, processor.replays)
	}
}

func TestOutboxPublisherRepublishesStableEventAfterInterruptionBeforeMark(t *testing.T) {
	event := postgres.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: uuid.New(),
		Payload:     []byte(`{"eventId":"stable-event"}`),
		OccurredAt:  time.Now(),
	}
	store := &fakeOutboxStore{event: event}
	output := &fakeOutboxQueue{}
	publisher := &OutboxPublisher{
		queue: output,
		store: store,
		afterPublish: func(postgres.OutboxEvent) error {
			return errFaultInjected
		},
	}

	err := publisher.publish(context.Background())
	if !errors.Is(err, errFaultInjected) {
		t.Fatalf("first publish error = %v, want injected interruption", err)
	}
	if len(output.eventIDs) != 1 || output.eventIDs[0] != event.ID.String() {
		t.Fatalf("first published IDs = %v, want [%s]", output.eventIDs, event.ID)
	}
	if store.markCalls != 0 {
		t.Fatalf("MarkOutboxPublished calls = %d, want 0 after interruption", store.markCalls)
	}

	// Once the expired lease is claimed by a replacement worker, it republishes the
	// same event ID and only then marks the durable outbox record as published.
	if err := (&OutboxPublisher{queue: output, store: store}).publish(context.Background()); err != nil {
		t.Fatalf("publish recovered outbox event: %v", err)
	}
	if len(output.eventIDs) != 2 || output.eventIDs[1] != event.ID.String() {
		t.Fatalf("recovery published IDs = %v, want the stable event ID twice", output.eventIDs)
	}
	if store.markCalls != 1 || store.markedID != event.ID {
		t.Fatalf("outbox mark = calls %d, ID %s; want 1, %s", store.markCalls, store.markedID, event.ID)
	}
}

type fakeInputQueue struct {
	messages    []queue.Message
	deleteCalls int
}

func (q *fakeInputQueue) Receive(context.Context) ([]queue.Message, error) { return q.messages, nil }
func (q *fakeInputQueue) DeadLetter(context.Context, queue.Message) error  { return nil }
func (q *fakeInputQueue) ChangeVisibility(context.Context, string, time.Duration) error {
	return nil
}
func (q *fakeInputQueue) Delete(context.Context, string) error {
	q.deleteCalls++
	return nil
}

type replayProcessor struct {
	transactionID uuid.UUID
	calls         int
	replays       int
}

func (p *replayProcessor) ProcessInbox(context.Context, usecases.InboxMessage, usecases.Operation) (usecases.Result, error) {
	p.calls++
	replay := p.calls > 1
	if replay {
		p.replays++
	}
	return usecases.Result{TransactionID: p.transactionID, Status: model.Processed, IdempotentReplay: replay}, nil
}

type fakeOutboxQueue struct{ eventIDs []string }

func (q *fakeOutboxQueue) Publish(_ context.Context, eventID, _ string, _ string) error {
	q.eventIDs = append(q.eventIDs, eventID)
	return nil
}

type fakeOutboxStore struct {
	event     postgres.OutboxEvent
	markCalls int
	markedID  uuid.UUID
}

func (s *fakeOutboxStore) ClaimOutbox(context.Context, int) ([]postgres.OutboxEvent, error) {
	return []postgres.OutboxEvent{s.event}, nil
}
func (s *fakeOutboxStore) RetryOutbox(context.Context, uuid.UUID, time.Duration) error { return nil }
func (s *fakeOutboxStore) MarkOutboxPublished(_ context.Context, id uuid.UUID) error {
	s.markCalls++
	s.markedID = id
	return nil
}

func wagerRequestMessage(t *testing.T) queue.Message {
	t.Helper()
	money, err := model.ParseMoney("25.00", "BRL")
	if err != nil {
		t.Fatalf("parse money: %v", err)
	}
	body, err := json.Marshal(struct {
		MessageID string `json:"messageId"`
		Type      string `json:"type"`
		Data      struct {
			usecases.Operation
			IdempotencyKey string `json:"idempotencyKey"`
		} `json:"data"`
	}{
		MessageID: "sqs-message-1",
		Type:      "WagerTransactionRequested",
		Data: struct {
			usecases.Operation
			IdempotencyKey string `json:"idempotencyKey"`
		}{
			Operation: usecases.Operation{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-1",
				PlayerID:              uuid.New(),
				WalletID:              uuid.New(),
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  model.Bet,
				Money:                 money,
			},
			IdempotencyKey: "provider-a:external-1",
		},
	})
	if err != nil {
		t.Fatalf("marshal SQS request: %v", err)
	}
	return queue.Message{ID: "aws-message-1", ReceiptHandle: "receipt-1", Body: string(body), ReceiveCount: 1}
}
