package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/infra/queue"
	"apostas_api/internal/usecases"
	"go.uber.org/fx"
)

const consumerName = "wager-transaction-consumer"

type InputConsumer struct {
	queue *queue.Client
	wager *usecases.Wager
}

func NewInputConsumer(queue *queue.Client, wager *usecases.Wager) *InputConsumer {
	return &InputConsumer{queue: queue, wager: wager}
}

func (w *InputConsumer) Name() string { return consumerName }

func (w *InputConsumer) Run(ctx context.Context) {
	for {
		messages, err := w.queue.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("SQS receive failed", "worker", w.Name(), "error", err)
			if !wait(ctx, time.Second) {
				return
			}
			continue
		}
		for _, message := range messages {
			if err := w.handle(ctx, message); err != nil {
				slog.Error("SQS message processing failed", "worker", w.Name(), "messageId", message.ID, "error", err)
				continue
			}
			if err := w.queue.Delete(ctx, message.ReceiptHandle); err != nil && ctx.Err() == nil {
				slog.Error("SQS message deletion failed", "worker", w.Name(), "messageId", message.ID, "error", err)
			}
		}
	}
}

func (w *InputConsumer) handle(ctx context.Context, message queue.Message) error {
	var envelope struct {
		MessageID string          `json:"messageId"`
		Type      string          `json:"type"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
		return err
	}
	if envelope.MessageID == "" || envelope.Type != "WagerTransactionRequested" {
		return errors.New("unsupported SQS message")
	}
	var request struct {
		usecases.Operation
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if err := json.Unmarshal(envelope.Data, &request); err != nil {
		return err
	}
	request.Operation.IdempotencyKey = request.IdempotencyKey
	sum := sha256.Sum256([]byte(message.Body))
	_, err := w.wager.ProcessInbox(ctx, usecases.InboxMessage{
		ConsumerName: consumerName,
		MessageID:    envelope.MessageID,
		PayloadHash:  hex.EncodeToString(sum[:]),
	}, request.Operation)
	return err
}

type OutboxPublisher struct {
	queue *queue.Client
	store *postgres.Store
}

type ReferenceResolver struct {
	store *postgres.Store
}

func NewReferenceResolver(store *postgres.Store) *ReferenceResolver {
	return &ReferenceResolver{store: store}
}

func (w *ReferenceResolver) Name() string { return "pending-reference-resolver" }

func (w *ReferenceResolver) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		processed, err := w.store.ResolvePendingReference(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("pending reference resolution failed", "worker", w.Name(), "error", err)
		}
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func NewOutboxPublisher(queue *queue.Client, store *postgres.Store) *OutboxPublisher {
	return &OutboxPublisher{queue: queue, store: store}
}

func (w *OutboxPublisher) Name() string { return "outbox-publisher" }

func (w *OutboxPublisher) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := w.publish(ctx); err != nil && ctx.Err() == nil {
			slog.Error("outbox publishing failed", "worker", w.Name(), "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *OutboxPublisher) publish(ctx context.Context) error {
	events, err := w.store.ClaimOutbox(ctx, 20)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := w.queue.Publish(ctx, event.ID.String(), event.AggregateID.String(), string(event.Payload)); err != nil {
			if retryErr := w.store.RetryOutbox(ctx, event.ID, queue.RetryDelay(event.Attempts+1)); retryErr != nil {
				return retryErr
			}
			continue
		}
		if err := w.store.MarkOutboxPublished(ctx, event.ID); err != nil {
			return err
		}
	}
	return nil
}

type Manager struct {
	queue   *queue.Client
	workers []Runner
	cancel  context.CancelFunc
	done    chan struct{}
}

type Runner interface {
	Name() string
	Run(context.Context)
}

func NewManager(queue *queue.Client, input *InputConsumer, outbox *OutboxPublisher, references *ReferenceResolver) *Manager {
	return &Manager{queue: queue, workers: []Runner{input, outbox, references}}
}

func Register(lc fx.Lifecycle, manager *Manager) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := manager.queue.Ready(ctx); err != nil {
				return err
			}
			workerCtx, cancel := context.WithCancel(context.Background())
			manager.cancel = cancel
			manager.done = make(chan struct{})
			var group sync.WaitGroup
			group.Add(len(manager.workers))
			for _, runner := range manager.workers {
				runner := runner
				go func() {
					defer group.Done()
					slog.Info("worker started", "worker", runner.Name())
					runner.Run(workerCtx)
					slog.Info("worker stopped", "worker", runner.Name())
				}()
			}
			go func() {
				group.Wait()
				close(manager.done)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if manager.cancel == nil {
				return nil
			}
			manager.cancel()
			select {
			case <-manager.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
