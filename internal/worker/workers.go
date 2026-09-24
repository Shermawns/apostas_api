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

	"apostas_api/internal/infra/metrics"
	"apostas_api/internal/infra/postgres"
	"apostas_api/internal/infra/queue"
	"apostas_api/internal/usecases"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

const consumerName = "wager-transaction-consumer"

var errPermanentMessage = errors.New("permanent SQS message error")
var errFaultInjected = errors.New("fault injected after durable work")

type InputConsumer struct {
	queue       inputQueue
	wager       wagerProcessor
	afterCommit func(queue.Message) error
}

func NewInputConsumer(queue *queue.Client, wager *usecases.Wager) *InputConsumer {
	return &InputConsumer{queue: queue, wager: wager}
}

type inputQueue interface {
	Receive(context.Context) ([]queue.Message, error)
	DeadLetter(context.Context, queue.Message) error
	ChangeVisibility(context.Context, string, time.Duration) error
	Delete(context.Context, string) error
}

type wagerProcessor interface {
	ProcessInbox(context.Context, usecases.InboxMessage, usecases.Operation) (usecases.Result, error)
	Fail(context.Context, usecases.Operation, string) (usecases.Result, error)
}

func (w *InputConsumer) Name() string { return consumerName }

func (w *InputConsumer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
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
			started := time.Now()
			handleErr := w.handle(ctx, message)
			if ctx.Err() != nil {
				releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				if handleErr == nil {
					_ = w.queue.Delete(releaseCtx, message.ReceiptHandle)
				} else {
					_ = w.queue.ChangeVisibility(releaseCtx, message.ReceiptHandle, 0)
				}
				cancel()
				return
			}
			if handleErr != nil {
				slog.Error("SQS message processing failed", "worker", w.Name(), "sqsMessageId", message.ID, "receiveCount", message.ReceiveCount, "error", handleErr)
				if message.ReceiveCount >= 5 && !errors.Is(handleErr, errPermanentMessage) && !errors.Is(handleErr, usecases.ErrInvalidInput) && !errors.Is(handleErr, usecases.ErrConflict) {
					if result, err := w.recordPermanentFailure(ctx, message); err == nil {
						slog.Error("SQS processing permanently failed", "sqsMessageId", message.ID, "transactionId", result.TransactionID, "failureCode", result.FailureCode)
						if err := w.queue.Delete(ctx, message.ReceiptHandle); err != nil {
							slog.Error("SQS source deletion failed", "sqsMessageId", message.ID, "error", err)
						}
						metrics.Inc(`apostas_wager_results_total{status="FAILED"}`)
						continue
					}
				}
				if errors.Is(handleErr, errPermanentMessage) || errors.Is(handleErr, usecases.ErrInvalidInput) || errors.Is(handleErr, usecases.ErrConflict) || message.ReceiveCount >= 5 {
					if err := w.queue.DeadLetter(ctx, message); err != nil {
						slog.Error("SQS dead letter delivery failed", "sqsMessageId", message.ID, "error", err)
						continue
					}
					if err := w.queue.Delete(ctx, message.ReceiptHandle); err != nil {
						slog.Error("SQS source deletion failed", "sqsMessageId", message.ID, "error", err)
					}
					metrics.Inc("apostas_sqs_dlq_total")
					continue
				}
				metrics.Inc("apostas_sqs_retries_total")
				if err := w.queue.ChangeVisibility(ctx, message.ReceiptHandle, queue.RetryDelay(message.ReceiveCount)); err != nil {
					slog.Error("SQS retry scheduling failed", "sqsMessageId", message.ID, "error", err)
				}
				continue
			}
			if w.afterCommit != nil {
				if err := w.afterCommit(message); err != nil {
					slog.Warn("input consumer stopped by fault injection", "worker", w.Name(), "sqsMessageId", message.ID, "error", err)
					return
				}
			}
			metrics.Inc("apostas_sqs_messages_total{status=\"processed\"}")
			metrics.Add("apostas_sqs_processing_latency_milliseconds_total", uint64(time.Since(started).Milliseconds()))
			if err := w.queue.Delete(ctx, message.ReceiptHandle); err != nil {
				slog.Error("SQS message deletion failed", "sqsMessageId", message.ID, "error", err)
			}
		}
	}
}

func (w *InputConsumer) recordPermanentFailure(ctx context.Context, message queue.Message) (usecases.Result, error) {
	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil || envelope.Type != "WagerTransactionRequested" {
		return usecases.Result{}, errPermanentMessage
	}
	var request struct {
		usecases.Operation
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if err := json.Unmarshal(envelope.Data, &request); err != nil {
		return usecases.Result{}, err
	}
	request.Operation.IdempotencyKey = request.IdempotencyKey
	return w.wager.Fail(ctx, request.Operation, "PROCESSING_RETRIES_EXHAUSTED")
}

func (w *InputConsumer) handle(ctx context.Context, message queue.Message) error {
	var envelope struct {
		MessageID string          `json:"messageId"`
		Type      string          `json:"type"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
		return errors.Join(errPermanentMessage, err)
	}
	if envelope.MessageID == "" || envelope.Type != "WagerTransactionRequested" {
		return errPermanentMessage
	}
	var request struct {
		usecases.Operation
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if err := json.Unmarshal(envelope.Data, &request); err != nil {
		return errors.Join(errPermanentMessage, err)
	}
	request.Operation.IdempotencyKey = request.IdempotencyKey
	sum := sha256.Sum256([]byte(message.Body))
	result, err := w.wager.ProcessInbox(ctx, usecases.InboxMessage{
		ConsumerName: consumerName,
		MessageID:    envelope.MessageID,
		PayloadHash:  hex.EncodeToString(sum[:]),
	}, request.Operation)
	if err == nil {
		slog.Info("SQS wager handled", "messageId", envelope.MessageID, "correlationId", result.TransactionID, "transactionId", result.TransactionID, "walletId", request.WalletID, "providerId", request.ProviderID, "status", result.Status, "idempotentReplay", result.IdempotentReplay)
		metrics.Inc(`apostas_wager_results_total{status="` + string(result.Status) + `"}`)
		if result.IdempotentReplay {
			metrics.Inc("apostas_wager_duplicates_total")
		}
	}
	return err
}

type OutboxPublisher struct {
	queue        outboxQueue
	store        outboxStore
	afterPublish func(postgres.OutboxEvent) error
}

type outboxQueue interface {
	Publish(context.Context, string, string, string) error
}

type outboxStore interface {
	ClaimOutbox(context.Context, int) ([]postgres.OutboxEvent, error)
	RetryOutbox(context.Context, uuid.UUID, time.Duration) error
	MarkOutboxPublished(context.Context, uuid.UUID) error
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
			metrics.Inc("apostas_outbox_retries_total")
			if retryErr := w.store.RetryOutbox(ctx, event.ID, queue.RetryDelay(event.Attempts+1)); retryErr != nil {
				return retryErr
			}
			continue
		}
		if w.afterPublish != nil {
			if err := w.afterPublish(event); err != nil {
				return err
			}
		}
		if err := w.store.MarkOutboxPublished(ctx, event.ID); err != nil {
			return err
		}
		metrics.Inc("apostas_outbox_published_total")
		metrics.Add("apostas_outbox_delay_milliseconds_total", uint64(time.Since(event.OccurredAt).Milliseconds()))
	}
	return nil
}

type Manager struct {
	queue   *queue.Client
	workers []Runner
	cancel  context.CancelFunc
	done    chan struct{}
}

func (m *Manager) Done() <-chan struct{} { return m.done }

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
			for {
				if err := manager.queue.Ready(ctx); err == nil {
					break
				} else if ctx.Err() != nil {
					return err
				}
				if !wait(ctx, time.Second) {
					return ctx.Err()
				}
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
