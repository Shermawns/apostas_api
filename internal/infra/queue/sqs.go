package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"apostas_api/internal/infra/config"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Message struct {
	ID            string
	ReceiptHandle string
	Body          string
}

type Client struct {
	client    *sqs.Client
	inputURL  string
	outputURL string
}

func NewClient(cfg config.Config) (*Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AWSAccessKey, cfg.AWSSecretKey, "")),
	}
	if cfg.AWSEndpoint != "" {
		loadOptions = append(loadOptions, awsconfig.WithBaseEndpoint(cfg.AWSEndpoint))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return &Client{client: sqs.NewFromConfig(awsCfg), inputURL: cfg.InputQueueURL, outputURL: cfg.OutputQueueURL}, nil
}

func (c *Client) Ready(ctx context.Context) error {
	for _, queueURL := range []string{c.inputURL, c.outputURL} {
		_, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &queueURL})
		if err != nil {
			return fmt.Errorf("check SQS queue: %w", err)
		}
	}
	return nil
}

func (c *Client) Receive(ctx context.Context) ([]Message, error) {
	response, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &c.inputURL,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     10,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(response.Messages))
	for _, message := range response.Messages {
		if message.MessageId == nil || message.ReceiptHandle == nil || message.Body == nil {
			continue
		}
		messages = append(messages, Message{ID: *message.MessageId, ReceiptHandle: *message.ReceiptHandle, Body: *message.Body})
	}
	return messages, nil
}

func (c *Client) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.inputURL, ReceiptHandle: &receiptHandle})
	return err
}

func (c *Client) Publish(ctx context.Context, eventID, groupID, payload string) error {
	if eventID == "" || groupID == "" {
		return errors.New("event and aggregate identity are required")
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               &c.outputURL,
		MessageBody:            &payload,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &eventID,
	})
	return err
}

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		return time.Second
	}
	delay := time.Second << min(attempt-1, 8)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
