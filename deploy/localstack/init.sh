#!/bin/sh
set -eu
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true
dlq_arn=$(awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "FifoQueue=true,VisibilityTimeout=30,RedrivePolicy={\"deadLetterTargetArn\":\"${dlq_arn}\",\"maxReceiveCount\":\"5\"}"
awslocal sqs create-queue --queue-name wager-events.fifo --attributes FifoQueue=true

