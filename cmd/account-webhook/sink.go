package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Notice is the payload a NoticeSink forwards. Provider and Mode are set from
// the matched route, never from the body — the ingress does not decrypt,
// verify, look anything up, or hold any provider credential (see
// cmd/account-webhook/main.go package doc). Fields carries only the six
// NewebPay MPG parameters this handler reads: Status, MerchantID, TradeInfo,
// TradeSha, Version, EncryptType.
type Notice struct {
	Provider   string            `json:"provider"`
	Mode       string            `json:"mode"`
	ReceivedAt time.Time         `json:"received_at"`
	Fields     map[string]string `json:"fields"`
	RequestID  string            `json:"request_id"`
}

// NoticeSink hands a received Notice onward. The real implementation enqueues
// it for the notice worker (N10, mirrorstack-infra#435); SendMessage succeeding
// is the only thing that earns the caller a 200, so a sink failure must be
// reported as an error here, never swallowed.
type NoticeSink interface {
	Send(ctx context.Context, n Notice) error
}

// sendMessageAPI is the narrow sqs.Client surface SQSSink depends on, so
// tests substitute a fake without a real AWS call.
type sendMessageAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSSink is the production NoticeSink: one SendMessage per notice, to the
// queue the infra half of this tracker (mirrorstack-infra#435) provisions.
// Until that queue is deployed, this type is still exercised in tests only
// through a fake satisfying sendMessageAPI.
type SQSSink struct {
	api      sendMessageAPI
	queueURL string
}

// NewSQSSink builds an SQSSink from an already-loaded aws.Config (region and
// credentials resolve through the SDK's default chain — the Lambda's own
// execution role in production).
func NewSQSSink(cfg aws.Config, queueURL string) *SQSSink {
	return &SQSSink{api: sqs.NewFromConfig(cfg), queueURL: queueURL}
}

// Send marshals the notice as the SQS message body and sends it. No key, no
// decryption, no verification happens here or anywhere else in this binary.
func (s *SQSSink) Send(ctx context.Context, n Notice) error {
	body, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notice: %w", err)
	}
	_, err = s.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("sqs SendMessage: %w", err)
	}
	return nil
}

// InMemorySink is the NoticeSink for local runs and tests — "serves local
// runs and tests" per billing-engine#295. Set Err to force a send failure
// (exercises the 503 path); otherwise every Send succeeds and is recorded.
type InMemorySink struct {
	mu      sync.Mutex
	Notices []Notice
	Err     error
}

// Send records n, or returns the configured Err without recording anything.
func (s *InMemorySink) Send(_ context.Context, n Notice) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.Notices = append(s.Notices, n)
	return nil
}

// Count returns how many notices have been recorded so far.
func (s *InMemorySink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Notices)
}

// errSinkNotConfigured is unconfiguredSink's Send error. The route handler
// recognizes it and answers 501, the same as every other unwired path,
// instead of the 503 a real send failure gets.
var errSinkNotConfigured = errors.New("notice sink not configured")

// unconfiguredSink stands in for the real SQS sink when
// NEWEBPAY_NOTICE_QUEUE_URL is unset under Lambda — the case before the infra
// half of this tracker (mirrorstack-infra#435) has deployed the queue. Using
// config.MustEnv here would os.Exit(1) at cold start and crash-loop the
// whole billing-webhook Lambda, which answers every other route too; this
// sink instead lets the process start and the notify route degrade to the
// same "not wired" answer the route gives for every other path today.
type unconfiguredSink struct{}

func (unconfiguredSink) Send(context.Context, Notice) error {
	return errSinkNotConfigured
}
