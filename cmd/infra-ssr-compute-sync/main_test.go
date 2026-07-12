package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
)

// TestHandler_BatchErrorsReturnRetryableError is the direct regression test
// for the MEDIUM-severity silent-creeping-data-loss bug: the Lambda handler
// used to return nil (success) whenever enumeration succeeded, even if one or
// more GetMetricData batches failed — meaning EventBridge never retried a
// failed batch, and its usage would simply age out of the lookback window if
// the SAME batch also failed on the next scheduled run. The handler must now
// surface a non-nil, retryable error whenever res.BatchErrors > 0, even
// though res.Failed (enumeration failure) is false, so EventBridge's own
// retry policy gets a chance to recover the failed batch's data before it
// ages out.
func TestHandler_BatchErrorsReturnRetryableError(t *testing.T) {
	fn := testFn("ms-apphost-batch-err", 512)
	lister := &fakeLister{fns: []awslambdainv.SSRFunction{fn}}
	querier := &fakeQuerier{errByCall: map[int]error{0: errors.New("throttled")}}
	idle := &fakeIdle{idle: map[string]bool{}}
	svc := newSvc(newFakeStore())

	h := handler(svc, lister, querier, idle)
	err := h(context.Background(), events.CloudWatchEvent{Time: at})
	if err == nil {
		t.Fatal("handler returned nil error, want a retryable error when a GetMetricData batch failed")
	}
}

// TestHandler_NoBatchErrorsReturnsNil confirms the happy path is unaffected:
// a clean run (no enumeration failure, no batch errors) must still return
// nil so EventBridge does NOT retry a run that already succeeded.
func TestHandler_NoBatchErrorsReturnsNil(t *testing.T) {
	fn := testFn("ms-apphost-ok", 512)
	lister := &fakeLister{fns: []awslambdainv.SSRFunction{fn}}
	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	ts := windows[0].start
	querier := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{100}},
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{10}},
		},
	}}
	idle := &fakeIdle{idle: map[string]bool{}}
	svc := newSvc(newFakeStore())

	h := handler(svc, lister, querier, idle)
	if err := h(context.Background(), events.CloudWatchEvent{Time: at}); err != nil {
		t.Fatalf("handler returned error %v, want nil for a clean run", err)
	}
}

// TestHandler_EnumerationFailureStillReturnsError confirms enumeration
// failure (res.Failed) still surfaces its own error, independent of the new
// BatchErrors escalation path.
func TestHandler_EnumerationFailureStillReturnsError(t *testing.T) {
	lister := &fakeLister{err: errors.New("aws unreachable")}
	querier := &fakeQuerier{}
	idle := &fakeIdle{}
	svc := newSvc(newFakeStore())

	h := handler(svc, lister, querier, idle)
	if err := h(context.Background(), events.CloudWatchEvent{Time: at}); err == nil {
		t.Fatal("handler returned nil error, want an error when ListSSRFunctions fails")
	}
}
