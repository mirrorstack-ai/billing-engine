package tappaygateway

import (
	"context"
	"fmt"
)

// GatewayClient is the three verbs this package's Client exposes.
// Declared narrow so a caller depends only on what it uses, and so
// FakeClient and *Client are interchangeable in tests.
type GatewayClient interface {
	PayByPrime(ctx context.Context, req PayByPrimeParams) (PayByPrimeResult, error)
	QueryRecord(ctx context.Context, req QueryRecordParams) (QueryRecordResult, error)
	Refund(ctx context.Context, req RefundParams) (RefundResult, error)
}

var (
	_ GatewayClient = (*Client)(nil)
	_ GatewayClient = (*FakeClient)(nil)
)

// FakeClient is an in-memory GatewayClient for tests outside this
// package. It is not a _test.go file, the same reason
// stripetest/recorder.go isn't: other packages' tests need to import
// it too.
//
// Each call records the request it received (so a test can assert on
// what was sent — e.g. that Remember was never true, or that
// BankTransactionID was always set) and returns the next scripted
// response, in order, panicking if the script runs out.
type FakeClient struct {
	PayByPrimeCalls  []PayByPrimeParams
	QueryRecordCalls []QueryRecordParams
	RefundCalls      []RefundParams

	payByPrimeScript  []fakeResult[PayByPrimeResult]
	queryRecordScript []fakeResult[QueryRecordResult]
	refundScript      []fakeResult[RefundResult]
}

type fakeResult[T any] struct {
	value T
	err   error
}

// ScriptPayByPrime queues one PayByPrime response, in call order.
func (f *FakeClient) ScriptPayByPrime(result PayByPrimeResult, err error) {
	f.payByPrimeScript = append(f.payByPrimeScript, fakeResult[PayByPrimeResult]{result, err})
}

// ScriptQueryRecord queues one QueryRecord response, in call order.
func (f *FakeClient) ScriptQueryRecord(result QueryRecordResult, err error) {
	f.queryRecordScript = append(f.queryRecordScript, fakeResult[QueryRecordResult]{result, err})
}

// ScriptRefund queues one Refund response, in call order.
func (f *FakeClient) ScriptRefund(result RefundResult, err error) {
	f.refundScript = append(f.refundScript, fakeResult[RefundResult]{result, err})
}

func (f *FakeClient) PayByPrime(_ context.Context, req PayByPrimeParams) (PayByPrimeResult, error) {
	f.PayByPrimeCalls = append(f.PayByPrimeCalls, req)
	if len(f.payByPrimeScript) == 0 {
		panic(fmt.Sprintf("tappaygateway.FakeClient: PayByPrime called with no scripted response (call %d)", len(f.PayByPrimeCalls)))
	}
	next := f.payByPrimeScript[0]
	f.payByPrimeScript = f.payByPrimeScript[1:]
	return next.value, next.err
}

func (f *FakeClient) QueryRecord(_ context.Context, req QueryRecordParams) (QueryRecordResult, error) {
	f.QueryRecordCalls = append(f.QueryRecordCalls, req)
	if len(f.queryRecordScript) == 0 {
		panic(fmt.Sprintf("tappaygateway.FakeClient: QueryRecord called with no scripted response (call %d)", len(f.QueryRecordCalls)))
	}
	next := f.queryRecordScript[0]
	f.queryRecordScript = f.queryRecordScript[1:]
	return next.value, next.err
}

func (f *FakeClient) Refund(_ context.Context, req RefundParams) (RefundResult, error) {
	f.RefundCalls = append(f.RefundCalls, req)
	if len(f.refundScript) == 0 {
		panic(fmt.Sprintf("tappaygateway.FakeClient: Refund called with no scripted response (call %d)", len(f.RefundCalls)))
	}
	next := f.refundScript[0]
	f.refundScript = f.refundScript[1:]
	return next.value, next.err
}
