package tappaygateway

import (
	"context"
	"errors"
	"testing"
)

func TestFakeClient_ScriptsInOrderAndRecordsCalls(t *testing.T) {
	fake := &FakeClient{}
	fake.ScriptPayByPrime(PayByPrimeResult{Status: 0, RecTradeID: "D1"}, nil)
	fake.ScriptPayByPrime(PayByPrimeResult{}, errors.New("boom"))

	first, err := fake.PayByPrime(context.Background(), PayByPrimeParams{OrderNumber: "o1"})
	if err != nil || first.RecTradeID != "D1" {
		t.Fatalf("first call = %+v, %v", first, err)
	}
	_, err = fake.PayByPrime(context.Background(), PayByPrimeParams{OrderNumber: "o2"})
	if err == nil {
		t.Fatal("expected scripted error on second call")
	}

	if len(fake.PayByPrimeCalls) != 2 {
		t.Fatalf("recorded %d calls, want 2", len(fake.PayByPrimeCalls))
	}
	if fake.PayByPrimeCalls[0].OrderNumber != "o1" || fake.PayByPrimeCalls[1].OrderNumber != "o2" {
		t.Fatalf("calls recorded out of order: %+v", fake.PayByPrimeCalls)
	}
}

func TestFakeClient_UnscriptedCallPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic calling QueryRecord with no scripted response")
		}
	}()
	(&FakeClient{}).QueryRecord(context.Background(), QueryRecordParams{})
}
