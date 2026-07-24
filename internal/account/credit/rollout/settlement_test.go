package rollout

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type recordingSettlementObserver struct {
	calls int
}

func (o *recordingSettlementObserver) ObserveAccount(context.Context, uuid.UUID) error {
	o.calls++
	return nil
}

func TestSettlementObserverRequiresSelectedEnforce(t *testing.T) {
	accountID := uuid.New()
	enforce := &recordingSettlementObserver{}

	off := NewSettlementObserver(NewController(offPolicy(ComponentAPI), nil), enforce)
	require.NoError(t, off.ObserveAccount(context.Background(), accountID))

	shadowPolicy := Parse(Config{
		MasterEnabled: true, SchemaReady: true, Component: ComponentAPI,
		Mode: string(ModeShadow), BasisPoints: "10000",
		AllowlistSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CoreManifestSHA: "1111111111111111111111111111111111111111",
		BillingSHA:      "2222222222222222222222222222222222222222",
	})
	shadow := NewSettlementObserver(NewController(shadowPolicy, nil), enforce)
	require.NoError(t, shadow.ObserveAccount(context.Background(), accountID))

	excludedPolicy := Parse(Config{
		MasterEnabled: true, SchemaReady: true, Component: ComponentAPI,
		Mode: string(ModeEnforce), BasisPoints: "0",
		AllowlistSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CoreManifestSHA: "1111111111111111111111111111111111111111",
		BillingSHA:      "2222222222222222222222222222222222222222",
	})
	excluded := NewSettlementObserver(NewController(excludedPolicy, nil), enforce)
	require.NoError(t, excluded.ObserveAccount(context.Background(), accountID))

	enforcePolicy := Parse(Config{
		MasterEnabled: true, SchemaReady: true, Component: ComponentAPI,
		Mode: string(ModeEnforce), BasisPoints: "10000",
		AllowlistSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CoreManifestSHA: "1111111111111111111111111111111111111111",
		BillingSHA:      "2222222222222222222222222222222222222222",
	})
	selected := NewSettlementObserver(NewController(enforcePolicy, nil), enforce)
	require.NoError(t, selected.ObserveAccount(context.Background(), accountID))

	require.Equal(t, 1, enforce.calls)
}
