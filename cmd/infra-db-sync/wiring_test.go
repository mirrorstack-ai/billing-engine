package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
)

const (
	testSHA40      = "0123456789abcdef0123456789abcdef01234567"
	testEmptyAllow = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func enforceWorkerPolicy() rollout.Policy {
	return rollout.Parse(rollout.Config{
		MasterEnabled: true, SchemaReady: true, Component: rollout.ComponentWorker,
		Mode: string(rollout.ModeEnforce), BasisPoints: "10000",
		AllowlistSHA256: testEmptyAllow, CoreManifestSHA: testSHA40, BillingSHA: testSHA40,
	})
}

// 🔴 infra-db-sync is a usage-only sampler: infra#377 removed STRIPE_SECRET_KEY
// from its environment. config.MustEnv exits the process when a key is missing, so
// building the auto-top-up executor on an enforce stage would crash the Lambda at
// startup (os.Exit(1) fails this test binary). The sampler records usage; it never
// settles a charge, so it never builds a payment executor.
func TestWireUsageService_EnforceWithoutStripeKeyDoesNotExit(t *testing.T) {
	t.Setenv("STRIPE_SECRET_KEY", "")
	t.Setenv("REDIS_URL", "")
	policy := enforceWorkerPolicy()
	require.Equal(t, rollout.ModeEnforce, policy.Mode(), "the fixture must really be an enforce policy")

	svc := wireUsageService(nil, policy)
	require.NotNil(t, svc)
}
