//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 047 (ms_billing.app_custom_domains) — exercises the custom-domain
// activation-charge SQL against a real Postgres: the idempotent live-hostname
// insert, the activation-period sweep work list (activated-account + zero-grace
// gates, carrying the account anchor), the DISJOINT boundary count (which
// deliberately ignores charge_resolved), and the prospective, re-activatable
// removal. These are the proration-leg queries the unit fakes only approximate.
func TestCustomDomains_Integration_SweepWorkListAndBoundaryCount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	// Activate so the sweep's account activated_at IS NOT NULL gate passes; the
	// anchor day (4) sets each domain's activation-period window.
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET activated_at = $2 WHERE id = $1`,
		acct.String(), mustTime(t, "2026-05-04T00:00:00Z"))
	require.NoError(t, err)

	app := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, app, acct, uuid.Nil, 0, mustTime(t, "2026-06-01T00:00:00Z"), ""))

	activated := mustTime(t, "2026-06-10T00:00:00Z")
	require.NoError(t, store.InsertDomain(ctx, acct, app, "one.example.test", activated))

	// Idempotent re-activation of the SAME live hostname is a no-op: the partial
	// unique index keeps the first activation's id/time (ON CONFLICT DO NOTHING).
	dom, found, err := store.DomainByHostname(ctx, "one.example.test")
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, store.InsertDomain(ctx, acct, app, "one.example.test", activated.AddDate(0, 0, 5)))
	dom2, _, err := store.DomainByHostname(ctx, "one.example.test")
	require.NoError(t, err)
	require.Equal(t, dom.ID, dom2.ID, "a retry never mints a second live row")
	require.True(t, activated.Equal(dom2.ActivatedAt), "the first activation instant is immutable across retries")

	// Sweep work list as of Jun 15: the one live, unresolved, activated domain,
	// carrying the account's activation anchor for period derivation.
	cands, err := store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-15T00:00:00Z"))
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, dom.ID, cands[0].ID)
	require.True(t, mustTime(t, "2026-05-04T00:00:00Z").Equal(cands[0].AccountActivatedAt),
		"the sweep candidate carries the account activation anchor")
	require.True(t, cands[0].ChargeAttemptedAt.IsZero(), "no attempt marker before the first charge")

	// A domain not yet activated as of the sweep instant is excluded (zero-grace
	// cutoff is activated_at <= at).
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-05T00:00:00Z"))
	require.NoError(t, err)
	require.Empty(t, cands, "a domain activated after the sweep instant is not yet eligible")

	// Boundary advance count for the account's [Jul 4, Aug 4) period: the live
	// domain activated before Jul 4 owes one full fee.
	newPeriodEnd := mustTime(t, "2026-07-04T00:00:00Z")
	n, err := store.CountLiveDomainsActivatedBefore(ctx, acct, newPeriodEnd)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// DISJOINTNESS INVARIANT (the load-bearing correctness point): charging the
	// domain's activation period resolves it OUT of the sweep work list but keeps
	// it in the boundary count — the sweep owns only the activation period, the
	// boundary owns every subsequent full period, so the boundary must NOT depend
	// on charge_resolved (doing so would gap a period on cron-ordering skew).
	require.NoError(t, store.MarkDomainCharged(ctx, dom.ID, mustTime(t, "2026-06-15T00:00:00Z"), "in_real_stripe", "ii_real_stripe"))
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-15T00:00:00Z"))
	require.NoError(t, err)
	require.Empty(t, cands, "a charged domain drops out of the activation-period work list")
	n, err = store.CountLiveDomainsActivatedBefore(ctx, acct, newPeriodEnd)
	require.NoError(t, err)
	require.Equal(t, 1, n, "a resolved domain still owes every subsequent full period at the boundary")

	// A second mark is a first-write-wins no-op (WHERE charge_resolved = false).
	require.NoError(t, store.MarkDomainCharged(ctx, dom.ID, mustTime(t, "2026-06-16T00:00:00Z"), "in_second", "ii_second"))
	pending, err := store.DomainStillPending(ctx, dom.ID)
	require.NoError(t, err)
	require.False(t, pending, "a resolved domain is no longer pending")

	// Removal is prospective: it drops the domain from BOTH the work list and the
	// boundary count (no retroactive credit for the already-charged period; future
	// boundaries simply stop counting it).
	require.NoError(t, store.RemoveDomain(ctx, app, "one.example.test", mustTime(t, "2026-07-20T00:00:00Z")))
	n, err = store.CountLiveDomainsActivatedBefore(ctx, acct, mustTime(t, "2026-08-04T00:00:00Z"))
	require.NoError(t, err)
	require.Zero(t, n, "a removed domain leaves future boundary counts")

	// Idempotent removal keeps the first removal instant (re-fire affects nothing).
	require.NoError(t, store.RemoveDomain(ctx, app, "one.example.test", mustTime(t, "2026-07-25T00:00:00Z")))

	// A removed historical row does not block RE-activation of the same hostname:
	// the fresh live row gets its own charge identity + its own sweep candidacy.
	reactivated := mustTime(t, "2026-08-01T00:00:00Z")
	require.NoError(t, store.InsertDomain(ctx, acct, app, "one.example.test", reactivated))
	live, found, err := store.DomainByHostname(ctx, "one.example.test")
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, live.Removed, "DomainByHostname returns the live re-activation over the removed history")
	require.NotEqual(t, dom.ID, live.ID, "re-activation is a new charge identity, not a resurrection")
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-08-05T00:00:00Z"))
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, live.ID, cands[0].ID)
}

// Migration 047 — the account-activation gate and the D1d resolved-without-charge
// path against a real Postgres: an unactivated account's domains never enter the
// sweep work list, and MarkDomainChargeResolved (the period-closed forgiveness)
// terminally drops a domain from the work list without moving money.
func TestCustomDomains_Integration_UnactivatedGateAndResolvedForgiveness(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	// Deliberately NOT activated (activated_at stays NULL).
	acct := seedAccount(t, pool)
	app := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, app, acct, uuid.Nil, 0, mustTime(t, "2026-06-01T00:00:00Z"), ""))
	require.NoError(t, store.InsertDomain(ctx, acct, app, "gated.example.test", mustTime(t, "2026-06-10T00:00:00Z")))

	// Unactivated account → excluded from the sweep (the spine's D1d gate), even
	// though the domain is live, unresolved, and past its activation instant.
	cands, err := store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-20T00:00:00Z"))
	require.NoError(t, err)
	require.Empty(t, cands, "an unactivated account's domains never enter the work list")

	// Activate the account → the domain now surfaces as a candidate.
	_, err = pool.Exec(ctx, `UPDATE ms_billing.accounts SET activated_at = $2 WHERE id = $1`,
		acct.String(), mustTime(t, "2026-06-15T00:00:00Z"))
	require.NoError(t, err)
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-20T00:00:00Z"))
	require.NoError(t, err)
	require.Len(t, cands, 1)
	dom := cands[0]
	require.True(t, dom.ChargeAttemptedAt.IsZero())

	// The attempt marker is first-write-wins and surfaces on the next work-list
	// read (drives recovery-before-fresh-charge).
	require.NoError(t, store.MarkDomainChargeAttempted(ctx, dom.ID, mustTime(t, "2026-06-20T00:00:00Z")))
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-21T00:00:00Z"))
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.True(t, mustTime(t, "2026-06-20T00:00:00Z").Equal(cands[0].ChargeAttemptedAt),
		"a set attempt marker is carried to the retried candidate")

	// D1d forgiveness: MarkDomainChargeResolved terminally drops the domain from
	// the work list with no money moved (charge_invoice_id stays NULL).
	require.NoError(t, store.MarkDomainChargeResolved(ctx, dom.ID))
	pending, err := store.DomainStillPending(ctx, dom.ID)
	require.NoError(t, err)
	require.False(t, pending)
	cands, err = store.DomainsPendingCharge(ctx, mustTime(t, "2026-06-21T00:00:00Z"))
	require.NoError(t, err)
	require.Empty(t, cands, "a resolved-forgiven domain never resurfaces")
}
