//go:build integration

package usage_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 086 is the one-shot repoint of the platform samplers' ownerless
// infra events (T183). The test DB has it applied already (nothing to repoint at
// setup), so this seeds the shapes the statement must and must not touch and
// re-executes the migration's own text — the migration file is the single
// source of the SQL; a copy here would be the one that drifts.
func TestMigration086_RepointsExactlyTheOwnerlessInfraRowsOfTheOpenCycle(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	owner, acct, other := uuid.New(), uuid.New(), uuid.New()
	for _, a := range []struct{ id, owner uuid.UUID }{{acct, owner}, {other, uuid.New()}} {
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
			 VALUES ($1, 'user', $2, '2026-08-01T00:00:00Z')`, a.id.String(), a.owner.String())
		require.NoError(t, err)
	}
	app, unfundedOrgApp, unknownApp := uuid.New(), uuid.New(), uuid.New()
	seedMirrorApp(t, pool, acct, app, "2026-08-15T00:00:00Z", "")
	// An org app before its funding designation: roster row, NULL account (041).
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, owner_org_id)
		 VALUES ($1, NULL, 0, 0, '2026-08-15T00:00:00Z', $2)`, unfundedOrgApp.String(), uuid.New().String())
	require.NoError(t, err)

	mod := uuid.New()
	seed := func(id string, account *uuid.UUID, appID uuid.UUID, metric, recordedAt string) {
		t.Helper()
		var acc any
		if account != nil {
			acc = account.String()
		}
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.usage_events
			   (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
			 VALUES ($1,$2,$3,$4,$5,'sum',1,$6)`,
			id, acc, appID.String(), mod.String(), metric, recordedAt)
		require.NoError(t, err)
	}
	// MUST move: the samplers' shapes on a rostered app, this cycle.
	seed("storage-null", nil, app, "infra.storage.gib_hours", "2026-09-13T10:00:00Z")
	seed("cdn-null", nil, app, "infra.egress.cdn.bytes", "2026-09-14T11:00:00Z")
	// MUST NOT move — each is one clause of the WHERE:
	seed("module-metric-null", nil, app, "video.watch.minutes", "2026-09-13T10:00:00Z") // not infra.%
	seed("infra-old", nil, app, "infra.storage.gib_hours", "2026-08-20T10:00:00Z")     // before the open cycle
	seed("infra-unknown-app", nil, unknownApp, "infra.storage.gib_hours", "2026-09-13T10:00:00Z")
	seed("infra-unfunded-org", nil, unfundedOrgApp, "infra.storage.gib_hours", "2026-09-13T10:00:00Z")
	seed("infra-already-attributed", &other, app, "infra.storage.gib_hours", "2026-09-13T12:00:00Z")

	sql, err := os.ReadFile("../../../migrations/billing/086_repoint_ownerless_infra_events.up.sql")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(sql))
	require.NoError(t, err)

	accountOf := func(id string) (string, bool) {
		var got *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT account_id::text FROM ms_billing.usage_events WHERE event_id = $1`, id).Scan(&got))
		if got == nil {
			return "", false
		}
		return *got, true
	}
	for _, id := range []string{"storage-null", "cdn-null"} {
		got, ok := accountOf(id)
		require.True(t, ok, "%s must be attached", id)
		require.Equal(t, acct.String(), got, "%s attaches to the roster's account", id)
	}
	for _, id := range []string{"module-metric-null", "infra-old", "infra-unknown-app", "infra-unfunded-org"} {
		_, ok := accountOf(id)
		require.False(t, ok, "%s must stay NULL-account", id)
	}
	got, ok := accountOf("infra-already-attributed")
	require.True(t, ok)
	require.Equal(t, other.String(), got, "an attributed row is never re-pointed")

	// Idempotent: a second run finds nothing and changes nothing.
	tag, err := pool.Exec(ctx, string(sql))
	require.NoError(t, err)
	_ = tag
	got, _ = accountOf("storage-null")
	require.Equal(t, acct.String(), got)
}
