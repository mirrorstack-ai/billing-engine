package usage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func nan() float64 { return math.NaN() }
func inf() float64 { return math.Inf(1) }

// --- in-memory Store fake -------------------------------------------------

type fakeStore struct {
	defs                   map[string]usage.MetricDefinition   // key: module/metric
	modelPrices            map[string]usage.ModelPrice         // key: metric/model
	versionPrices          map[string]usage.MetricVersionPrice // key: module/metric/version (migration 044)
	accounts               map[uuid.UUID]uuid.UUID             // owner userID → accountID
	appOwnerOrgs           map[uuid.UUID]uuid.UUID             // app roster owner_org_id
	appOwnerLookups        int
	events                 map[string]usage.UsageEvent // event_id → event (idempotency)
	anchorDays             map[uuid.UUID]int           // accountID → billing-period anchor day (0/absent → 1)
	activations            map[uuid.UUID]time.Time
	periodRows             []usage.MetricUsageRaw
	historyRows            []usage.PeriodMetricUsageRaw
	versionRows            []usage.VersionUsageRaw
	appRows                []usage.AppMetricUsageRaw
	appBillRows            []usage.AppMetricUsageRaw
	appInfraBillRows       []usage.AppInfraUsage
	appModuleInfraBillRows []usage.AppModuleInfraUsage
	periodListRows         []usage.BillingPeriodRaw
	periodWindows          map[uuid.UUID]periodWindow // billing_periods id → window
	visibility             map[uuid.UUID]usage.Visibility
	invoiceRows            []usage.InvoiceMirrorRaw             // unordered; ListInvoices applies the SQL contract
	appMirrors             map[uuid.UUID]usage.AppMirrorInfo    // app_id → ms_billing.apps roster row (migration 027)
	baseSnapshots          map[string]usage.AppBaseSnapshotInfo // app_id/period_start → charged-base snapshot (migration 028)

	// ListNewCreationCharges fixtures (本期新建立). The Settled/Pending fakes
	// RE-IMPLEMENT the SQL contract over appMirrors + these parallel maps (as the
	// ListInvoices / MirroredAppIDs fakes re-implement their queries), so service
	// tests exercise realistic join + filter behavior.
	newAppProrationInvoiceID map[uuid.UUID]string   // app_id → armed proration_invoice_id (settled guard)
	newAppProrationSkipped   map[uuid.UUID]bool     // app_id → proration_skipped_at set (permanent skip)
	newAppInvoices           map[string]fakeInvoice // stripe_invoice_id → the mirror row the settled join lands on
	newAppProrationBase      map[uuid.UUID]int64    // app_id → 'proration' base snapshot base_micros (settled breakdown; absent → 0)
	coCreatedOverTimerCounts map[uuid.UUID]int      // app_id → account-FIFO over-count for pending creation preview
	errSettledNewApp         error
	errPendingNewApp         error
	settledDomainCharges     []usage.SettledDomainCreationChargeRaw
	errSettledDomains        error
	gotPendingGraceCutoff    time.Time // captured graceCutoff the service resolved (now − GraceDays)

	// GetAccountBill's single-snapshot unresolved one-time projection fixtures.
	unresolvedOneTimeCharges []usage.UnresolvedOneTimeChargeRaw
	errUnresolvedOneTime     error

	// Pending ADD-ON rows (post-creation in-grace over-module timers): the timer
	// table has no other representation in this fake, so tests set the exact
	// per-app rows the query would return (soonest-first, the SQL's ORDER BY).
	pendingAddonCharges []usage.PendingAddonChargeRaw
	errPendingAddon     error
	gotPendingAddonNow  time.Time // captured `now` the service passed

	// per-module overage display (migration 033): LiveModuleTimerCountForAccount
	// counts the account's live install timers — the sole input to
	// GetAccountBill's steady-state account-overage estimate. The single-account
	// usage fake models it as Σ module_count over its live appMirrors (one live
	// timer per installed module), so display tests set counts on appMirrors.
	errLiveTimerCount     error
	liveOverTimerCounts   map[uuid.UUID]int
	errLiveOverTimerCount error
	liveDomainCount       int
	errLiveDomainCount    error
	activatedRecurring    *usage.RecurringFeeCounts
	errActivatedRecurring error

	// usageAppIDs is what AppIDsWithUsage enumerates (the usage half of
	// GetAccountBill's roster); the mirror half is DERIVED from appMirrors with
	// the real overlap rule (see MirroredAppIDs).
	usageAppIDs []uuid.UUID

	// Per-app row sets for the multi-app GetAccountBill reads. When an app has
	// an entry here it wins; otherwise the flat slices above apply (so the
	// single-app GetAppBill tests keep their simpler setup).
	appBillRowsByApp            map[uuid.UUID][]usage.AppMetricUsageRaw
	appInfraBillRowsByApp       map[uuid.UUID][]usage.AppInfraUsage
	appModuleInfraBillRowsByApp map[uuid.UUID][]usage.AppModuleInfraUsage

	// captured VersionBreakdown call args, so a test can assert the resolved
	// module filter reached the store unchanged.
	gotVersionModuleID uuid.UUID

	// captured AppUsage call args, so a test can assert account_id (payer) and
	// app_id reached the store unchanged.
	gotAppUsageAccountID uuid.UUID
	gotAppUsageAppID     uuid.UUID

	// captured AppBill call args (the full bill read gate).
	gotAppBillAccountID uuid.UUID
	gotAppBillAppID     uuid.UUID

	// captured AppInfraBill call args (the catalog-anchored infra breakdown).
	gotAppInfraBillAccountID uuid.UUID
	gotAppInfraBillAppID     uuid.UUID

	// captured AppModuleInfraBill call args (the per-module dual-price infra breakdown).
	gotAppModuleInfraBillAccountID uuid.UUID
	gotAppModuleInfraBillAppID     uuid.UUID
	appModuleInfraBillCalled       bool

	errLookup             error
	errAccount            error
	errInsert             error
	closedPeriod          bool
	errPeriod             error
	errVisibility         error
	errUpsertDef          error
	errUpsertVersionPrice error
	errUpsertOverride     error
	errHistory            error
	errVersion            error
	errAppUsage           error
	errAppBill            error
	errAppInfraBill       error
	errAppModuleInfraBill error
	errPeriodList         error
	errPeriodWindow       error
	errAnchor             error
	errListInvoices       error

	// captured ListInvoices call args, so a test can assert the clamped
	// page+1 limit and the decoded cursor reached the store unchanged.
	gotInvoiceLimit    int32
	gotInvoiceCursor   *usage.InvoiceCursor
	errAppMirror       error
	errBaseSnapshot    error
	errAppIDsWithUsage error
	errMirroredApps    error

	// captured window a read-path RPC resolved from the account's anchor, so a
	// test can assert the anchored [start, end) reached the store unchanged.
	gotPeriodStart time.Time
	gotPeriodEnd   time.Time
}

// periodWindow is a fake billing_periods window for BillingPeriodWindow lookups.
type periodWindow struct {
	start, end time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		defs: map[string]usage.MetricDefinition{},
		modelPrices: map[string]usage.ModelPrice{
			modelPriceKey("infra.task.gpu.hours", usage.TaskGPUModelG5GXlarge): {UnitPriceMicros: 566900, Active: true},
		},
		versionPrices:               map[string]usage.MetricVersionPrice{},
		accounts:                    map[uuid.UUID]uuid.UUID{},
		appOwnerOrgs:                map[uuid.UUID]uuid.UUID{},
		events:                      map[string]usage.UsageEvent{},
		anchorDays:                  map[uuid.UUID]int{},
		activations:                 map[uuid.UUID]time.Time{},
		periodWindows:               map[uuid.UUID]periodWindow{},
		visibility:                  map[uuid.UUID]usage.Visibility{},
		appMirrors:                  map[uuid.UUID]usage.AppMirrorInfo{},
		liveOverTimerCounts:         map[uuid.UUID]int{},
		baseSnapshots:               map[string]usage.AppBaseSnapshotInfo{},
		appBillRowsByApp:            map[uuid.UUID][]usage.AppMetricUsageRaw{},
		appInfraBillRowsByApp:       map[uuid.UUID][]usage.AppInfraUsage{},
		appModuleInfraBillRowsByApp: map[uuid.UUID][]usage.AppModuleInfraUsage{},
		newAppProrationInvoiceID:    map[uuid.UUID]string{},
		newAppProrationSkipped:      map[uuid.UUID]bool{},
		newAppInvoices:              map[string]fakeInvoice{},
		newAppProrationBase:         map[uuid.UUID]int64{},
		coCreatedOverTimerCounts:    map[uuid.UUID]int{},
	}
}

// fakeInvoice is the minimal ms_billing.invoices mirror row the SettledNewCreationCharges
// join lands on: what the settled read projects (id/number/amount/created_at/status).
type fakeInvoice struct {
	id           uuid.UUID
	number       string
	status       string
	amountMicros int64
	createdAt    time.Time
}

// AppIDsWithUsage returns the configured usage-half roster (the fake does not
// re-derive it from events — enumeration SQL is integration-tested).
func (f *fakeStore) AppIDsWithUsage(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]uuid.UUID, error) {
	if f.errAppIDsWithUsage != nil {
		return nil, f.errAppIDsWithUsage
	}
	return f.usageAppIDs, nil
}

// MirroredAppIDs derives the mirror-half roster from appMirrors with the SAME
// [created_at, deleted_at) ∩ [start, end) overlap rule as the real query —
// and, being map iteration, returns it in RANDOM order, so tests prove the
// service's deterministic sort rather than inheriting the fake's.
func (f *fakeStore) MirroredAppIDs(_ context.Context, _ uuid.UUID, start, end time.Time) ([]uuid.UUID, error) {
	if f.errMirroredApps != nil {
		return nil, f.errMirroredApps
	}
	out := make([]uuid.UUID, 0, len(f.appMirrors))
	for id, m := range f.appMirrors {
		if !m.CreatedAt.Before(end) {
			continue // created on/after the window end → no overlap
		}
		if m.Deleted && !m.DeletedAt.After(start) {
			continue // deleted before the window opened → no overlap
		}
		out = append(out, id)
	}
	return out, nil
}

// SettledNewCreationCharges re-implements the SettledNewCreationCharges SQL contract in
// Go over appMirrors + the proration/invoice fixtures: apps whose settlement is
// in [start, end) and whose proration guard is armed, joined to their invoice,
// excluding voided / $0 invoices, ordered by the invoice created_at DESC (app_id
// tie-break) — so service tests prove the assembly, not the fake's map order.
func (f *fakeStore) SettledNewCreationCharges(_ context.Context, _ uuid.UUID, start, end time.Time) ([]usage.SettledNewCreationChargeRaw, error) {
	if f.errSettledNewApp != nil {
		return nil, f.errSettledNewApp
	}
	out := make([]usage.SettledNewCreationChargeRaw, 0)
	for id, m := range f.appMirrors {
		sid := f.newAppProrationInvoiceID[id]
		if sid == "" {
			continue // guard NULL → not settled (skipped / no-charge / still pending)
		}
		inv, ok := f.newAppInvoices[sid]
		if !ok {
			continue // no mirror row → the join finds nothing
		}
		if inv.status == "void" || inv.amountMicros <= 0 {
			continue // SQL drops voided / $0 invoices
		}
		if inv.createdAt.Before(start) || !inv.createdAt.Before(end) {
			continue // settlement instant ∉ [start, end)
		}
		out = append(out, usage.SettledNewCreationChargeRaw{
			AppID:           id,
			InvoiceID:       inv.id,
			Number:          inv.number,
			AmountDueMicros: inv.amountMicros,
			RecordedAt:      inv.createdAt,
			// The fake models created_module_count with the roster row's ModuleCount
			// and the 'proration' base snapshot with newAppProrationBase (absent → 0,
			// the LEFT-JOIN-miss contract).
			Name:               m.Name,
			CreatedModuleCount: m.ModuleCount,
			BaseMicros:         f.newAppProrationBase[id],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].RecordedAt.Equal(out[j].RecordedAt) {
			return out[i].RecordedAt.After(out[j].RecordedAt)
		}
		return out[i].AppID.String() < out[j].AppID.String()
	})
	return out, nil
}

func (f *fakeStore) SettledDomainCreationCharges(_ context.Context, _ uuid.UUID, start, end time.Time) ([]usage.SettledDomainCreationChargeRaw, error) {
	if f.errSettledDomains != nil {
		return nil, f.errSettledDomains
	}
	out := make([]usage.SettledDomainCreationChargeRaw, 0, len(f.settledDomainCharges))
	for _, charge := range f.settledDomainCharges {
		if !charge.ChargedAt.Before(start) && charge.ChargedAt.Before(end) {
			out = append(out, charge)
		}
	}
	return out, nil
}

// PendingNewCreationCharges re-implements the PendingNewCreationCharges SQL contract: apps
// created in [start, end) that are uncharged (guard NULL), not skipped, live,
// and still in grace (created_at > graceCutoff), ordered by created_at. It
// captures graceCutoff so a test can assert the service passed now − GraceDays.
func (f *fakeStore) PendingNewCreationCharges(_ context.Context, _ uuid.UUID, start, end, graceCutoff time.Time) ([]usage.PendingNewCreationChargeRaw, error) {
	f.gotPendingGraceCutoff = graceCutoff
	if f.errPendingNewApp != nil {
		return nil, f.errPendingNewApp
	}
	out := make([]usage.PendingNewCreationChargeRaw, 0)
	for id, m := range f.appMirrors {
		if m.CreatedAt.Before(start) || !m.CreatedAt.Before(end) {
			continue // created_at ∉ [start, end)
		}
		if f.newAppProrationInvoiceID[id] != "" {
			continue // guard armed → settled, not pending
		}
		if f.newAppProrationSkipped[id] {
			continue // permanently skipped
		}
		if m.Deleted {
			continue // soft-deleted → excluded
		}
		if !m.CreatedAt.After(graceCutoff) {
			continue // grace already elapsed → not pending
		}
		out = append(out, usage.PendingNewCreationChargeRaw{
			AppID:              id,
			CreatedAt:          m.CreatedAt,
			Name:               m.Name,
			CreatedModuleCount: m.ModuleCount,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// CoCreatedOverModuleTimerCount returns the configured account-FIFO over-count
// for an app's co-created module timers; absent fixtures default to zero.
func (f *fakeStore) CoCreatedOverModuleTimerCount(_ context.Context, _, appID uuid.UUID, _ time.Time, _ int) (int, error) {
	return f.coCreatedOverTimerCounts[appID], nil
}

// PendingAddonModuleCharges returns the fixture rows verbatim (the timer table
// has no other fake representation); captures `now` so a test can assert the
// service passed its own nowFn instant.
func (f *fakeStore) PendingAddonModuleCharges(_ context.Context, _ uuid.UUID, _ int, now time.Time) ([]usage.PendingAddonChargeRaw, error) {
	f.gotPendingAddonNow = now
	if f.errPendingAddon != nil {
		return nil, f.errPendingAddon
	}
	return f.pendingAddonCharges, nil
}

// UnresolvedOneTimeCharges returns the configured single-snapshot projection
// rows. SQL eligibility and ownership predicates are covered in Postgres.
func (f *fakeStore) UnresolvedOneTimeCharges(_ context.Context, _ uuid.UUID, _, _ int) ([]usage.UnresolvedOneTimeChargeRaw, error) {
	if f.errUnresolvedOneTime != nil {
		return nil, f.errUnresolvedOneTime
	}
	return f.unresolvedOneTimeCharges, nil
}

// baseSnapKey mirrors the app_base_snapshots PRIMARY KEY (app_id, period_start).
func baseSnapKey(appID uuid.UUID, periodStart time.Time) string {
	return appID.String() + "/" + periodStart.UTC().Format(time.RFC3339Nano)
}

// AppMirror returns the fake ms_billing.apps roster row for an app; absent →
// not mirrored (pre-backfill), so GetAppBill keeps the usage-proxy fallback.
func (f *fakeStore) AppMirror(_ context.Context, appID uuid.UUID) (usage.AppMirrorInfo, bool, error) {
	if f.errAppMirror != nil {
		return usage.AppMirrorInfo{}, false, f.errAppMirror
	}
	m, ok := f.appMirrors[appID]
	return m, ok, nil
}

// AppBaseSnapshot returns the fake migration-028 charged-base row for one
// (app, period_start); absent → the period was never base-charged, so
// GetAppBill falls back to the live-count display estimate.
func (f *fakeStore) AppBaseSnapshot(_ context.Context, appID uuid.UUID, periodStart time.Time) (usage.AppBaseSnapshotInfo, bool, error) {
	if f.errBaseSnapshot != nil {
		return usage.AppBaseSnapshotInfo{}, false, f.errBaseSnapshot
	}
	s, ok := f.baseSnapshots[baseSnapKey(appID, periodStart)]
	return s, ok, nil
}

// LiveModuleTimerCountForAccount returns the account's live install-timer count
// — the sole input to GetAccountBill's steady-state account-overage estimate
// (migration 033). The single-account usage fake models one live timer per
// installed module, so it sums module_count over the non-deleted appMirrors
// (numerically identical to counting live timer rows).
func (f *fakeStore) LiveModuleTimerCountForAccount(_ context.Context, _ uuid.UUID) (int, error) {
	if f.errLiveTimerCount != nil {
		return 0, f.errLiveTimerCount
	}
	sum := 0
	for _, m := range f.appMirrors {
		if !m.Deleted {
			sum += m.ModuleCount
		}
	}
	return sum, nil
}

// LiveOverModuleTimerCountForApp returns the fixture's exact attribution for
// one app. Tests opt in explicitly so the fake never invents account FIFO order
// from its unordered appMirrors map.
func (f *fakeStore) LiveOverModuleTimerCountForApp(_ context.Context, _, appID uuid.UUID, _ int) (int, error) {
	if f.errLiveOverTimerCount != nil {
		return 0, f.errLiveOverTimerCount
	}
	return f.liveOverTimerCounts[appID], nil
}

// LiveDomainCountForAccount returns the configured current live-domain count
// used by GetAccountBill's steady-state custom-domain estimate.
func (f *fakeStore) LiveDomainCountForAccount(_ context.Context, _ uuid.UUID) (int, error) {
	if f.errLiveDomainCount != nil {
		return 0, f.errLiveDomainCount
	}
	return f.liveDomainCount, nil
}

// ActivatedRecurringFeeShares re-implements the SQL contract's SHAPE over the
// fake's mirrors: one row per live app, carrying the activation flag and the
// surcharge units it owns. Tests that pin exact account totals set
// activatedRecurring; the counts are then spread over the live apps so that
// usage.RecurringFeeCountsOf(shares) reproduces them exactly — the same
// sum-of-rows identity the real query guarantees.
func (f *fakeStore) ActivatedRecurringFeeShares(_ context.Context, _ uuid.UUID, _ int, _ time.Time) ([]usage.AppRecurringFeeShare, error) {
	if f.errActivatedRecurring != nil {
		return nil, f.errActivatedRecurring
	}

	liveIDs := make([]uuid.UUID, 0, len(f.appMirrors))
	for appID, app := range f.appMirrors {
		if !app.Deleted {
			liveIDs = append(liveIDs, appID)
		}
	}
	// The map is unordered; the real query is ORDER BY app_id. Sort so an
	// allocation tie-break lands on the same app every run.
	sort.Slice(liveIDs, func(i, j int) bool {
		return bytes.Compare(liveIDs[i][:], liveIDs[j][:]) < 0
	})

	counts := usage.RecurringFeeCounts{
		Apps:           len(liveIDs),
		ModuleOverages: max(0, f.liveModuleTimerCount()-usage.IncludedModules),
		CustomDomains:  f.liveDomainCount,
	}
	if f.activatedRecurring != nil {
		counts = *f.activatedRecurring
	}

	shares := make([]usage.AppRecurringFeeShare, 0, len(liveIDs))
	for i, appID := range liveIDs {
		share := usage.AppRecurringFeeShare{AppID: appID, Activated: i < counts.Apps}
		// Surcharges concentrate on the first live app — the account totals are
		// what these tests assert, and the allocator's own spreading behaviour is
		// covered directly in projected_base_alloc_test.go.
		if i == 0 {
			share.OverModuleCount = counts.ModuleOverages
			share.CustomDomainCount = counts.CustomDomains
		}
		shares = append(shares, share)
	}
	if len(shares) == 0 && (counts.ModuleOverages > 0 || counts.CustomDomains > 0) {
		// Surcharges with no app to own them cannot happen against the real
		// schema (both tables carry a NOT NULL app_id FK), and letting the fake
		// invent an off-roster owner would drop the money from the per-app
		// decomposition while leaving it in the account total.
		return nil, fmt.Errorf("fake: %d overage + %d domain units with no live app to own them",
			counts.ModuleOverages, counts.CustomDomains)
	}
	return shares, nil
}

func (f *fakeStore) liveModuleTimerCount() int {
	total := 0
	for _, app := range f.appMirrors {
		if !app.Deleted {
			total += app.ModuleCount
		}
	}
	return total
}

// AccountAnchorDay returns the configured anchor day for an account, defaulting
// to 1 (the UTC calendar month) so tests that don't set one keep the pre-anchor
// window behavior.
func (f *fakeStore) AccountAnchorDay(_ context.Context, accountID uuid.UUID) (int, error) {
	if f.errAnchor != nil {
		return 0, f.errAnchor
	}
	if d, ok := f.anchorDays[accountID]; ok && d != 0 {
		return d, nil
	}
	return 1, nil
}

func (f *fakeStore) AccountActivation(_ context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	at, ok := f.activations[accountID]
	return at, ok, nil
}

func defKey(moduleID uuid.UUID, metric string) string { return moduleID.String() + "/" + metric }

func modelPriceKey(metric, model string) string { return metric + "/" + model }

func (f *fakeStore) LookupMetricDefinition(_ context.Context, moduleID uuid.UUID, metric string) (usage.MetricDefinition, bool, error) {
	if f.errLookup != nil {
		return usage.MetricDefinition{}, false, f.errLookup
	}
	d, ok := f.defs[defKey(moduleID, metric)]
	return d, ok, nil
}

func (f *fakeStore) LookupModelPrice(_ context.Context, metric, model string) (usage.ModelPrice, bool, error) {
	price, ok := f.modelPrices[modelPriceKey(metric, model)]
	return price, ok, nil
}

func (f *fakeStore) UpsertMetricDefinitions(_ context.Context, defs []usage.MetricDeclaration) error {
	if f.errUpsertDef != nil {
		return f.errUpsertDef // all-or-nothing: nothing is written on error
	}
	for _, def := range defs {
		f.defs[defKey(def.ModuleID, def.Metric)] = usage.MetricDefinition{
			Kind:            def.Kind,
			AggregationKey:  def.AggregationKey,
			Unit:            def.Unit,
			UnitPriceMicros: def.UnitPriceMicros,
			Priced:          def.Priced,
			Active:          def.Active,
		}
	}
	return nil
}

// versionPriceKey mirrors the metric_version_prices PRIMARY KEY
// (module_id, metric, module_version).
func versionPriceKey(moduleID uuid.UUID, metric, version string) string {
	return moduleID.String() + "/" + metric + "/" + version
}

func (f *fakeStore) UpsertMetricVersionPrices(_ context.Context, prices []usage.MetricVersionPrice) error {
	if f.errUpsertVersionPrice != nil {
		return f.errUpsertVersionPrice // all-or-nothing: nothing is written on error
	}
	for _, p := range prices {
		key := versionPriceKey(p.ModuleID, p.Metric, p.ModuleVersion)
		if _, exists := f.versionPrices[key]; exists {
			continue // ON CONFLICT DO NOTHING: immutable once written
		}
		f.versionPrices[key] = p
	}
	return nil
}

// SyncInfraPriceOverrides mirrors the real store's three steps in order:
// absorb-all expands from the SENTINEL rows (the catalog is the set — no list of
// metric names lives in the fake either), explicit overrides upsert on top and
// win, and everything the manifest no longer declares is deleted. A missing
// sentinel row errors (the real store's INSERT ... SELECT affects 0 rows), never
// a silent write. The written rows key the REAL moduleID, so a test asserts
// f.defs[defKey(module, metric)].
func (f *fakeStore) SyncInfraPriceOverrides(_ context.Context, moduleID uuid.UUID, absorbAll bool, overrides []usage.InfraPriceOverride) error {
	if f.errUpsertOverride != nil {
		return f.errUpsertOverride // all-or-nothing: nothing is written on error
	}
	sentinel := usage.PlatformInfraModuleID()
	// Validate the whole batch first so a mid-batch miss writes nothing.
	for _, o := range overrides {
		if _, ok := f.defs[defKey(sentinel, o.Metric)]; !ok {
			return errors.New("no sentinel catalog row for infra metric " + o.Metric)
		}
	}

	keep := map[string]bool{}
	write := func(metric string, price int64) {
		base := f.defs[defKey(sentinel, metric)]
		f.defs[defKey(moduleID, metric)] = usage.MetricDefinition{
			Kind:            base.Kind, // inherited from the sentinel row
			Unit:            base.Unit, // inherited from the sentinel row
			UnitPriceMicros: price,
			Priced:          true,
			Active:          true,
		}
		keep[metric] = true
	}

	if absorbAll {
		for key, def := range f.defs {
			mod, metric, ok := splitDefKey(key)
			if !ok || mod != sentinel.String() || !def.Active {
				continue
			}
			write(metric, 0)
		}
	}
	for _, o := range overrides {
		write(o.Metric, o.UnitPriceMicros) // after the absorb, so it wins
	}

	// Withdraw: reserved rows under THIS module that the manifest dropped. A
	// module's own custom metric rows are SetMetricDefinitions' business and must
	// survive, so the sweep is scoped to the reserved namespaces.
	for key := range f.defs {
		mod, metric, ok := splitDefKey(key)
		if !ok || mod != moduleID.String() || keep[metric] {
			continue
		}
		if strings.HasPrefix(metric, "infra.") || strings.HasPrefix(metric, "platform.") {
			delete(f.defs, key)
		}
	}
	return nil
}

// splitDefKey inverts defKey (module/metric). A metric name may itself contain
// "/" in principle, so split on the FIRST separator only.
func splitDefKey(key string) (module, metric string, ok bool) {
	i := strings.Index(key, "/")
	if i < 0 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

func (f *fakeStore) InsertUsageEvent(_ context.Context, ev usage.UsageEvent) (bool, error) {
	if f.errInsert != nil {
		return false, f.errInsert
	}
	if existing, exists := f.events[ev.EventID]; exists {
		if !bytes.Equal(existing.PayloadFingerprint, ev.PayloadFingerprint) {
			return false, usage.ErrUsageEventConflict
		}
		return false, nil
	}
	f.events[ev.EventID] = ev
	return true, nil
}

func (f *fakeStore) CheckUsageEventID(_ context.Context, eventID string, fingerprint []byte) (bool, error) {
	existing, exists := f.events[eventID]
	if !exists {
		return false, nil
	}
	if !bytes.Equal(existing.PayloadFingerprint, fingerprint) {
		return false, usage.ErrUsageEventConflict
	}
	return true, nil
}

func (f *fakeStore) InsertUsageObservation(ctx context.Context, ev usage.UsageEvent, start, end time.Time, rejection usage.UsageRejectionReason) (bool, float64, error) {
	if existing, ok := f.events[ev.EventID]; ok {
		if !bytes.Equal(existing.PayloadFingerprint, ev.PayloadFingerprint) {
			return false, 0, usage.ErrUsageEventConflict
		}
		return false, 0, nil
	}
	if rejection == usage.UsageRejectionOccurredFuture {
		return false, 0, usage.ErrUsageOccurredFuture
	}
	if rejection == usage.UsageRejectionOccurredTooOld {
		return false, 0, usage.ErrUsageOccurredTooOld
	}
	if rejection == usage.UsageRejectionPeriodClosed {
		return false, 0, usage.ErrUsagePeriodClosed
	}
	if f.closedPeriod {
		return false, 0, usage.ErrUsagePeriodClosed
	}
	previous := 0.0
	if ev.AggregationKey == usage.AggregationKeySubject {
		for _, prior := range f.events {
			if prior.AccountID == ev.AccountID && prior.AppID == ev.AppID &&
				prior.ModuleID == ev.ModuleID && prior.Metric == ev.Metric &&
				prior.Model == ev.Model && prior.ModuleVersion == ev.ModuleVersion &&
				prior.Subject == ev.Subject && !prior.OccurredAt.Before(start) && prior.OccurredAt.Before(end) &&
				prior.Value > previous {
				previous = prior.Value
			}
		}
	}
	recorded, err := f.InsertUsageEvent(ctx, ev)
	if err != nil || !recorded {
		return recorded, 0, err
	}
	if ev.AggregationKey == usage.AggregationKeySubject && previous >= ev.Value {
		return true, 0, nil
	}
	return true, ev.Value - previous, nil
}

func (f *fakeStore) AccountByOwner(_ context.Context, owner usage.Owner) (uuid.UUID, bool, error) {
	if f.errAccount != nil {
		return uuid.Nil, false, f.errAccount
	}
	if owner.OrgID != uuid.Nil {
		return uuid.Nil, false, nil // org path not yet provisioned
	}
	id, ok := f.accounts[owner.UserID]
	return id, ok, nil
}

func (f *fakeStore) AppOwnerOrg(_ context.Context, appID uuid.UUID) (uuid.UUID, bool, error) {
	f.appOwnerLookups++
	orgID, found := f.appOwnerOrgs[appID]
	return orgID, found, nil
}

func (f *fakeStore) CurrentPeriodUsage(_ context.Context, _ uuid.UUID, start, end time.Time) ([]usage.MetricUsageRaw, error) {
	f.gotPeriodStart, f.gotPeriodEnd = start, end
	if f.errPeriod != nil {
		return nil, f.errPeriod
	}
	return f.periodRows, nil
}

func (f *fakeStore) UpsertModuleVisibility(_ context.Context, moduleID uuid.UUID, vis usage.Visibility) error {
	if f.errVisibility != nil {
		return f.errVisibility
	}
	f.visibility[moduleID] = vis
	return nil
}

func (f *fakeStore) UsageHistory(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.PeriodMetricUsageRaw, error) {
	if f.errHistory != nil {
		return nil, f.errHistory
	}
	return f.historyRows, nil
}

func (f *fakeStore) VersionBreakdown(_ context.Context, _ uuid.UUID, _ time.Time, moduleID uuid.UUID) ([]usage.VersionUsageRaw, error) {
	f.gotVersionModuleID = moduleID
	if f.errVersion != nil {
		return nil, f.errVersion
	}
	return f.versionRows, nil
}

func (f *fakeStore) AppUsage(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	f.gotAppUsageAccountID = accountID
	f.gotAppUsageAppID = appID
	if f.errAppUsage != nil {
		return nil, f.errAppUsage
	}
	return f.appRows, nil
}

func (f *fakeStore) AppBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	f.gotAppBillAccountID = accountID
	f.gotAppBillAppID = appID
	if f.errAppBill != nil {
		return nil, f.errAppBill
	}
	if rows, ok := f.appBillRowsByApp[appID]; ok {
		return rows, nil
	}
	return f.appBillRows, nil
}

func (f *fakeStore) AppInfraBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppInfraUsage, error) {
	f.gotAppInfraBillAccountID = accountID
	f.gotAppInfraBillAppID = appID
	if f.errAppInfraBill != nil {
		return nil, f.errAppInfraBill
	}
	if rows, ok := f.appInfraBillRowsByApp[appID]; ok {
		return rows, nil
	}
	return f.appInfraBillRows, nil
}

func (f *fakeStore) AppModuleInfraBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppModuleInfraUsage, error) {
	f.appModuleInfraBillCalled = true
	f.gotAppModuleInfraBillAccountID = accountID
	f.gotAppModuleInfraBillAppID = appID
	if f.errAppModuleInfraBill != nil {
		return nil, f.errAppModuleInfraBill
	}
	if rows, ok := f.appModuleInfraBillRowsByApp[appID]; ok {
		return rows, nil
	}
	return f.appModuleInfraBillRows, nil
}

func (f *fakeStore) ListBillingPeriods(_ context.Context, _ uuid.UUID, _ time.Time) ([]usage.BillingPeriodRaw, error) {
	if f.errPeriodList != nil {
		return nil, f.errPeriodList
	}
	return f.periodListRows, nil
}

func (f *fakeStore) BillingPeriodWindow(_ context.Context, _, periodID uuid.UUID) (time.Time, time.Time, bool, error) {
	if f.errPeriodWindow != nil {
		return time.Time{}, time.Time{}, false, f.errPeriodWindow
	}
	w, ok := f.periodWindows[periodID]
	return w.start, w.end, ok, nil
}

// ListInvoices re-implements the ListInvoicesForAccount SQL contract in
// memory — drop drafts, keyset-filter strictly past the cursor, order
// (created_at, id) DESC, LIMIT — so service tests can walk real multi-page
// flows. The authoritative SQL is exercised by the integration tests.
func (f *fakeStore) ListInvoices(_ context.Context, _ uuid.UUID, limit int32, cursor *usage.InvoiceCursor) ([]usage.InvoiceMirrorRaw, error) {
	if f.errListInvoices != nil {
		return nil, f.errListInvoices
	}
	f.gotInvoiceLimit = limit
	f.gotInvoiceCursor = cursor

	rows := make([]usage.InvoiceMirrorRaw, 0, len(f.invoiceRows))
	for _, r := range f.invoiceRows {
		if r.Status == "draft" {
			continue
		}
		if cursor != nil {
			// Keep only rows strictly BEFORE the cursor tuple in DESC order,
			// i.e. (created_at, id) < (cursor.CreatedAt, cursor.ID). Postgres
			// compares uuids bytewise, matched here via bytes.Compare.
			if r.CreatedAt.After(cursor.CreatedAt) {
				continue
			}
			if r.CreatedAt.Equal(cursor.CreatedAt) && bytes.Compare(r.ID[:], cursor.ID[:]) >= 0 {
				continue
			}
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return bytes.Compare(rows[i].ID[:], rows[j].ID[:]) > 0
	})
	if int32(len(rows)) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// --- helpers --------------------------------------------------------------

func newService(store usage.Store) *usage.Service { return usage.NewService(store) }

func validRecord() usage.RecordUsageRequest {
	return usage.RecordUsageRequest{
		EventID:     "evt-1",
		AppID:       uuid.New(),
		ModuleID:    uuid.New(),
		OwnerUserID: uuid.New(),
		Metric:      "orders.placed",
		Value:       3,
		RecordedAt:  time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
}

func requireCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "want *billing.Error, got %T", err)
	require.Equal(t, want, be.Code)
}

// --- RecordUsage ----------------------------------------------------------

// declare registers a metric in the fake catalog so RecordUsage accepts it
// (declaration-first: an undeclared metric is rejected).
func declare(store *fakeStore, req usage.RecordUsageRequest, kind usage.Kind) {
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: kind, Active: true,
	}
}

func TestRecordUsage_FreshInsert(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	store.accounts[req.OwnerUserID] = uuid.New()

	resp, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	require.Len(t, store.events, 1)
}

func TestRecordUsage_IdempotentRetry(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	svc := newService(store)

	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)

	// Same event_id → deduped, still success.
	second, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)
	require.Len(t, store.events, 1)
}

func v2Record(now time.Time) usage.RecordUsageRequest {
	req := validRecord()
	req.Version = 2
	req.RecordedAt = now
	req.OccurredAt = now.Add(-time.Hour)
	return req
}

func TestRecordUsage_V2PersistsCanonicalObservation(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	req.Subject = "user_123"
	req.Metadata = json.RawMessage(`{"provider":"google","attempt":1.0}`)
	declare(store, req, usage.KindCount)

	resp, err := usage.NewService(store).WithNow(func() time.Time { return now }).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	event := store.events[req.EventID]
	require.Equal(t, 2, event.ObservationVersion)
	require.Equal(t, req.OccurredAt, event.OccurredAt)
	require.Equal(t, "user_123", event.Subject)
	require.JSONEq(t, `{"attempt":1,"provider":"google"}`, string(event.Metadata))
	require.Len(t, event.PayloadFingerprint, 32)
}

func TestRecordUsage_V2CanonicalRetryIgnoresMetadataKeyOrderAndNumberSpelling(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	req.Metadata = json.RawMessage(`{"b":2e0,"a":1}`)
	declare(store, req, usage.KindCount)
	svc := usage.NewService(store).WithNow(func() time.Time { return now })

	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)
	req.Metadata = json.RawMessage(" { \"a\" : 1.0, \"b\" : 2 }")
	second, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)
}

func TestRecordUsage_EventIDCollisionReturnsConflict(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	req.Subject = "user-a"
	declare(store, req, usage.KindCount)
	svc := usage.NewService(store).WithNow(func() time.Time { return now })
	require.NoError(t, func() error { _, err := svc.RecordUsage(context.Background(), req); return err }())

	req.Subject = "user-b"
	_, err := svc.RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeConflict)
	require.Equal(t, "user-a", store.events[req.EventID].Subject)
}

func TestRecordUsage_V2ContractValidation(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	base := v2Record(now)
	cases := map[string]func(*usage.RecordUsageRequest){
		"unknown version":        func(r *usage.RecordUsageRequest) { r.Version = 3 },
		"missing occurrence":     func(r *usage.RecordUsageRequest) { r.OccurredAt = time.Time{} },
		"legacy subject":         func(r *usage.RecordUsageRequest) { r.Version, r.Subject = 1, "user" },
		"legacy metadata":        func(r *usage.RecordUsageRequest) { r.Version, r.Metadata = 1, json.RawMessage(`{}`) },
		"oversize subject":       func(r *usage.RecordUsageRequest) { r.Subject = strings.Repeat("x", 257) },
		"control subject":        func(r *usage.RecordUsageRequest) { r.Subject = "user\n1" },
		"invalid utf8 subject":   func(r *usage.RecordUsageRequest) { r.Subject = string([]byte{0xff}) },
		"metadata not object":    func(r *usage.RecordUsageRequest) { r.Metadata = json.RawMessage(`[]`) },
		"metadata bad key":       func(r *usage.RecordUsageRequest) { r.Metadata = json.RawMessage(`{"bad key":1}`) },
		"metadata too deep":      func(r *usage.RecordUsageRequest) { r.Metadata = json.RawMessage(`{"a":{"b":{"c":{"d":1}}}}`) },
		"metadata trailing json": func(r *usage.RecordUsageRequest) { r.Metadata = json.RawMessage(`{} {}`) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			req := base
			mutate(&req)
			declare(store, req, usage.KindCount)
			_, err := usage.NewService(store).WithNow(func() time.Time { return now }).RecordUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
			require.Empty(t, store.events)
		})
	}
}

func TestRecordUsage_KeyedPeakRequiresV2Subject(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindPeak, AggregationKey: usage.AggregationKeySubject, Active: true,
	}

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	req.Version = 2
	req.OccurredAt = req.RecordedAt
	_, err = newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestRecordUsage_V2OccurrencePolicyBoundariesAndClosedPeriod(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		occurredAt time.Time
		closed     bool
		wantCode   billing.Code
		wantPolicy usage.OccurrencePolicy
	}{
		{name: "future tolerance inclusive", occurredAt: now.Add(5 * time.Minute), wantPolicy: usage.OccurrencePolicyOnTime},
		{name: "future rejected", occurredAt: now.Add(5*time.Minute + time.Nanosecond), wantCode: billing.CodeInvalidInput},
		{name: "past window inclusive", occurredAt: now.Add(-35 * 24 * time.Hour), wantPolicy: usage.OccurrencePolicyLateOpen},
		{name: "past rejected", occurredAt: now.Add(-35*24*time.Hour - time.Nanosecond), wantCode: billing.CodeInvalidInput},
		{name: "closing rejected", occurredAt: now.Add(-time.Hour), closed: true, wantCode: billing.CodeConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			store.closedPeriod = tc.closed
			req := v2Record(now)
			req.OccurredAt = tc.occurredAt
			declare(store, req, usage.KindCount)
			resp, err := usage.NewService(store).WithNow(func() time.Time { return now }).RecordUsage(context.Background(), req)
			if tc.wantCode != "" {
				requireCode(t, err, tc.wantCode)
				require.Empty(t, store.events)
				return
			}
			require.NoError(t, err)
			require.True(t, resp.Recorded)
			require.Equal(t, tc.wantPolicy, store.events[req.EventID].OccurrencePolicy)
		})
	}
}

func TestRecordUsage_PreActivationOccurrenceClampsToFirstFundedPeriod(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	req.OccurredAt = time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	declare(store, req, usage.KindCount)
	accountID := uuid.New()
	store.accounts[req.OwnerUserID] = accountID
	store.anchorDays[accountID] = 15
	store.activations[accountID] = time.Date(2026, 8, 15, 14, 0, 0, 0, time.UTC)

	resp, err := usage.NewService(store).WithNow(func() time.Time { return now }).
		RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	event := store.events[req.EventID]
	require.Equal(t, req.OccurredAt, event.OccurredAt, "occurrence remains audit evidence")
	require.Equal(t, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC), event.BillableAt)
	require.Equal(t, usage.OccurrencePolicyFirstFunded, event.OccurrencePolicy)
}

func TestRecordUsage_LogicallyClosedMissingPeriodIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	req.OccurredAt = time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC) // within 35d, but two windows old
	declare(store, req, usage.KindCount)
	accountID := uuid.New()
	store.accounts[req.OwnerUserID] = accountID
	store.anchorDays[accountID] = 1

	_, err := usage.NewService(store).WithNow(func() time.Time { return now }).
		RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeConflict)
	require.Empty(t, store.events)
}

func TestRecordUsage_IdenticalRetryRemainsIdempotentAfterPeriodCloses(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	req := v2Record(now)
	declare(store, req, usage.KindCount)
	svc := usage.NewService(store).WithNow(func() time.Time { return now })
	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)
	store.closedPeriod = true
	second, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)
}

func TestRecordUsage_SnapshotsDeclaredKindFromCatalog(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	req.Metric = "myapp.objects.bytes"
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindTimeWeighted, Active: true, UnitPriceMicros: 5, Priced: true,
	}

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, usage.KindTimeWeighted, store.events[req.EventID].Kind)
}

func TestRecordUsage_RejectsUndeclaredMetric(t *testing.T) {
	// Declaration-first (design §1): a metric with no catalog row is
	// REJECTED, not recorded with a fallback kind.
	store := newFakeStore()
	req := validRecord() // no catalog row for this metric

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "undeclared metric must not be recorded")
}

func TestRecordUsage_RejectsRetiredMetric(t *testing.T) {
	// active=false means the metric is retired and no longer accepts events
	// (migration 006). RecordUsage rejects it rather than recording a fact
	// against a retired declaration.
	store := newFakeStore()
	req := validRecord()
	req.Metric = "myapp.objects.bytes"
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindTimeWeighted, Active: false, UnitPriceMicros: 5, Priced: true,
	}

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "retired metric must not be recorded")
}

func TestRecordUsage_AcceptedRetryRemainsIdempotentAfterMetricRetires(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	svc := newService(store)
	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)

	def := store.defs[defKey(req.ModuleID, req.Metric)]
	def.Active = false
	store.defs[defKey(req.ModuleID, req.Metric)] = def
	retry, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, retry.Recorded)

	changed := req
	changed.Value++
	_, err = svc.RecordUsage(context.Background(), changed)
	requireCode(t, err, billing.CodeConflict)
}

func TestRecordUsage_LazyAccountWhenNoBillingAccount(t *testing.T) {
	store := newFakeStore()
	req := validRecord() // owner has no account row
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.events[req.EventID].AccountID, "lazy event records NULL account")
}

func TestRecordUsage_UnresolvedOrgWithRegisteredAppRetainsNullAccount(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	req.OwnerUserID = uuid.Nil
	req.OwnerOrgID = uuid.New()
	store.appOwnerOrgs[req.AppID] = req.OwnerOrgID
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.events[req.EventID].AccountID)
	require.Equal(t, 1, store.appOwnerLookups)
}

func TestRecordUsage_UnresolvedOrgWithoutRegisteredAppRejectsBeforeInsert(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	req.OwnerUserID = uuid.Nil
	req.OwnerOrgID = uuid.New()
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "unattributable usage must never reach InsertUsageEvent")
}

func TestRecordUsage_UnresolvedUserKeepsLazyNullWithoutAppLookup(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.events[req.EventID].AccountID)
	require.Zero(t, store.appOwnerLookups)
}

func TestRecordUsage_RejectsReservedPrefixes(t *testing.T) {
	for _, metric := range []string{"platform.tokens", "infra.egress.bytes", "infra.compute.ms"} {
		store := newFakeStore()
		req := validRecord()
		req.Metric = metric
		_, err := newService(store).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.events, "reserved metric must not be recorded: %s", metric)
	}
}

func TestRecordUsage_RejectsNegativeAndNonFinite(t *testing.T) {
	for _, v := range []float64{-1, -0.0001} {
		req := validRecord()
		req.Value = v
		_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
	}
	// NaN / +Inf
	for _, v := range []float64{nan(), inf()} {
		req := validRecord()
		req.Value = v
		_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

func TestRecordUsage_ValidatesRequiredFields(t *testing.T) {
	base := validRecord()
	cases := map[string]func(*usage.RecordUsageRequest){
		"no event_id": func(r *usage.RecordUsageRequest) { r.EventID = "" },
		"no app_id":   func(r *usage.RecordUsageRequest) { r.AppID = uuid.Nil },
		"no module":   func(r *usage.RecordUsageRequest) { r.ModuleID = uuid.Nil },
		"no metric":   func(r *usage.RecordUsageRequest) { r.Metric = "" },
		"both owners": func(r *usage.RecordUsageRequest) { r.OwnerOrgID = uuid.New() },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := base
			mutate(&req)
			_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

func TestRecordUsage_DefaultsRecordedAtToNow(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	req.RecordedAt = time.Time{}

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, store.events[req.EventID].RecordedAt.IsZero())
}

func TestRecordUsage_CarriesModuleVersion(t *testing.T) {
	// The optional ModuleVersion field (migration 023, purely reporting) is
	// carried onto the usage_events.module_version column.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	req.ModuleVersion = "3.2.1"

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "3.2.1", store.events[req.EventID].ModuleVersion)
}

func TestRecordUsage_ModuleVersionEmptyWhenNotCarried(t *testing.T) {
	store := newFakeStore()
	req := validRecord() // no ModuleVersion set
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "", store.events[req.EventID].ModuleVersion)
}

func TestRecordUsage_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	store.errInsert = errors.New("boom")
	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInternal)
}

func TestRecordUsage_InternalOnLookupError(t *testing.T) {
	// A store failure resolving the catalog (LookupMetricDefinition) is an
	// INTERNAL error, distinct from the INVALID_INPUT "metric not declared"
	// no-row path. Exercises the billing.Internal branch at service.go:88.
	store := newFakeStore()
	req := validRecord()
	store.errLookup = errors.New("boom")
	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInternal)
	require.Empty(t, store.events, "no event recorded when the catalog lookup fails")
}

// fakeBudgetEvaluator satisfies usage.BudgetEvaluator. err lets a test force
// a budget-eval failure to prove it does NOT fail the usage ingest; called
// records whether the hook ran.
type fakeBudgetEvaluator struct {
	err     error
	called  bool
	gotApp  uuid.UUID
	gotFrom time.Time
	gotTo   time.Time
}

func (f *fakeBudgetEvaluator) EvaluateAppBudget(_ context.Context, appID uuid.UUID, from, to time.Time) ([]int, error) {
	f.called = true
	f.gotApp = appID
	f.gotFrom = from
	f.gotTo = to
	return nil, f.err
}

func TestRecordUsage_BudgetEvalErrorDoesNotFailIngest(t *testing.T) {
	// Best-effort hook (design §10): a budget-evaluation error must NOT fail
	// the usage ingest — the event is already recorded.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	eval := &fakeBudgetEvaluator{err: errors.New("budget boom")}

	svc := usage.NewService(store).WithBudgetEvaluator(eval)
	resp, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err, "budget error must not surface on the ingest path")
	require.True(t, resp.Recorded)
	require.True(t, eval.called, "the hook fires on a fresh insert")
	require.Equal(t, req.AppID, eval.gotApp)
	require.Len(t, store.events, 1)
}

func TestRecordUsage_BudgetEvalSkippedOnDedupedRetry(t *testing.T) {
	// A deduped retry (recorded=false) was already evaluated for its event_id;
	// the hook must be skipped so the same spend isn't re-walked.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	eval := &fakeBudgetEvaluator{}
	svc := usage.NewService(store).WithBudgetEvaluator(eval)

	_, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, eval.called)

	eval.called = false
	_, err = svc.RecordUsage(context.Background(), req) // same event_id → deduped
	require.NoError(t, err)
	require.False(t, eval.called, "hook is skipped on a deduped retry")
}

type fakeCreditEvaluator struct {
	err    error
	calls  int
	events []credit.UsageEvent
}

func (f *fakeCreditEvaluator) EvaluateCreditUsage(_ context.Context, event credit.UsageEvent) error {
	f.calls++
	f.events = append(f.events, event)
	return f.err
}

func TestRecordUsage_CreditEvaluatorRunsOnceForFreshEventOnly(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	accountID := uuid.New()
	store.accounts[req.OwnerUserID] = accountID
	eval := &fakeCreditEvaluator{}
	svc := usage.NewService(store).WithCreditEvaluator(eval)

	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)
	second, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)

	require.Equal(t, 1, eval.calls, "the deduped event must not increment credit exposure twice")
	require.Len(t, eval.events, 1)
	require.Equal(t, accountID, eval.events[0].AccountID)
	require.Equal(t, req.EventID, eval.events[0].EventID)
}

func TestRecordUsage_KeyedPeakCreditUsesOnlyPositiveSubjectMaxDelta(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	accountID := uuid.New()
	req := v2Record(now)
	req.Subject = "end-user-1"
	req.Value = 1
	store.accounts[req.OwnerUserID] = accountID
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindPeak, AggregationKey: usage.AggregationKeySubject,
		UnitPriceMicros: 250, Priced: true, Active: true,
	}
	eval := &fakeCreditEvaluator{}
	svc := usage.NewService(store).
		WithNow(func() time.Time { return now }).
		WithCreditEvaluator(eval)

	_, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	req.EventID = "evt-2"
	req.Metadata = json.RawMessage(`{"provider":"github"}`)
	_, err = svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	req.EventID = "evt-3"
	req.Value = 3
	_, err = svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)

	require.Len(t, eval.events, 3)
	require.Equal(t, int64(250), eval.events[0].ApproximateChargeMicros)
	require.Zero(t, eval.events[1].ApproximateChargeMicros, "provider duplicate cannot increase keyed MAX")
	require.Equal(t, int64(500), eval.events[2].ApproximateChargeMicros, "only MAX increase from 1 to 3 is charged")
}

func TestRecordUsage_CreditEvaluatorErrorDoesNotFailIngest(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	store.accounts[req.OwnerUserID] = uuid.New()
	eval := &fakeCreditEvaluator{err: errors.New("credit evaluator down")}

	resp, err := usage.NewService(store).
		WithCreditEvaluator(eval).
		RecordUsage(context.Background(), req)

	require.NoError(t, err)
	require.True(t, resp.Recorded)
	require.Equal(t, 1, eval.calls)
}

// --- GetUsageSummary ------------------------------------------------------

func TestGetUsageSummary_ChargesDeclaredPriceNoMarkup(t *testing.T) {
	// Declaration-first (design §1 / §4 Axis 1): a custom metric is charged
	// at quantity × the developer's declared price with NO blanket 1.2×, so
	// the customer charge equals the raw (quantity × unit_price) cost.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, int64(1000), resp.Metrics[0].RawCostMicros)
	// No markup: charged == raw.
	require.Equal(t, int64(1000), resp.Metrics[0].ChargedMicros)
	require.Equal(t, int64(100), resp.Metrics[0].UnitPriceMicros)
}

func TestGetUsageSummary_PropagatesDisplayGroup(t *testing.T) {
	// §11 billing-display compaction: the catalog's display_group classification
	// (resolved at the store from metric_definitions.display_group) must travel
	// verbatim through GetUsageSummary so api-platform can proxy it and the
	// frontend can roll metrics up into ~7 group rows. billing-engine is the
	// AUTHORITATIVE classifier; the service never re-derives the group.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "infra.ai.input.tokens", Kind: usage.KindSum, Quantity: 5, UnitPriceMicros: 1000, RawCostMicros: 5000, Group: "ai"},
		{Metric: "infra.egress.bytes", Kind: usage.KindSum, Quantity: 2, UnitPriceMicros: 1, RawCostMicros: 2, Group: "network"},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)
	require.Equal(t, "ai", resp.Metrics[0].Group)
	require.Equal(t, "network", resp.Metrics[1].Group)
}

func TestGetUsageSummary_DefaultsGroupToOther(t *testing.T) {
	// A custom (Plane-2) metric, or any infra metric not yet mapped, carries
	// display_group 'other' — the store COALESCEs a missing/ungrouped catalog
	// row to "other" (mirroring the column's NOT NULL DEFAULT 'other'). The
	// service passes that through unchanged so the frontend always has a valid
	// group to bucket into.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Group: "other"},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, "other", resp.Metrics[0].Group)
}

func TestGetUsageSummary_PropagatesModuleIDAndVisibility(t *testing.T) {
	// A consumer previously had to hardcode a 30% platform-take assumption
	// because it couldn't see the real module_visibility value; GetUsageSummary
	// now carries both the emitting module_id and its visibility per metric.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Visibility: usage.VisibilityPublished},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, mod, resp.Metrics[0].ModuleID)
	require.Equal(t, usage.VisibilityPublished, resp.Metrics[0].Visibility)
}

func TestGetUsageSummary_DefaultsVisibilityToPrivate(t *testing.T) {
	// A module with no visibility row yet defaults to 'private' (the higher
	// platform-take rate), matching the settlement default (design §7-B: never
	// under-collect on a lagging publish).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Visibility: usage.VisibilityPrivate},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, usage.VisibilityPrivate, resp.Metrics[0].Visibility)
}

func TestGetUsageSummary_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Metrics)
}

func TestGetUsageSummary_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestGetUsageSummary_WindowsOnAccountAnchorDay proves the live-summary window is
// anchored to the account's card-binding day (ADR 0005), not the 1st, and that
// the exact anchored [start, end) reaches the store unchanged.
func TestGetUsageSummary_WindowsOnAccountAnchorDay(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	acct := uuid.New()
	store.accounts[owner] = acct
	store.anchorDays[acct] = 17 // bound a card on the 17th

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	// Both boundaries fall on the 17th (17 ≤ 28, so no month ever clamps it).
	require.Equal(t, 17, resp.PeriodStart.Day(), "period starts on the anchor day, not the 1st")
	require.Equal(t, 17, resp.PeriodEnd.Day(), "period ends on the next anchor boundary")
	require.Equal(t, time.UTC, resp.PeriodStart.Location())
	// The resolved window is the one handed to the store (threaded unchanged).
	require.True(t, resp.PeriodStart.Equal(store.gotPeriodStart), "start threaded to store")
	require.True(t, resp.PeriodEnd.Equal(store.gotPeriodEnd), "end threaded to store")
}

// TestGetUsageSummary_DefaultsToCalendarMonthWhenUnactivated proves an account
// with no card-binding anchor (fake default) windows on the 1st — the pre-025
// calendar month — so un-activated accounts keep the historical behavior.
func TestGetUsageSummary_DefaultsToCalendarMonthWhenUnactivated(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New() // no anchorDays entry → default day 1
	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Equal(t, 1, resp.PeriodStart.Day(), "un-activated account windows on the calendar month")
}

// TestGetUsageSummary_AnchorLookupErrorSurfaces proves an anchor-day lookup error
// fails the read loud (Internal) rather than silently mis-windowing.
func TestGetUsageSummary_AnchorLookupErrorSurfaces(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAnchor = errors.New("anchor db down")
	_, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetUsageHistory -------------------------------------------------------

func TestGetUsageHistory_BucketsRowsIntoOrderedPeriods(t *testing.T) {
	// Multi-month data returns correctly ordered/bucketed: rows for two
	// different periods (already ordered period_start ASC, metric ASC by the
	// store contract) must split into two PeriodUsage entries, oldest first,
	// each carrying only its own period's metrics.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mod := uuid.New()
	store.historyRows = []usage.PeriodMetricUsageRaw{
		{PeriodStart: jan, PeriodEnd: feb, ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, RawCostMicros: 1000, ChargedMicros: 1000, Visibility: usage.VisibilityPublished},
		{PeriodStart: feb, PeriodEnd: mar, ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 20, RawCostMicros: 2000, ChargedMicros: 2000, Visibility: usage.VisibilityPublished},
	}

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 2)
	require.True(t, resp.Periods[0].PeriodStart.Equal(jan), "oldest period first")
	require.True(t, resp.Periods[1].PeriodStart.Equal(feb))
	require.Len(t, resp.Periods[0].Metrics, 1)
	require.EqualValues(t, 1000, resp.Periods[0].Metrics[0].ChargedMicros)
	require.EqualValues(t, 2000, resp.Periods[1].Metrics[0].ChargedMicros)
	// Module attribution rides through — the pre-#32 shape dropped both.
	require.Equal(t, mod, resp.Periods[0].Metrics[0].ModuleID)
	require.Equal(t, usage.VisibilityPublished, resp.Periods[0].Metrics[0].Visibility)
}

func TestGetUsageHistory_MultipleMetricsWithinOnePeriod(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	store.historyRows = []usage.PeriodMetricUsageRaw{
		{PeriodStart: jan, PeriodEnd: feb, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, ChargedMicros: 1000},
		{PeriodStart: jan, PeriodEnd: feb, Metric: "storage.bytes", Kind: usage.KindTimeWeighted, Quantity: 5, ChargedMicros: 500},
	}

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 1, "both rows share one period_start")
	require.Len(t, resp.Periods[0].Metrics, 2)
}

func TestGetUsageHistory_MissingMonthsDoNotError(t *testing.T) {
	// A month with no rolled-up usage (rollup hasn't run, or zero usage)
	// simply contributes no row — a gap in the returned Periods, never an
	// error.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	// historyRows stays empty: no usage_aggregates rows exist for the window.

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Empty(t, resp.Periods)
}

func TestGetUsageHistory_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: uuid.New(), Months: 6})
	require.NoError(t, err)
	require.Empty(t, resp.Periods)
}

func TestGetUsageHistory_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{Months: 6})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetUsageHistory_RejectsNonPositiveMonths(t *testing.T) {
	for _, months := range []int{0, -1} {
		_, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: uuid.New(), Months: months})
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

func TestGetUsageHistory_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errHistory = errors.New("boom")
	_, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetVersionBreakdown ---------------------------------------------------

func TestGetVersionBreakdown_GroupsByVersion(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.versionRows = []usage.VersionUsageRaw{
		{ModuleVersion: "1.0.0", BillableQuantity: 10, RawCostMicros: 1000, ChargedMicros: 1000},
		{ModuleVersion: "2.0.0", BillableQuantity: 5, RawCostMicros: 500, ChargedMicros: 500},
	}

	resp, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 2)
	require.Equal(t, "1.0.0", resp.Versions[0].ModuleVersion)
	require.EqualValues(t, 1000, resp.Versions[0].ChargedMicros)
	require.Equal(t, "2.0.0", resp.Versions[1].ModuleVersion)
	require.EqualValues(t, 500, resp.Versions[1].ChargedMicros)
}

func TestGetVersionBreakdown_EmptyVersionRollsUpWithoutCrashing(t *testing.T) {
	// An event with an empty/missing version rolls up under '' (migration
	// 023's COALESCE(module_version, '')) rather than erroring.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.versionRows = []usage.VersionUsageRaw{
		{ModuleVersion: "", BillableQuantity: 10, RawCostMicros: 1000, ChargedMicros: 1000},
	}

	resp, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 1)
	require.Equal(t, "", resp.Versions[0].ModuleVersion)
}

func TestGetVersionBreakdown_PassesThroughOptionalModuleFilter(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()

	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner, ModuleID: mod})
	require.NoError(t, err)
	require.Equal(t, mod, store.gotVersionModuleID)
}

func TestGetVersionBreakdown_NoModuleFilterMeansAllModules(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.gotVersionModuleID, "omitted module_id reaches the store as the zero UUID (no filter)")
}

func TestGetVersionBreakdown_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Versions)
}

func TestGetVersionBreakdown_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetVersionBreakdown_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errVersion = errors.New("boom")
	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetAppUsageSummary ----------------------------------------------------

func TestGetAppUsageSummary_ReturnsPerModuleVersionLines(t *testing.T) {
	// The app bill carries one line per (module, metric, model, module_version)
	// so the UI can render per-version sub-lines (data exists — migration 023).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appRows = []usage.AppMetricUsageRaw{
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, ModuleVersion: "1.0.0", BillableQuantity: 4, UnitPriceMicros: 100, ChargedMicros: 400},
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, ModuleVersion: "2.0.0", BillableQuantity: 6, UnitPriceMicros: 100, ChargedMicros: 600},
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)
	require.Equal(t, mod, resp.Metrics[0].ModuleID)
	require.Equal(t, "1.0.0", resp.Metrics[0].ModuleVersion)
	require.EqualValues(t, 400, resp.Metrics[0].ChargedMicros)
	require.Equal(t, "2.0.0", resp.Metrics[1].ModuleVersion)
	require.EqualValues(t, 600, resp.Metrics[1].ChargedMicros)
}

func TestGetAppUsageSummary_ForwardsActiveWindowFields(t *testing.T) {
	// GetAppUsageSummary must carry ActiveSeconds/PeriodDays from the raw store
	// row onto the wire AppMetricUsage verbatim (usage-time-pricing Phase 2,
	// display-only read path) — nil stays nil, a populated value stays exact.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	rolledActiveSeconds := 1_296_000.0
	rolledPeriodDays := 30.0
	store.appRows = []usage.AppMetricUsageRaw{
		{Metric: "storage.gib_hours", Kind: usage.KindTimeWeighted, ModuleVersion: "1.0.0",
			ActiveSeconds: &rolledActiveSeconds, PeriodDays: &rolledPeriodDays},
		{Metric: "orders.placed", Kind: usage.KindCount, ModuleVersion: "1.0.0"}, // live / additive: both nil
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)

	rolled := resp.Metrics[0]
	require.NotNil(t, rolled.ActiveSeconds)
	require.NotNil(t, rolled.PeriodDays)
	require.EqualValues(t, 1_296_000.0, *rolled.ActiveSeconds)
	require.EqualValues(t, 30.0, *rolled.PeriodDays)

	notRolled := resp.Metrics[1]
	require.Nil(t, notRolled.ActiveSeconds, "nil stays nil — never coerced to 0")
	require.Nil(t, notRolled.PeriodDays, "nil stays nil — never coerced to 0")
}

func TestGetAppUsageSummary_ChargesDeclaredPriceNoMarkup(t *testing.T) {
	// The app owner pays the module's declared unit_price per unit with NO
	// customer markup by visibility — charged == unit_price × quantity, and the
	// response carries no visibility/markup fields at all.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appRows = []usage.AppMetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, BillableQuantity: 10, UnitPriceMicros: 100, ChargedMicros: 1000},
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.EqualValues(t, 100, resp.Metrics[0].UnitPriceMicros)
	require.EqualValues(t, 1000, resp.Metrics[0].ChargedMicros)
}

func TestGetAppUsageSummary_GatesOnPayerAccountAndApp(t *testing.T) {
	// account_id (resolved from the owner principal) gates the payer; app_id
	// filters to the one app. Both must reach the store unchanged.
	store := newFakeStore()
	owner := uuid.New()
	acct := uuid.New()
	store.accounts[owner] = acct
	app := uuid.New()

	_, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: app})
	require.NoError(t, err)
	require.Equal(t, acct, store.gotAppUsageAccountID, "the payer account gates the query")
	require.Equal(t, app, store.gotAppUsageAppID, "the app_id filters to the one app")
}

func TestGetAppUsageSummary_EchoesAppIDAndWindow(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	app := uuid.New()

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: app})
	require.NoError(t, err)
	require.Equal(t, app, resp.AppID)
	require.False(t, resp.PeriodStart.IsZero())
	require.True(t, resp.PeriodEnd.After(resp.PeriodStart))
}

func TestGetAppUsageSummary_NoAccountReturnsEmpty(t *testing.T) {
	// No billing account yet → empty Metrics slice (not nil) + nil error, and
	// the requested app is still echoed.
	app := uuid.New()
	resp, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: uuid.New(), AppID: app})
	require.NoError(t, err)
	require.Empty(t, resp.Metrics)
	require.Equal(t, app, resp.AppID)
}

func TestGetAppUsageSummary_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{AppID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(), AppID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_RequiresAppID(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAppUsage = errors.New("boom")
	_, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	requireCode(t, err, billing.CodeInternal)
}

// --- SetModuleVisibility --------------------------------------------------

func TestSetModuleVisibility_Upserts(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	_, err := newService(store).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		ModuleID: mod, Visibility: usage.VisibilityPublished,
	})
	require.NoError(t, err)
	require.Equal(t, usage.VisibilityPublished, store.visibility[mod])
}

func TestSetModuleVisibility_RejectsBadVisibility(t *testing.T) {
	_, err := newService(newFakeStore()).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		ModuleID: uuid.New(), Visibility: usage.Visibility("nonsense"),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetModuleVisibility_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		Visibility: usage.VisibilityPrivate,
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- SetMetricDefinitions -------------------------------------------------

func TestSetMetricDefinitions_SyncsCatalog(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	resp, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: mod,
		Metrics: []usage.MetricDef{
			{Metric: "orders.placed", Kind: usage.KindCount, Unit: "order", UnitPriceMicros: 50_000, Priced: true, Active: true},
			{Metric: "myapp.objects.bytes", Kind: usage.KindTimeWeighted, Unit: "byte", Active: true}, // unpriced
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, resp.Synced)

	got := store.defs[defKey(mod, "orders.placed")]
	require.Equal(t, usage.KindCount, got.Kind)
	require.True(t, got.Priced)
	require.Equal(t, int64(50_000), got.UnitPriceMicros)

	unpriced := store.defs[defKey(mod, "myapp.objects.bytes")]
	require.False(t, unpriced.Priced, "unpriced metric stays unpriced")
}

func TestSetMetricDefinitions_SubjectAggregationIsPeakOnly(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	_, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: mod,
		Metrics: []usage.MetricDef{{
			Metric: "users.monthly_active", Kind: usage.KindPeak,
			AggregationKey: usage.AggregationKeySubject, Active: true,
		}},
	})
	require.NoError(t, err)
	require.Equal(t, usage.AggregationKeySubject, store.defs[defKey(mod, "users.monthly_active")].AggregationKey)

	for name, metric := range map[string]usage.MetricDef{
		"unknown key": {Metric: "users.bad", Kind: usage.KindPeak, AggregationKey: usage.AggregationKey("metadata.user"), Active: true},
		"non peak":    {Metric: "users.bad", Kind: usage.KindCount, AggregationKey: usage.AggregationKeySubject, Active: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newService(newFakeStore()).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
				ModuleID: uuid.New(), Metrics: []usage.MetricDef{metric},
			})
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

func TestSetMetricDefinitions_RejectsReservedPrefix(t *testing.T) {
	store := newFakeStore()
	_, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "infra.egress.bytes", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs, "reserved metric must not be synced")
}

func TestSetMetricDefinitions_RejectsBadKind(t *testing.T) {
	_, err := newService(newFakeStore()).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "orders.placed", Kind: usage.Kind("nonsense"), Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetMetricDefinitions_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		Metrics: []usage.MetricDef{{Metric: "orders.placed", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetMetricDefinitions_InternalOnStoreError(t *testing.T) {
	// A store failure on the batch upsert surfaces as INTERNAL (the catalog
	// sync is all-or-nothing; the transaction rolls back). Exercises the
	// billing.Internal branch around the UpsertMetricDefinitions call.
	store := newFakeStore()
	store.errUpsertDef = errors.New("boom")
	_, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "orders.placed", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInternal)
	require.Empty(t, store.defs, "all-or-nothing: nothing synced when the store errors")
}

// --- SetInfraPriceOverrides (decision 19 §4.3) ----------------------------

// seedSentinelInfra seeds a SENTINEL base infra catalog row into the fake so an
// override can inherit its kind/unit (the real store's INSERT ... SELECT source).
func seedSentinelInfra(store *fakeStore, metric string, kind usage.Kind, unit string, priceMicros int64) {
	store.defs[defKey(usage.PlatformInfraModuleID(), metric)] = usage.MetricDefinition{
		Kind: kind, Unit: unit, UnitPriceMicros: priceMicros, Priced: true, Active: true,
	}
}

func TestSetInfraPriceOverrides_WritesPriceOnlyRowInheritingKindUnit(t *testing.T) {
	store := newFakeStore()
	// The sentinel base row (as migration 017 seeds it): sum / millisecond / 1µ$.
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()

	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.Synced)

	// The override row is keyed by the REAL module id (never the sentinel),
	// carries the override PRICE, and INHERITS kind + unit from the sentinel row.
	got := store.defs[defKey(mod, "infra.compute.walltime.ms")]
	require.Equal(t, int64(5), got.UnitPriceMicros, "override price is written")
	require.Equal(t, usage.KindSum, got.Kind, "kind inherited from sentinel")
	require.Equal(t, "millisecond", got.Unit, "unit inherited from sentinel")
	require.True(t, got.Active)

	// The sentinel base row is untouched (still the platform default price).
	base := store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")]
	require.Equal(t, int64(1), base.UnitPriceMicros, "sentinel base price unchanged")
}

func TestSetInfraPriceOverrides_ZeroPriceIsFullAbsorb(t *testing.T) {
	// ms.Price(0) → override 0 → full absorb. Zero is a VALID override price
	// (not "unpriced"), so it must persist as 0, not be rejected.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()

	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.Synced)
	require.Equal(t, int64(0), store.defs[defKey(mod, "infra.compute.walltime.ms")].UnitPriceMicros)
}

func TestSetInfraPriceOverrides_RejectsNonReservedMetric(t *testing.T) {
	// A custom (non-reserved) metric belongs on SetMetricDefinitions — this
	// RPC is the INVERSE gate and rejects it.
	store := newFakeStore()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "orders.placed", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs, "non-reserved metric must not be written")
}

func TestSetInfraPriceOverrides_RejectsUnregisteredReservedMetric(t *testing.T) {
	// A reserved-prefixed name that is NOT a registered platform infra metric
	// has no platform-owned catalog row to inherit from → rejected.
	store := newFakeStore()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.not.a.real.metric", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs)
}

func TestSetInfraPriceOverrides_RejectsSentinelModuleID(t *testing.T) {
	// The all-zero sentinel is the platform's BASE catalog, seeded by migration
	// and never re-priced through this RPC.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  usage.PlatformInfraModuleID(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	// The sentinel base row is unchanged (only the seeded row exists).
	require.Len(t, store.defs, 1)
	require.Equal(t, int64(1), store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")].UnitPriceMicros)
}

func TestSetInfraPriceOverrides_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetInfraPriceOverrides_RejectsNegativePrice(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: -1}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetInfraPriceOverrides_AllOrNothingRejectsWholeBatch(t *testing.T) {
	// One invalid override in the batch rejects the whole request BEFORE the
	// store is touched, so no partial write lands.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod,
		Overrides: []usage.InfraPriceOverride{
			{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}, // valid
			{Metric: "orders.placed", UnitPriceMicros: 5},             // invalid (non-reserved)
		},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs[defKey(mod, "infra.compute.walltime.ms")].Unit, "no override written when any in the batch is invalid")
}

// 🔴 AN EMPTY CALL IS A WITHDRAWAL, NOT A NO-OP. This is the whole reason the
// store method is Sync and not Upsert: when a module deletes its ms.Price line,
// the manifest arrives carrying nothing, and if that means "do nothing" the old
// row keeps pricing the app owner's bill at a price no module declares.
func TestSetInfraPriceOverrides_EmptyPayloadWithdrawsThePreviousSet(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()
	svc := newService(store)

	_, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	require.NoError(t, err)
	require.True(t, store.defs[defKey(mod, "infra.compute.walltime.ms")].Priced, "precondition: the override exists")

	// The author deletes the declaration and republishes.
	resp, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{ModuleID: mod})
	require.NoError(t, err)
	require.Equal(t, 0, resp.Synced)
	_, still := store.defs[defKey(mod, "infra.compute.walltime.ms")]
	require.False(t, still, "the withdrawn override is gone, so the line reverts to the platform default")
}

// Dropping ONE of several overrides withdraws only that one — the sweep is
// keyed on the metrics the manifest still names, not on the whole set being empty.
func TestSetInfraPriceOverrides_WithdrawsOnlyTheDroppedMetric(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	seedSentinelInfra(store, "infra.egress.api.bytes", usage.KindSum, "GiB", 90_000)
	mod := uuid.New()
	svc := newService(store)

	_, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod,
		Overrides: []usage.InfraPriceOverride{
			{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5},
			{Metric: "infra.egress.api.bytes", UnitPriceMicros: 45_000},
		},
	})
	require.NoError(t, err)

	_, err = svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.egress.api.bytes", UnitPriceMicros: 45_000}},
	})
	require.NoError(t, err)

	_, compute := store.defs[defKey(mod, "infra.compute.walltime.ms")]
	require.False(t, compute, "the dropped metric is withdrawn")
	require.Equal(t, int64(45_000), store.defs[defKey(mod, "infra.egress.api.bytes")].UnitPriceMicros,
		"the still-declared metric keeps its override")
}

// 🔴 THE SWEEP MUST NOT REACH A MODULE'S OWN CUSTOM METRICS. Both live in
// metric_definitions under the same module_id; only the reserved namespaces are
// this call's business, and SetMetricDefinitions owns the rest.
func TestSetInfraPriceOverrides_WithdrawalLeavesCustomMetricsAlone(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()
	store.defs[defKey(mod, "video.publish")] = usage.MetricDefinition{
		Kind: usage.KindCount, Unit: "video", UnitPriceMicros: 30_000, Priced: true, Active: true,
	}
	svc := newService(store)

	_, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	require.NoError(t, err)
	_, err = svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{ModuleID: mod})
	require.NoError(t, err)

	require.Equal(t, int64(30_000), store.defs[defKey(mod, "video.publish")].UnitPriceMicros,
		"the module's own custom metric survives an infra withdrawal")
}

// --- ms.AbsorbInfra() ------------------------------------------------------

func TestSetInfraPriceOverrides_AbsorbAllZeroesEveryActiveSentinelMetric(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	seedSentinelInfra(store, "infra.egress.api.bytes", usage.KindSum, "GiB", 90_000)
	seedSentinelInfra(store, "infra.storage.gib_hours", usage.KindTimeWeighted, "GiB-hour", 32)
	mod := uuid.New()

	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		AbsorbAll: true,
	})
	require.NoError(t, err)
	require.Equal(t, 0, resp.Synced, "Synced counts EXPLICIT overrides; the absorbed set is the catalog's size")
	require.True(t, resp.AbsorbAll)

	// The module never named a metric — the set came from the sentinel catalog.
	for _, m := range []string{"infra.compute.walltime.ms", "infra.egress.api.bytes", "infra.storage.gib_hours"} {
		got, ok := store.defs[defKey(mod, m)]
		require.True(t, ok, "%s absorbed", m)
		require.Equal(t, int64(0), got.UnitPriceMicros, "%s passes through at 0", m)
		require.True(t, got.Priced, "%s is priced-at-0, not unpriced", m)
	}
	// kind + unit still come from the sentinel row, never from the caller.
	require.Equal(t, "GiB-hour", store.defs[defKey(mod, "infra.storage.gib_hours")].Unit)
}

// "Absorb everything except egress, which I mark up" — the explicit override is
// applied after the expansion, so it wins.
func TestSetInfraPriceOverrides_ExplicitOverrideBeatsAbsorbAll(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	seedSentinelInfra(store, "infra.egress.api.bytes", usage.KindSum, "GiB", 90_000)
	mod := uuid.New()

	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		AbsorbAll: true,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.egress.api.bytes", UnitPriceMicros: 120_000}},
	})
	require.NoError(t, err)

	require.Equal(t, int64(0), store.defs[defKey(mod, "infra.compute.walltime.ms")].UnitPriceMicros, "absorbed")
	require.Equal(t, int64(120_000), store.defs[defKey(mod, "infra.egress.api.bytes")].UnitPriceMicros,
		"the named metric overwrites the absorbed 0, not the other way round")
}

// A metric the platform has since DEACTIVATED is not re-absorbed, and its stale
// row from an earlier publish is swept — otherwise absorb-all would resurrect a
// price for a metric the catalog retired.
func TestSetInfraPriceOverrides_AbsorbAllSkipsAndSweepsInactiveSentinelMetrics(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()
	svc := newService(store)

	_, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod, AbsorbAll: true,
	})
	require.NoError(t, err)
	require.True(t, store.defs[defKey(mod, "infra.compute.walltime.ms")].Priced, "precondition: absorbed")

	// The platform retires it.
	sentinel := store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")]
	sentinel.Active = false
	store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")] = sentinel

	_, err = svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod, AbsorbAll: true,
	})
	require.NoError(t, err)
	_, still := store.defs[defKey(mod, "infra.compute.walltime.ms")]
	require.False(t, still, "a retired metric is neither re-absorbed nor left behind")
}

// Withdrawing ms.AbsorbInfra() itself clears the whole expanded set.
func TestSetInfraPriceOverrides_WithdrawingAbsorbAllClearsEverything(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	seedSentinelInfra(store, "infra.egress.api.bytes", usage.KindSum, "GiB", 90_000)
	mod := uuid.New()
	svc := newService(store)

	_, err := svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod, AbsorbAll: true,
	})
	require.NoError(t, err)

	_, err = svc.SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{ModuleID: mod})
	require.NoError(t, err)
	for _, m := range []string{"infra.compute.walltime.ms", "infra.egress.api.bytes"} {
		_, still := store.defs[defKey(mod, m)]
		require.False(t, still, "%s reverts to the platform default", m)
	}
}

func TestSetInfraPriceOverrides_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	store.errUpsertOverride = errors.New("boom")
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInternal)
}

func TestSetInfraPriceOverrides_AcceptsAllRegisteredInfraMetrics(t *testing.T) {
	// Every metric RecordInfraUsage accepts is also overridable here (the two
	// gates share platformInfraKind), including the platform.* namespace door.
	metrics := []string{
		"infra.compute.walltime.ms", "infra.ai.input.tokens", "infra.ai.requests",
		"infra.request.count", "infra.mcp.tool_call.count", "infra.cron.count",
		"infra.event.count", "infra.event.bytes", "infra.egress.api.bytes",
		"infra.storage.put.count", "infra.storage.list.count", "infra.storage.gib_hours",
	}
	store := newFakeStore()
	for _, m := range metrics {
		seedSentinelInfra(store, m, usage.KindSum, "unit", 1)
	}
	mod := uuid.New()
	overrides := make([]usage.InfraPriceOverride, 0, len(metrics))
	for _, m := range metrics {
		overrides = append(overrides, usage.InfraPriceOverride{Metric: m, UnitPriceMicros: 0})
	}
	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: overrides,
	})
	require.NoError(t, err)
	require.Equal(t, len(metrics), resp.Synced)
}
