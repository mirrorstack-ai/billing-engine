package cycle_test

import (
	"context"
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// --- in-memory Store fake -------------------------------------------------

type aggKey struct {
	period, app, module, metric, model, moduleVersion string
}

type fakeStore struct {
	// rollup inputs
	raws        []cycle.RawAggregate
	prices      map[string]int64 // module/metric → price; absent = unpriced (0)
	modelPrices map[string]int64 // metric/model → per-model price (migration 018); checked before prices when a model is carried
	// versionPrices models the metric_version_prices immutable snapshot
	// (migration 044): module/metric/version → price, checked BEFORE model
	// and catalog when the row carries a module_version — mirrors the
	// pgxStore's version-first resolution.
	versionPrices map[string]int64
	// inactiveModelPrices models metric_model_prices rows that EXIST but were
	// retired (active=false). A model in this set returns ErrInactiveModelPrice
	// (the rollup fails loud) rather than silently falling back to the catalog,
	// mirroring the pgxStore's active-flag handling.
	inactiveModelPrices map[string]bool // metric/model → retired
	// settlement inputs
	incomes    []cycle.ModuleIncome
	visibility map[uuid.UUID]cycle.Visibility

	// captured writes
	periodID    uuid.UUID
	aggregates  map[aggKey]cycle.MetricAggregate
	settlements map[string]cycle.ModuleSettlement // period/module → settlement

	// charge-cycle inputs
	chargedTotal     int64       // PeriodChargedTotal return
	hasPM            bool        // HasUsableDefaultPM return
	stripeCustomer   string      // AccountStripeCustomer return
	unbilledAccounts []uuid.UUID // AccountsWithUnbilledUsage return
	usageEventAccts  []uuid.UUID // AccountsWithUsageEvents return

	// universal credit-wallet inputs/captured draw state (billing-engine#95).
	// Sources retain their remaining amount so reclaimed-cycle tests can prove
	// period idempotency rather than accidentally consuming a second lot.
	walletMode        cycle.CreditBillingMode
	walletSources     map[uuid.UUID]*fakeWalletSource
	walletDraws       map[string][]fakeWalletDraw
	walletDrawOrder   []uuid.UUID
	walletUnallocated int64
	walletStateCalls  int
	// beforeWalletCreditState fires once (one-shot) at the top of WalletCreditState
	// — the seam a test uses to model a concurrent worker mutating a timer between
	// the charging worker's pending re-check and its WalletCreditState read (the
	// Job 3 credits→standard flip window).
	beforeWalletCreditState func(*fakeStore)
	// creationDrawn records the per-app creation-proration wallet debit
	// (billing-engine #99), keyed by app id — the per-CHARGE analogue of the
	// period-keyed walletDraws above (a creation draw carries NO period_id).
	creationDrawn            map[uuid.UUID]int64
	creationWalletDrawCalls  int
	beforeCreationWalletDraw func(*fakeStore, uuid.UUID)
	creationWalletOutcomes   []cycle.ProrationOutcome
	// moduleOverageDrawn records the per-timer module-overage wallet debit
	// (billing-engine Job 3), keyed by timer id — the per-CHARGE analogue of
	// creationDrawn above (a module-overage draw carries NO period_id).
	moduleOverageDrawn          map[uuid.UUID]int64
	moduleOverageDrawCalls      int
	beforeModuleOverageDraw     func(*fakeStore, uuid.UUID)
	moduleOverageWalletOutcomes []cycle.ModuleOverageWalletOutcome

	// anchored close-driver inputs (migration 025 / ADR 0005)
	activatedAccounts []cycle.AccountAnchor   // ActivatedAccounts return
	latestPeriodEnd   map[uuid.UUID]time.Time // LatestClosedPeriodEnd return (absent → not found)

	// onFreezeCharge runs at the top of FreezeBillingRunCharge (see there).
	onFreezeCharge func(runID uuid.UUID)

	// risk-graded collection inputs (PR #9)
	collection    cycle.AccountCollection // AccountCollection return
	unpaidInvoice bool                    // HasUnpaidInvoice return (delinquency signal)

	// captured collection writes
	updatedCollection *cycle.AccountCollection // last UpdateAccountCollection arg

	// apps mirror state (migration 027 / base-fee v1). accountsByUser models
	// the AccountIDByUser resolution; activation the activated_at
	// anchor (absent → unactivated, never charged). baseSnapshots models the
	// migration-028 per-app-period base ledger, keyed (app, period_start) like
	// the PRIMARY KEY, with the source recorded so tests can assert which
	// charge leg wrote (and kept) each row.
	apps           map[uuid.UUID]cycle.AppMirror
	accountsByUser map[uuid.UUID]uuid.UUID
	activation     map[uuid.UUID]time.Time
	baseSnapshots  map[snapKey]fakeBaseSnapshot
	// custom-domain mirror state (migration 047). The map is keyed by the
	// surrogate domain id; InsertDomain enforces the live-hostname partial
	// unique constraint by scanning the currently-live rows.
	domains map[uuid.UUID]*fakeDomain

	// per-account overrides for HasUsableDefaultPM / AccountStripeCustomer
	// (absent → the flat hasPM / stripeCustomer defaults above). The org
	// funding-hop tests need the sponsor and org accounts to differ.
	hasPMByAccount          map[uuid.UUID]bool
	stripeCustomerByAccount map[uuid.UUID]string
	// cardCount overrides UsableNonFraudCardCount per account (the create
	// gate's card predicate); absent → derived from the usable-PM state above.
	cardCount map[uuid.UUID]int

	// org-billing state (migration 041). accountsByOrg models the
	// EnsureOrgAccount get-or-create; orgDesignations the designation rows;
	// appOwnerOrg the roster rows' owner_org_id stamp (cycle.AppMirror does
	// not carry it); orgBacklog seeds OrgUnbilledBacklogMicros; orgNullEvents
	// seeds each org's pending NULL-account event count, consumed by the
	// repoint sweep (swept rows never match again); repointCalls records the
	// sweep's events half so tests can assert the window clamp.
	accountsByOrg   map[uuid.UUID]uuid.UUID
	orgDesignations map[uuid.UUID]cycle.OrgDesignation
	appOwnerOrg     map[uuid.UUID]uuid.UUID
	orgBacklog      map[uuid.UUID]int64
	orgNullEvents   map[uuid.UUID]int64
	repointCalls    []repointCall
	// sponsoredOrgs seeds ListSponsoredOrgIDs (sponsor user → the funded,
	// activated orgs they sponsor) — the /me sponsored-orgs read's roster.
	sponsoredOrgs map[uuid.UUID][]uuid.UUID

	// per-module install-timer state (migration 033). timers models
	// ms_billing.app_module_overage_timers keyed by surrogate id; each row's
	// removed/graceResolved/graceCharged* fields mirror the columns the FIFO +
	// Leg 1 sweep read and write.
	timers map[uuid.UUID]*fakeTimer

	// captured charge writes
	insertedRuns  map[string]uuid.UUID                     // (account/start/end) → run id (the idempotency gate state)
	runStatus     map[uuid.UUID]cycle.BillingRunStatus     // run id → current status (models the DB row's terminal state)
	markedRuns    map[uuid.UUID]markedRun                  // run id → terminal mark
	invoices      map[string]cycle.InvoiceMirror           // stripe_invoice_id → mirror
	frozenCharges map[uuid.UUID]cycle.FrozenBoundaryCharge // run id → frozen boundary charge (migration 035); survives a reclaim

	// injected errors
	errOpen               error
	errRaw                error
	errPrice              error
	errUpsert             error
	errIncome             error
	errVis                error
	errSettle             error
	errInsertRun          error
	errTotal              error
	errPM                 error
	errCustomer           error
	errInvoice            error
	errMarkRun            error
	errUnbilled           error
	errUsageEvents        error
	errActivated          error // ActivatedAccounts
	errLatestPeriod       error // LatestClosedPeriodEnd
	errCollection         error // AccountCollection
	errUpdateColl         error // UpdateAccountCollection
	errUnpaid             error // HasUnpaidInvoice
	errCardCount          error // UsableNonFraudCardCount
	errActivation         error // AccountActivation
	errAppInsert          error // InsertAppMirror
	errAppMirror          error // AppMirror
	errDomainInsert       error // InsertDomain
	errDomainLookup       error // DomainByHostname
	errDomainRemove       error // RemoveDomain
	errDomainsPending     error // DomainsPendingCharge
	errDomainPending      error // DomainStillPending
	errDomainAttempted    error // MarkDomainChargeAttempted
	errDomainResolved     error // MarkDomainChargeResolved
	errDomainCharged      error // MarkDomainCharged
	errLiveDomainCount    error // CountLiveDomainsActivatedBefore
	errSetProration       error // SetAppProrationInvoice
	errSetSkipped         error // SetAppProrationSkipped
	errSetCount           error // SetAppModuleCount
	errMarkDeleted        error // MarkAppDeleted
	errLiveCounts         error // LiveAppsCreatedBefore
	errProrationSnap      error // UpsertProrationBaseSnapshot
	errAdvanceSnap        error // InsertAdvanceBaseSnapshot
	errPendingProration   error // AppsPendingProration
	errChargeLocked       error // ChargeProrationLocked
	errDrawCreationWallet error // DrawCreationProrationFromWallet
	// errPersistAfterStripe fails ChargeProrationLocked's persist phase (Phase 3)
	// AFTER the charge callback's Stripe calls already succeeded — modeling a
	// combined-invoice charge whose guard/timer marks fail to commit (deadlock /
	// transient tx error) even though Stripe already moved the money.
	errPersistAfterStripe error
	errFreezeCharge       error // FreezeBillingRunCharge
	errFrozenCharge       error // BillingRunFrozenCharge

	errLiveTimerCount          error // LiveModuleTimerCountForApp
	errInsertTimers            error // InsertModuleOverageTimers
	errReconcileTimers         error // ReconcileModuleTimersToTarget
	errRemoveNewest            error // SoftRemoveNewestModuleTimers
	errRemoveAllTimers         error // SoftRemoveAllModuleTimersForApp
	errTimersPastGrace         error // ModuleOverageTimersPastGrace
	errTimerRank               error // LiveModuleTimerRankBefore
	errMarkIncluded            error // MarkModuleTimerIncluded
	errMarkTimerCharged        error // MarkModuleTimerCharged
	errDrawModuleOverageWallet error // DrawModuleOverageFromWallet
	errCountOngoingOver        error // CountOngoingOverModuleTimers
	errCoCreatedOver           error // CoCreatedOverModuleTimers
}

// fakeTimer models one ms_billing.app_module_overage_timers row (migration 033).
type fakeTimer struct {
	id                 uuid.UUID
	accountID          uuid.UUID
	appID              uuid.UUID
	installedAt        time.Time
	graceExpiresAt     time.Time
	removed            bool
	removedAt          time.Time
	graceResolved      bool
	graceCharged       bool
	graceChargedAt     time.Time
	graceInvoiceID     string
	graceInvoiceItemID string
	chargeAttemptedAt  time.Time // migration-036 recovery marker
}

type fakeWalletSource struct {
	id        uuid.UUID
	typ       string
	remaining int64
	expiresAt time.Time
	createdAt time.Time
}

type fakeWalletDraw struct {
	sourceID uuid.UUID
	amount   int64 // positive magnitude; the real ledger row is negative
}

// fakeDomain carries the migration-047 mirror plus the one-shot activation
// charge state used by the domain sweep tests.
type fakeDomain struct {
	domain              cycle.Domain
	chargeAttemptedAt   time.Time
	chargeResolved      bool
	chargedAt           time.Time
	chargeInvoiceID     string
	chargeInvoiceItemID string
}

// snapKey mirrors the app_base_snapshots PRIMARY KEY (app_id, period_start).
type snapKey struct {
	app         uuid.UUID
	periodStart time.Time
}

// fakeBaseSnapshot is one recorded app_base_snapshots row plus its source.
type fakeBaseSnapshot struct {
	snap   cycle.AppBaseSnapshot
	source string
}

// markedRun records a MarkBillingRun call so a test can assert the terminal
// status + invoice id + charged cents the cycle wrote.
type markedRun struct {
	status     cycle.BillingRunStatus
	invoiceID  string
	totalCents int64
}

// repointCall records one RepointOrgNullAccountEvents call — orgID/accountID
// plus the windowStart the sweep clamped the swept events into.
type repointCall struct {
	orgID       uuid.UUID
	accountID   uuid.UUID
	windowStart time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		prices:              map[string]int64{},
		modelPrices:         map[string]int64{},
		versionPrices:       map[string]int64{},
		inactiveModelPrices: map[string]bool{},
		visibility:          map[uuid.UUID]cycle.Visibility{},
		periodID:            uuid.New(),
		aggregates:          map[aggKey]cycle.MetricAggregate{},
		settlements:         map[string]cycle.ModuleSettlement{},
		insertedRuns:        map[string]uuid.UUID{},
		runStatus:           map[uuid.UUID]cycle.BillingRunStatus{},
		markedRuns:          map[uuid.UUID]markedRun{},
		invoices:            map[string]cycle.InvoiceMirror{},
		frozenCharges:       map[uuid.UUID]cycle.FrozenBoundaryCharge{},
		walletMode:          cycle.CreditBillingModeStandard,
		walletSources:       map[uuid.UUID]*fakeWalletSource{},
		walletDraws:         map[string][]fakeWalletDraw{},
		creationDrawn:       map[uuid.UUID]int64{},
		moduleOverageDrawn:  map[uuid.UUID]int64{},
		apps:                map[uuid.UUID]cycle.AppMirror{},
		accountsByUser:      map[uuid.UUID]uuid.UUID{},
		activation:          map[uuid.UUID]time.Time{},
		baseSnapshots:       map[snapKey]fakeBaseSnapshot{},
		domains:             map[uuid.UUID]*fakeDomain{},
		timers:              map[uuid.UUID]*fakeTimer{},

		hasPMByAccount:          map[uuid.UUID]bool{},
		cardCount:               map[uuid.UUID]int{},
		stripeCustomerByAccount: map[uuid.UUID]string{},
		accountsByOrg:           map[uuid.UUID]uuid.UUID{},
		orgDesignations:         map[uuid.UUID]cycle.OrgDesignation{},
		appOwnerOrg:             map[uuid.UUID]uuid.UUID{},
		orgBacklog:              map[uuid.UUID]int64{},
		orgNullEvents:           map[uuid.UUID]int64{},
		sponsoredOrgs:           map[uuid.UUID][]uuid.UUID{},
		// Default collection state: arrears mode with a high credit limit + no
		// spend ceiling, so the existing charge tests (which don't set risk
		// fields) flow through the gate to the charge path unchanged. Risk tests
		// override these explicitly.
		collection: cycle.AccountCollection{
			Mode:              cycle.BillingModeArrears,
			CreditLimitMicros: math.MaxInt64, // effectively unlimited so legacy charge tests never tighten
			HasSpendCeiling:   false,
		},
	}
}

// runKey is the idempotency key the fake's InsertBillingRun dedupes on, mirroring
// the DB UNIQUE(account_id, period_start, period_end).
func runKey(accountID uuid.UUID, start, end time.Time) string {
	return accountID.String() + "/" + start.Format(time.RFC3339Nano) + "/" + end.Format(time.RFC3339Nano)
}

func priceKey(moduleID uuid.UUID, metric string) string { return moduleID.String() + "/" + metric }

// modelPriceKey mirrors the metric_model_prices PRIMARY KEY (metric, model).
func modelPriceKey(metric, model string) string { return metric + "/" + model }

// versionPriceKey mirrors the metric_version_prices PRIMARY KEY (module_id,
// metric, module_version).
func versionPriceKey(moduleID uuid.UUID, metric, version string) string {
	return moduleID.String() + "/" + metric + "/" + version
}

func (f *fakeStore) OpenPeriodForAccount(_ context.Context, _ uuid.UUID, _, _ time.Time) (uuid.UUID, error) {
	if f.errOpen != nil {
		return uuid.Nil, f.errOpen
	}
	return f.periodID, nil
}

func (f *fakeStore) RawAggregates(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]cycle.RawAggregate, error) {
	if f.errRaw != nil {
		return nil, f.errRaw
	}
	return f.raws, nil
}

func (f *fakeStore) MetricPriceMicros(_ context.Context, moduleID uuid.UUID, metric, model, moduleVersion string) (int64, bool, error) {
	if f.errPrice != nil {
		return 0, false, f.errPrice
	}
	// VERSION-FIRST (migration 044): an immutable version snapshot wins
	// outright over both the per-model and catalog paths, mirroring the
	// pgxStore's resolution order. A miss falls through unchanged.
	if moduleVersion != "" {
		if p, ok := f.versionPrices[versionPriceKey(moduleID, metric, moduleVersion)]; ok {
			return p, true, nil
		}
	}
	// Per-model price wins when the event carries a model (migration 018); a miss
	// falls back to the (module, metric) catalog price, mirroring the pgxStore. A
	// RETIRED per-model row (active=false) fails loud rather than falling back.
	if model != "" {
		if f.inactiveModelPrices[modelPriceKey(metric, model)] {
			return 0, false, cycle.ErrInactiveModelPrice
		}
		if p, ok := f.modelPrices[modelPriceKey(metric, model)]; ok {
			return p, true, nil
		}
	}
	p, ok := f.prices[priceKey(moduleID, metric)]
	return p, ok, nil
}

func (f *fakeStore) UpsertUsageAggregate(_ context.Context, periodID, _ uuid.UUID, agg cycle.MetricAggregate) error {
	if f.errUpsert != nil {
		return f.errUpsert
	}
	f.aggregates[aggKey{periodID.String(), agg.AppID.String(), agg.ModuleID.String(), agg.Metric, agg.Model, agg.ModuleVersion}] = agg
	return nil
}

func (f *fakeStore) ModuleIncome(_ context.Context, _ uuid.UUID) ([]cycle.ModuleIncome, error) {
	if f.errIncome != nil {
		return nil, f.errIncome
	}
	return f.incomes, nil
}

func (f *fakeStore) ModuleVisibility(_ context.Context, moduleID uuid.UUID) (cycle.Visibility, bool, error) {
	if f.errVis != nil {
		return "", false, f.errVis
	}
	v, ok := f.visibility[moduleID]
	return v, ok, nil
}

func (f *fakeStore) UpsertDeveloperSettlement(_ context.Context, periodID, _ uuid.UUID, s cycle.ModuleSettlement) error {
	if f.errSettle != nil {
		return f.errSettle
	}
	f.settlements[periodID.String()+"/"+s.ModuleID.String()] = s
	return nil
}

func (f *fakeStore) InsertBillingRun(_ context.Context, accountID uuid.UUID, start, end time.Time) (uuid.UUID, bool, error) {
	if f.errInsertRun != nil {
		return uuid.Nil, false, f.errInsertRun
	}
	k := runKey(accountID, start, end)
	if id, exists := f.insertedRuns[k]; exists {
		// Conflict on an existing row. Mirrors the DB ON CONFLICT DO UPDATE …
		// WHERE status <> 'invoiced': an 'invoiced' row blocks (shouldCharge=
		// false); any non-terminal row (skipped_no_pm / failed / pending) is
		// RECLAIMED — same id, reset to pending, shouldCharge=true.
		if f.runStatus[id] == cycle.RunStatusInvoiced {
			return id, false, nil
		}
		f.runStatus[id] = "pending"
		return id, true, nil
	}
	id := uuid.New()
	f.insertedRuns[k] = id
	f.runStatus[id] = "pending"
	return id, true, nil
}

func (f *fakeStore) PeriodChargedTotal(_ context.Context, _ uuid.UUID, _, _ time.Time) (int64, error) {
	if f.errTotal != nil {
		return 0, f.errTotal
	}
	return f.chargedTotal, nil
}

func (f *fakeStore) WalletCreditState(_ context.Context, accountID uuid.UUID, start, end time.Time) (cycle.WalletCreditState, error) {
	f.walletStateCalls++
	if f.beforeWalletCreditState != nil {
		hook := f.beforeWalletCreditState
		f.beforeWalletCreditState = nil
		hook(f)
	}
	key := runKey(accountID, start, end)
	var spendable, drawn int64
	for _, source := range f.walletSources {
		if source.remaining <= 0 || (!source.expiresAt.IsZero() && !source.expiresAt.After(time.Now())) {
			continue
		}
		spendable += source.remaining
	}
	for _, draw := range f.walletDraws[key] {
		drawn += draw.amount
	}
	return cycle.WalletCreditState{
		Mode:                   f.walletMode,
		SpendableBalanceMicros: spendable,
		PeriodDrawnMicros:      drawn,
	}, nil
}

func (f *fakeStore) DrawWalletCredits(_ context.Context, accountID uuid.UUID, start, end time.Time, amountMicros int64, allowNew bool) (cycle.WalletDrawdown, error) {
	key := runKey(accountID, start, end)
	if prior := f.walletDraws[key]; len(prior) > 0 {
		var total int64
		for _, draw := range prior {
			total += draw.amount
		}
		return cycle.WalletDrawdown{Mode: f.walletMode, DrawnMicros: total}, nil
	}
	if !allowNew || amountMicros <= 0 {
		return cycle.WalletDrawdown{Mode: f.walletMode}, nil
	}

	sources := make([]*fakeWalletSource, 0, len(f.walletSources))
	for _, source := range f.walletSources {
		if source.remaining > 0 && (source.expiresAt.IsZero() || source.expiresAt.After(time.Now())) {
			sources = append(sources, source)
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		a, b := sources[i], sources[j]
		tier := func(source *fakeWalletSource) int {
			switch {
			case source.typ == "grant" && !source.expiresAt.IsZero():
				return 0
			case source.typ == "grant", source.typ == "preallocation", source.typ == "refund", source.typ == "adjustment":
				return 1
			default:
				return 2
			}
		}
		if ta, tb := tier(a), tier(b); ta != tb {
			return ta < tb
		}
		if !a.expiresAt.Equal(b.expiresAt) {
			if a.expiresAt.IsZero() {
				return false
			}
			if b.expiresAt.IsZero() {
				return true
			}
			return a.expiresAt.Before(b.expiresAt)
		}
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		return a.id.String() < b.id.String()
	})

	target := amountMicros
	if f.walletMode == cycle.CreditBillingModeStandard {
		var available int64
		for _, source := range sources {
			available += source.remaining
		}
		if target > available {
			target = available
		}
	}
	left := target
	for _, source := range sources {
		if left == 0 {
			break
		}
		consume := source.remaining
		if consume > left {
			consume = left
		}
		source.remaining -= consume
		left -= consume
		f.walletDraws[key] = append(f.walletDraws[key], fakeWalletDraw{sourceID: source.id, amount: consume})
		f.walletDrawOrder = append(f.walletDrawOrder, source.id)
	}
	if left > 0 { // credits mode may spend through zero into its configured limit
		f.walletDraws[key] = append(f.walletDraws[key], fakeWalletDraw{amount: left})
		f.walletUnallocated += left
		left = 0
	}
	return cycle.WalletDrawdown{Mode: f.walletMode, DrawnMicros: target}, nil
}

func (f *fakeStore) HasUsableDefaultPM(_ context.Context, accountID uuid.UUID) (bool, error) {
	if f.errPM != nil {
		return false, f.errPM
	}
	if v, ok := f.hasPMByAccount[accountID]; ok {
		return v, nil
	}
	return f.hasPM, nil
}

func (f *fakeStore) AccountStripeCustomer(_ context.Context, accountID uuid.UUID) (string, error) {
	if f.errCustomer != nil {
		return "", f.errCustomer
	}
	if c, ok := f.stripeCustomerByAccount[accountID]; ok {
		return c, nil
	}
	return f.stripeCustomer, nil
}

func (f *fakeStore) AccountCollection(_ context.Context, _ uuid.UUID) (cycle.AccountCollection, error) {
	if f.errCollection != nil {
		return cycle.AccountCollection{}, f.errCollection
	}
	return f.collection, nil
}

func (f *fakeStore) UpdateAccountCollection(_ context.Context, _ uuid.UUID, c cycle.AccountCollection) error {
	if f.errUpdateColl != nil {
		return f.errUpdateColl
	}
	f.collection = c // persist so a re-run reads the transitioned mode
	cp := c
	f.updatedCollection = &cp
	return nil
}

func (f *fakeStore) TightenAndMarkRun(_ context.Context, _ uuid.UUID, c cycle.AccountCollection, runID uuid.UUID, status cycle.BillingRunStatus) error {
	// Models the atomic tx: persist the mode transition AND mark the run skipped.
	// An injected error on EITHER underlying op fails the whole call (all-or-
	// nothing) so a test can assert the cycle surfaces a tighten-tx failure.
	if f.errUpdateColl != nil {
		return f.errUpdateColl
	}
	if f.errMarkRun != nil {
		return f.errMarkRun
	}
	f.collection = c
	cp := c
	f.updatedCollection = &cp
	f.markedRuns[runID] = markedRun{status: status, totalCents: 0}
	f.runStatus[runID] = status
	return nil
}

func (f *fakeStore) HasUnpaidInvoice(_ context.Context, _ uuid.UUID) (bool, error) {
	if f.errUnpaid != nil {
		return false, f.errUnpaid
	}
	return f.unpaidInvoice, nil
}

func (f *fakeStore) UpsertInvoice(_ context.Context, inv cycle.InvoiceMirror) error {
	if f.errInvoice != nil {
		return f.errInvoice
	}
	f.invoices[inv.StripeInvoiceID] = inv
	return nil
}

func (f *fakeStore) MarkBillingRun(_ context.Context, runID uuid.UUID, status cycle.BillingRunStatus, invoiceID string, totalCents int64) error {
	if f.errMarkRun != nil {
		return f.errMarkRun
	}
	f.markedRuns[runID] = markedRun{status: status, invoiceID: invoiceID, totalCents: totalCents}
	f.runStatus[runID] = status // persist terminal state so a re-run's reclaim gate sees it
	return nil
}

// FreezeBillingRunCharge records the run's frozen boundary charge (migration 035).
// First-write-wins, mirroring the SQL's WHERE frozen_charge_cents IS NULL: a
// reclaim that already froze keeps the ORIGINAL values, so a retry can never
// overwrite the amount a crashed attempt already put through Stripe.
func (f *fakeStore) MarkBillingRunInvoicedIfUnfrozen(ctx context.Context, runID uuid.UUID) (bool, error) {
	// Mirrors the SQL guard: the terminal zero-mark loses to a concurrent freeze.
	if _, frozen := f.frozenCharges[runID]; frozen {
		return false, nil
	}
	if err := f.MarkBillingRun(ctx, runID, cycle.RunStatusInvoiced, "", 0); err != nil {
		return false, err
	}
	return true, nil
}

func (f *fakeStore) FreezeBillingRunCharge(_ context.Context, runID uuid.UUID, charge cycle.FrozenBoundaryCharge) (cycle.FrozenBoundaryCharge, error) {
	if f.errFreezeCharge != nil {
		return cycle.FrozenBoundaryCharge{}, f.errFreezeCharge
	}
	// onFreezeCharge, when set, runs BEFORE this process's write — modeling a
	// concurrent second daemon that reclaimed the same run and froze first (its
	// write lands in the race window between the caller's top-of-run frozen read
	// and this freeze). Used by the H6 regression test.
	if f.onFreezeCharge != nil {
		f.onFreezeCharge(runID)
	}
	// First-write-wins, returning the SURVIVING value (mirrors the SQL's
	// WHERE frozen_charge_cents IS NULL + read-back).
	if _, exists := f.frozenCharges[runID]; !exists {
		f.frozenCharges[runID] = charge
	}
	return f.frozenCharges[runID], nil
}

func (f *fakeStore) BillingRunFrozenCharge(_ context.Context, runID uuid.UUID) (cycle.FrozenBoundaryCharge, bool, error) {
	if f.errFrozenCharge != nil {
		return cycle.FrozenBoundaryCharge{}, false, f.errFrozenCharge
	}
	c, ok := f.frozenCharges[runID]
	return c, ok, nil
}

func (f *fakeStore) AccountsWithUsageEvents(_ context.Context, _, _ time.Time) ([]uuid.UUID, error) {
	if f.errUsageEvents != nil {
		return nil, f.errUsageEvents
	}
	return f.usageEventAccts, nil
}

func (f *fakeStore) AccountsWithUnbilledUsage(_ context.Context, _, _ time.Time) ([]uuid.UUID, error) {
	if f.errUnbilled != nil {
		return nil, f.errUnbilled
	}
	return f.unbilledAccounts, nil
}

func (f *fakeStore) ActivatedAccounts(_ context.Context) ([]cycle.AccountAnchor, error) {
	if f.errActivated != nil {
		return nil, f.errActivated
	}
	return f.activatedAccounts, nil
}

func (f *fakeStore) LatestClosedPeriodEnd(_ context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	if f.errLatestPeriod != nil {
		return time.Time{}, false, f.errLatestPeriod
	}
	end, ok := f.latestPeriodEnd[accountID]
	return end, ok, nil
}

// --- apps mirror fake (migration 027 / base-fee v1) -------------------------

// UsableNonFraudCardCount mirrors the pgxStore's reuse of the standing card
// predicate: an explicit cardCount override wins; otherwise the count derives
// from the usable-PM state (hasPMByAccount / hasPM → 1 card) so the existing
// fully-chargeable fixtures (registeredAccount and friends) stay FUNDED under
// the create gate without every test re-declaring a card.
func (f *fakeStore) UsableNonFraudCardCount(_ context.Context, accountID uuid.UUID) (int, error) {
	if f.errCardCount != nil {
		return 0, f.errCardCount
	}
	if n, ok := f.cardCount[accountID]; ok {
		return n, nil
	}
	if has, ok := f.hasPMByAccount[accountID]; ok {
		if has {
			return 1, nil
		}
		return 0, nil
	}
	if f.hasPM {
		return 1, nil
	}
	return 0, nil
}

func (f *fakeStore) AccountActivation(_ context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	if f.errActivation != nil {
		return time.Time{}, false, f.errActivation
	}
	at, ok := f.activation[accountID]
	return at, ok, nil
}

func (f *fakeStore) InsertAppMirror(_ context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount int, createdAt time.Time, name string) error {
	if f.errAppInsert != nil {
		return f.errAppInsert
	}
	if _, exists := f.apps[appID]; exists {
		return nil // ON CONFLICT (app_id) DO NOTHING — the FIRST registration wins
	}
	f.apps[appID] = cycle.AppMirror{
		AppID: appID, AccountID: accountID, ModuleCount: moduleCount,
		CreatedModuleCount: moduleCount, // frozen at insert, mirroring InsertAppMirror's $3/$3 write
		CreatedAt:          createdAt,
		Name:               name, // frozen on first registration (migration 037)
	}
	if ownerOrgID != uuid.Nil {
		f.appOwnerOrg[appID] = ownerOrgID // owner_org_id stamp (migration 041); Nil = user-owned (NULL)
	}
	return nil
}

// --- org account + designation fake (migration 041) -------------------------

func (f *fakeStore) EnsureOrgAccount(_ context.Context, orgID uuid.UUID) (uuid.UUID, error) {
	if id, ok := f.accountsByOrg[orgID]; ok {
		return id, nil // get-or-create: the same org always resolves the same account
	}
	id := uuid.New()
	f.accountsByOrg[orgID] = id
	return id, nil
}

func (f *fakeStore) AccountIDByUser(_ context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	id, ok := f.accountsByUser[userID]
	return id, ok, nil
}

func (f *fakeStore) OrgAccountID(_ context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	id, ok := f.accountsByOrg[orgID]
	return id, ok, nil
}

func (f *fakeStore) OrgDesignation(_ context.Context, orgID uuid.UUID) (cycle.OrgDesignation, bool, error) {
	d, ok := f.orgDesignations[orgID]
	return d, ok, nil
}

func (f *fakeStore) UpsertOrgDesignation(_ context.Context, d cycle.OrgDesignation) error {
	f.orgDesignations[d.OrgID] = d // re-designation overwrites in place
	return nil
}

func (f *fakeStore) DeleteOrgDesignation(_ context.Context, orgID uuid.UUID) (bool, error) {
	_, existed := f.orgDesignations[orgID]
	delete(f.orgDesignations, orgID)
	return existed, nil
}

func (f *fakeStore) ResolveOrgFundedAccount(_ context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	// Mirrors the SQL's single funded gate: a designation row exists AND the
	// org's account row is activated.
	if _, designated := f.orgDesignations[orgID]; !designated {
		return uuid.Nil, false, nil
	}
	acct, ok := f.accountsByOrg[orgID]
	if !ok {
		return uuid.Nil, false, nil
	}
	if _, activated := f.activation[acct]; !activated {
		return uuid.Nil, false, nil
	}
	return acct, true, nil
}

func (f *fakeStore) ActivateAccountIfUnset(_ context.Context, accountID uuid.UUID, at time.Time) error {
	if _, ok := f.activation[accountID]; !ok {
		f.activation[accountID] = at // the anchor is immutable once set (ADR 0006)
	}
	return nil
}

func (f *fakeStore) OrgUnbilledBacklogMicros(_ context.Context, orgID uuid.UUID) (int64, error) {
	return f.orgBacklog[orgID], nil
}

func (f *fakeStore) AttachOrgAppsToAccount(_ context.Context, orgID, accountID uuid.UUID) (int64, error) {
	// WHERE owner_org_id = $1 AND account_id IS NULL — attached rows never
	// match again (idempotent sweep).
	var n int64
	for id, app := range f.apps {
		if f.appOwnerOrg[id] == orgID && app.AccountID == uuid.Nil {
			app.AccountID = accountID
			f.apps[id] = app
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) RepointOrgNullAccountEvents(_ context.Context, orgID, accountID uuid.UUID, windowStart time.Time) (int64, error) {
	f.repointCalls = append(f.repointCalls, repointCall{orgID, accountID, windowStart})
	n := f.orgNullEvents[orgID]
	delete(f.orgNullEvents, orgID) // swept events never match again (account_id IS NULL)
	return n, nil
}

func (f *fakeStore) OrgLiveAppIDs(_ context.Context, orgID uuid.UUID) ([]uuid.UUID, error) {
	var out []uuid.UUID
	for id, app := range f.apps {
		if f.appOwnerOrg[id] == orgID && !app.Deleted {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

func (f *fakeStore) ListSponsoredOrgIDs(_ context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return f.sponsoredOrgs[userID], nil
}

func (f *fakeStore) ChargeFundingAccount(_ context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	// Identity, unless the account is an org account whose designation names a
	// sponsor — mirrors the SQL's LEFT JOIN + COALESCE (D1 funding hop).
	for orgID, acct := range f.accountsByOrg {
		if acct != accountID {
			continue
		}
		if d, ok := f.orgDesignations[orgID]; ok && d.Funding == cycle.OrgFundingSponsor {
			return d.SponsorAccountID, nil
		}
	}
	return accountID, nil
}

func (f *fakeStore) SetAppName(_ context.Context, appID uuid.UUID, name string) error {
	if app, ok := f.apps[appID]; ok && !app.Deleted { // no-op once deleted (WHERE deleted_at IS NULL)
		app.Name = name
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) AppMirror(_ context.Context, appID uuid.UUID) (cycle.AppMirror, bool, error) {
	if f.errAppMirror != nil {
		return cycle.AppMirror{}, false, f.errAppMirror
	}
	app, ok := f.apps[appID]
	return app, ok, nil
}

// --- custom-domain mirror fake (migration 047) -----------------------------

func (f *fakeStore) InsertDomain(_ context.Context, accountID, appID uuid.UUID, hostname string, activatedAt time.Time) error {
	if f.errDomainInsert != nil {
		return f.errDomainInsert
	}
	for _, d := range f.domains {
		if d.domain.Hostname == hostname && !d.domain.Removed {
			return nil // partial unique conflict: the existing live row wins
		}
	}
	id := uuid.New()
	f.domains[id] = &fakeDomain{domain: cycle.Domain{
		ID: id, AccountID: accountID, AppID: appID, Hostname: hostname, ActivatedAt: activatedAt,
	}}
	return nil
}

func (f *fakeStore) DomainByHostname(_ context.Context, hostname string) (cycle.Domain, bool, error) {
	if f.errDomainLookup != nil {
		return cycle.Domain{}, false, f.errDomainLookup
	}
	var best *fakeDomain
	for _, d := range f.domains {
		if d.domain.Hostname != hostname {
			continue
		}
		if best == nil || (!d.domain.Removed && best.domain.Removed) ||
			(d.domain.Removed == best.domain.Removed && d.domain.ActivatedAt.After(best.domain.ActivatedAt)) {
			best = d
		}
	}
	if best == nil {
		return cycle.Domain{}, false, nil
	}
	return best.domain, true, nil
}

func (f *fakeStore) RemoveDomain(_ context.Context, appID uuid.UUID, hostname string, removedAt time.Time) error {
	if f.errDomainRemove != nil {
		return f.errDomainRemove
	}
	for _, d := range f.domains {
		if d.domain.AppID == appID && d.domain.Hostname == hostname && !d.domain.Removed {
			d.domain.Removed = true
			d.domain.RemovedAt = removedAt
		}
	}
	return nil
}

func (f *fakeStore) DomainsPendingCharge(_ context.Context, activatedBefore time.Time) ([]cycle.DomainChargeCandidate, error) {
	if f.errDomainsPending != nil {
		return nil, f.errDomainsPending
	}
	var out []cycle.DomainChargeCandidate
	for _, d := range f.domains {
		if d.domain.Removed || d.chargeResolved || d.domain.ActivatedAt.After(activatedBefore) {
			continue
		}
		out = append(out, cycle.DomainChargeCandidate{
			ID:                 d.domain.ID,
			AccountID:          d.domain.AccountID,
			AppID:              d.domain.AppID,
			Hostname:           d.domain.Hostname,
			ActivatedAt:        d.domain.ActivatedAt,
			AccountActivatedAt: f.activation[d.domain.AccountID],
			ChargeAttemptedAt:  d.chargeAttemptedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ActivatedAt.Equal(out[j].ActivatedAt) {
			return out[i].ID.String() < out[j].ID.String()
		}
		return out[i].ActivatedAt.Before(out[j].ActivatedAt)
	})
	return out, nil
}

func (f *fakeStore) DomainStillPending(_ context.Context, domainID uuid.UUID) (bool, error) {
	if f.errDomainPending != nil {
		return false, f.errDomainPending
	}
	d, ok := f.domains[domainID]
	return ok && !d.domain.Removed && !d.chargeResolved, nil
}

func (f *fakeStore) MarkDomainChargeAttempted(_ context.Context, domainID uuid.UUID, at time.Time) error {
	if f.errDomainAttempted != nil {
		return f.errDomainAttempted
	}
	if d, ok := f.domains[domainID]; ok && d.chargeAttemptedAt.IsZero() {
		d.chargeAttemptedAt = at
	}
	return nil
}

func (f *fakeStore) MarkDomainChargeResolved(_ context.Context, domainID uuid.UUID) error {
	if f.errDomainResolved != nil {
		return f.errDomainResolved
	}
	if d, ok := f.domains[domainID]; ok {
		d.chargeResolved = true
	}
	return nil
}

func (f *fakeStore) MarkDomainCharged(_ context.Context, domainID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error {
	if f.errDomainCharged != nil {
		return f.errDomainCharged
	}
	if d, ok := f.domains[domainID]; ok {
		d.chargeResolved = true
		d.chargedAt = chargedAt
		d.chargeInvoiceID = invoiceID
		d.chargeInvoiceItemID = invoiceItemID
	}
	return nil
}

func (f *fakeStore) CountLiveDomainsActivatedBefore(_ context.Context, accountID uuid.UUID, activatedBefore time.Time) (int, error) {
	if f.errLiveDomainCount != nil {
		return 0, f.errLiveDomainCount
	}
	var count int
	for _, d := range f.domains {
		if d.domain.AccountID == accountID && !d.domain.Removed && d.domain.ActivatedAt.Before(activatedBefore) {
			count++
		}
	}
	return count, nil
}

func (f *fakeStore) SetAppProrationInvoice(_ context.Context, appID uuid.UUID, stripeInvoiceID string) error {
	if f.errSetProration != nil {
		return f.errSetProration
	}
	if app, ok := f.apps[appID]; ok && app.ProrationInvoiceID == "" {
		app.ProrationInvoiceID = stripeInvoiceID // first-charge-wins, like the WHERE … IS NULL
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) SetAppProrationSkipped(_ context.Context, appID uuid.UUID) error {
	if f.errSetSkipped != nil {
		return f.errSetSkipped
	}
	if app, ok := f.apps[appID]; ok && !app.ProrationSkipped && app.ProrationInvoiceID == "" {
		app.ProrationSkipped = true // first-write-wins, like the WHERE … IS NULL guard
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) SetAppModuleCount(_ context.Context, appID uuid.UUID, moduleCount int) error {
	if f.errSetCount != nil {
		return f.errSetCount
	}
	if app, ok := f.apps[appID]; ok && !app.Deleted {
		app.ModuleCount = moduleCount // deleted rows are frozen (WHERE deleted_at IS NULL)
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) MarkAppDeleted(_ context.Context, appID uuid.UUID) error {
	if f.errMarkDeleted != nil {
		return f.errMarkDeleted
	}
	if app, ok := f.apps[appID]; ok && !app.Deleted {
		app.Deleted = true
		app.DeletedAt = time.Now().UTC() // first deletion instant is kept
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) AppsPendingProration(_ context.Context, createdBefore time.Time) ([]uuid.UUID, error) {
	if f.errPendingProration != nil {
		return nil, f.errPendingProration
	}
	// Mirrors the SQL: created_at <= cutoff AND proration_invoice_id IS NULL AND
	// (deleted_at IS NULL OR deleted after the grace elapsed — D11: a survivor
	// still owes) AND proration_skipped_at IS NULL. Sorted by created_at for a
	// deterministic sweep order.
	var out []uuid.UUID
	for id, app := range f.apps {
		deletedInGrace := app.Deleted && app.DeletedAt.Before(app.CreatedAt.UTC().AddDate(0, 0, 3))
		if app.ProrationInvoiceID == "" && !deletedInGrace && !app.ProrationSkipped && !app.CreatedAt.After(createdBefore) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return f.apps[out[i]].CreatedAt.Before(f.apps[out[j]].CreatedAt)
	})
	return out, nil
}

// ChargeProrationLocked models the pgxStore's FOR UPDATE-locked charge section:
// it re-checks the terminal state (the fake's in-memory row is the "locked"
// read), invokes charge only when still chargeable, and persists the invoice +
// snapshot + arms the guard on a non-nil payload — exactly what the real tx does
// atomically.
func (f *fakeStore) ChargeProrationLocked(_ context.Context, appID uuid.UUID, charge func(cycle.AppMirror) (*cycle.ProrationCharge, error)) (cycle.ProrationOutcome, string, error) {
	if f.errChargeLocked != nil {
		return 0, "", f.errChargeLocked
	}
	app, ok := f.apps[appID]
	if !ok {
		return cycle.ProrationLockedNotFound, "", nil
	}
	if app.Deleted && app.DeletedAt.Before(app.CreatedAt.UTC().AddDate(0, 0, 3)) {
		return cycle.ProrationLockedDeleted, "", nil // deleted WITHIN grace only (D11)
	}
	if app.ProrationInvoiceID != "" {
		return cycle.ProrationLockedAlreadyCharged, app.ProrationInvoiceID, nil
	}
	pc, err := charge(app)
	if err != nil {
		return 0, "", err
	}
	if pc == nil {
		return cycle.ProrationLockedNoCharge, "", nil
	}
	// Phase 3 failure AFTER the callback's Stripe calls already succeeded: the
	// money moved, but the guard-arm + timer marks never commit. Models a
	// deadlock/transient tx error so a test can prove the co-created over-module
	// timers stay unresolved (and are NOT independently re-invoiced by Leg 1).
	if f.errPersistAfterStripe != nil {
		return 0, "", f.errPersistAfterStripe
	}
	f.invoices[pc.Invoice.StripeInvoiceID] = pc.Invoice
	f.baseSnapshots[snapKey{pc.Snapshot.AppID, pc.Snapshot.PeriodStart}] = fakeBaseSnapshot{snap: pc.Snapshot, source: "proration"}
	if pc.StraddleSnapshot != nil {
		f.baseSnapshots[snapKey{pc.StraddleSnapshot.AppID, pc.StraddleSnapshot.PeriodStart}] = fakeBaseSnapshot{snap: *pc.StraddleSnapshot, source: "proration"}
	}
	app.ProrationInvoiceID = pc.InvoiceID // first-charge-wins, like WHERE … IS NULL under the lock
	f.apps[appID] = app
	// Scenario 3 — mark the co-created over-module timers billed on this combined
	// invoice terminally charged, in the SAME "transaction" (first-write-wins on
	// grace_resolved, like the real MarkModuleTimerCharged WHERE grace_resolved = false).
	for _, tc := range pc.TimerCharges {
		if t, ok := f.timers[tc.TimerID]; ok && !t.graceResolved {
			t.graceResolved = true
			t.graceCharged = true
			t.graceChargedAt = tc.ChargedAt
			t.graceInvoiceID = tc.InvoiceID
			t.graceInvoiceItemID = tc.InvoiceItemID
		}
	}
	return cycle.ProrationLockedCharged, pc.InvoiceID, nil
}

// DrawCreationProrationFromWallet models the pgxStore's atomic wallet-settled
// creation proration (billing-engine #99): it re-checks the terminal state, then
// draws the amount from the wallet sources in the SAME consumption order as
// DrawWalletCredits (a credits account spends through zero into its unsecured
// remainder; a standard account that cannot fully cover draws NOTHING and returns
// ProrationWalletShort), and only on a full cover freezes the snapshot(s) and
// arms the guard — exactly what the real single tx does.
func (f *fakeStore) DrawCreationProrationFromWallet(_ context.Context, appID uuid.UUID, pc cycle.ProrationWalletCharge) (cycle.ProrationOutcome, string, error) {
	f.creationWalletDrawCalls++
	if f.errDrawCreationWallet != nil {
		return 0, "", f.errDrawCreationWallet
	}
	if pc.AmountMicros <= 0 {
		return cycle.ProrationLockedNoCharge, "", nil
	}
	if f.beforeCreationWalletDraw != nil {
		hook := f.beforeCreationWalletDraw
		f.beforeCreationWalletDraw = nil
		hook(f, appID)
	}
	app, ok := f.apps[appID]
	if !ok {
		return cycle.ProrationLockedNotFound, "", nil
	}
	if app.Deleted && app.DeletedAt.Before(app.CreatedAt.UTC().AddDate(0, 0, 3)) {
		return cycle.ProrationLockedDeleted, "", nil // deleted WITHIN grace only (D11)
	}
	if app.ProrationInvoiceID != "" {
		return cycle.ProrationLockedAlreadyCharged, app.ProrationInvoiceID, nil
	}
	if app.ProrationAttempted {
		return cycle.ProrationWalletDeferToStripe, "", nil
	}
	if len(f.creationWalletOutcomes) > 0 {
		outcome := f.creationWalletOutcomes[0]
		f.creationWalletOutcomes = f.creationWalletOutcomes[1:]
		if outcome != cycle.ProrationLockedCharged {
			return outcome, "", nil
		}
	}

	sources := make([]*fakeWalletSource, 0, len(f.walletSources))
	for _, source := range f.walletSources {
		if source.remaining > 0 && (source.expiresAt.IsZero() || source.expiresAt.After(time.Now())) {
			sources = append(sources, source)
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		a, b := sources[i], sources[j]
		tier := func(source *fakeWalletSource) int {
			switch {
			case source.typ == "grant" && !source.expiresAt.IsZero():
				return 0
			case source.typ == "grant", source.typ == "preallocation", source.typ == "refund", source.typ == "adjustment":
				return 1
			default:
				return 2
			}
		}
		if ta, tb := tier(a), tier(b); ta != tb {
			return ta < tb
		}
		if !a.expiresAt.Equal(b.expiresAt) {
			if a.expiresAt.IsZero() {
				return false
			}
			if b.expiresAt.IsZero() {
				return true
			}
			return a.expiresAt.Before(b.expiresAt)
		}
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		return a.id.String() < b.id.String()
	})

	// Standard mode cannot fully cover from its spendable lots → unsettled (no
	// draw, never Stripe). Credits mode always fully covers via the unsecured
	// remainder below.
	if f.walletMode == cycle.CreditBillingModeStandard {
		var available int64
		for _, source := range sources {
			available += source.remaining
		}
		if available < pc.AmountMicros {
			return cycle.ProrationWalletShort, "", nil
		}
	}

	left := pc.AmountMicros
	for _, source := range sources {
		if left == 0 {
			break
		}
		consume := source.remaining
		if consume > left {
			consume = left
		}
		source.remaining -= consume
		left -= consume
		f.creationDrawn[appID] += consume
		f.walletDrawOrder = append(f.walletDrawOrder, source.id)
	}
	if left > 0 {
		if f.walletMode != cycle.CreditBillingModeCredits {
			return cycle.ProrationWalletShort, "", nil
		}
		f.creationDrawn[appID] += left
		f.walletUnallocated += left
		left = 0
	}

	// Full cover — freeze the snapshot(s) and arm the guard.
	f.baseSnapshots[snapKey{pc.Snapshot.AppID, pc.Snapshot.PeriodStart}] = fakeBaseSnapshot{snap: pc.Snapshot, source: "proration"}
	if pc.StraddleSnapshot != nil {
		f.baseSnapshots[snapKey{pc.StraddleSnapshot.AppID, pc.StraddleSnapshot.PeriodStart}] = fakeBaseSnapshot{snap: *pc.StraddleSnapshot, source: "proration"}
	}
	app.ProrationInvoiceID = pc.Ref // first-charge-wins, like WHERE … IS NULL under the lock
	f.apps[appID] = app
	return cycle.ProrationLockedCharged, pc.Ref, nil
}

func (f *fakeStore) LiveAppsCreatedBefore(_ context.Context, accountID uuid.UUID, createdBefore time.Time, graceDays int) ([]cycle.AppModuleCount, error) {
	if f.errLiveCounts != nil {
		return nil, f.errLiveCounts
	}
	apps := []cycle.AppModuleCount{}
	for _, app := range f.apps {
		// Strictly-before cutoffs, mirroring the SQL: an app created ON or AFTER
		// the new period's start is excluded (its base is the proration leg's,
		// never the advance leg's), and so is an app whose creation grace had not
		// yet elapsed when the new period opened (H2 — it hasn't survived grace,
		// and its creation charge covers through the grace-elapsed period).
		if app.AccountID == accountID && !app.Deleted && app.CreatedAt.Before(createdBefore) &&
			app.CreatedAt.AddDate(0, 0, graceDays).Before(createdBefore) {
			apps = append(apps, cycle.AppModuleCount{AppID: app.AppID, ModuleCount: app.ModuleCount})
		}
	}
	return apps, nil
}

func (f *fakeStore) UpsertProrationBaseSnapshot(_ context.Context, snap cycle.AppBaseSnapshot) error {
	if f.errProrationSnap != nil {
		return f.errProrationSnap
	}
	// ON CONFLICT DO UPDATE: the proration row always wins / a retry rewrites
	// identical values.
	f.baseSnapshots[snapKey{snap.AppID, snap.PeriodStart}] = fakeBaseSnapshot{snap: snap, source: "proration"}
	return nil
}

func (f *fakeStore) InsertAdvanceBaseSnapshot(_ context.Context, snap cycle.AppBaseSnapshot) error {
	if f.errAdvanceSnap != nil {
		return f.errAdvanceSnap
	}
	// ON CONFLICT DO NOTHING: an existing row (proration, or a prior reclaimed
	// attempt's write) wins.
	k := snapKey{snap.AppID, snap.PeriodStart}
	if _, exists := f.baseSnapshots[k]; exists {
		return nil
	}
	f.baseSnapshots[k] = fakeBaseSnapshot{snap: snap, source: "advance"}
	return nil
}

// --- per-module install-timer fake (migration 033) --------------------------

func (f *fakeStore) LiveModuleTimerCountForApp(_ context.Context, appID uuid.UUID) (int, error) {
	if f.errLiveTimerCount != nil {
		return 0, f.errLiveTimerCount
	}
	n := 0
	for _, t := range f.timers {
		if t.appID == appID && !t.removed {
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) InsertModuleOverageTimers(_ context.Context, accountID, appID uuid.UUID, installedAt, graceExpiresAt time.Time, n int) error {
	if f.errInsertTimers != nil {
		return f.errInsertTimers
	}
	for i := 0; i < n; i++ {
		id := uuid.New()
		f.timers[id] = &fakeTimer{
			id:             id,
			accountID:      accountID,
			appID:          appID,
			installedAt:    installedAt,
			graceExpiresAt: graceExpiresAt,
		}
	}
	return nil
}

// liveTimersForApp returns the app's live timers sorted (installed_at DESC, id
// DESC) — the LIFO removal order.
func (f *fakeStore) liveTimersForApp(appID uuid.UUID) []*fakeTimer {
	var out []*fakeTimer
	for _, t := range f.timers {
		if t.appID == appID && !t.removed {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].installedAt.Equal(out[j].installedAt) {
			return out[i].installedAt.After(out[j].installedAt)
		}
		return out[i].id.String() > out[j].id.String()
	})
	return out
}

func (f *fakeStore) SoftRemoveNewestModuleTimers(_ context.Context, appID uuid.UUID, n int, removedAt time.Time) error {
	if f.errRemoveNewest != nil {
		return f.errRemoveNewest
	}
	live := f.liveTimersForApp(appID)
	for i := 0; i < n && i < len(live); i++ {
		live[i].removed = true
		live[i].removedAt = removedAt
	}
	return nil
}

func (f *fakeStore) ReconcileModuleTimersToTarget(ctx context.Context, appID uuid.UUID, installedAt, graceExpiresAt, removedAt time.Time) error {
	if f.errReconcileTimers != nil {
		return f.errReconcileTimers
	}
	// Mirrors the pgx locked reconcile (wave 2, D8/D9): the target, account, and
	// deleted state come from the CURRENT roster row — never the caller; a
	// deleted row reconciles to zero. (Unit tests are single-threaded; the
	// advisory-lock serialization itself is exercised by the integration test.)
	app, ok := f.apps[appID]
	if !ok {
		return nil
	}
	target := app.ModuleCount
	if app.Deleted {
		target = 0
	}
	live, err := f.LiveModuleTimerCountForApp(ctx, appID)
	if err != nil {
		return err
	}
	switch {
	case target > live:
		return f.InsertModuleOverageTimers(ctx, app.AccountID, appID, installedAt, graceExpiresAt, target-live)
	case target < live:
		return f.SoftRemoveNewestModuleTimers(ctx, appID, live-target, removedAt)
	}
	return nil
}

func (f *fakeStore) MarkAppDeletedAndRemoveTimers(ctx context.Context, appID uuid.UUID, removedAt time.Time) error {
	if f.errMarkDeleted != nil {
		return f.errMarkDeleted
	}
	// DeletedAt = the service clock's removedAt (the real SQL uses now() in the
	// same transaction) — NOT the test host's wall clock, which would misplace
	// the deletion relative to the fixtures' grace windows (D11 pivots on it).
	if app, ok := f.apps[appID]; ok && !app.Deleted {
		app.Deleted = true
		app.DeletedAt = removedAt
		f.apps[appID] = app
	}
	return f.SoftRemoveAllModuleTimersForApp(ctx, appID, removedAt)
}

func (f *fakeStore) SoftRemoveAllModuleTimersForApp(_ context.Context, appID uuid.UUID, removedAt time.Time) error {
	if f.errRemoveAllTimers != nil {
		return f.errRemoveAllTimers
	}
	for _, t := range f.timers {
		if t.appID == appID && !t.removed {
			t.removed = true
			t.removedAt = removedAt
		}
	}
	return nil
}

func (f *fakeStore) ModuleOverageTimersPastGrace(_ context.Context, at time.Time) ([]cycle.ModuleOverageCandidate, error) {
	if f.errTimersPastGrace != nil {
		return nil, f.errTimersPastGrace
	}
	var out []cycle.ModuleOverageCandidate
	for _, t := range f.timers {
		if t.removed || t.graceResolved || t.graceExpiresAt.After(at) {
			continue
		}
		activatedAt, activated := f.activation[t.accountID]
		if !activated {
			continue // activated_at IS NOT NULL gate
		}
		out = append(out, cycle.ModuleOverageCandidate{
			ID:                t.id,
			AccountID:         t.accountID,
			AppID:             t.appID,
			InstalledAt:       t.installedAt,
			GraceExpiresAt:    t.graceExpiresAt,
			ChargeAttemptedAt: t.chargeAttemptedAt,
			ActivatedAt:       activatedAt,
		})
	}
	// Ordered (installed_at, id) like the query, so the sweep charges oldest-first
	// deterministically.
	sort.Slice(out, func(i, j int) bool {
		if !out[i].InstalledAt.Equal(out[j].InstalledAt) {
			return out[i].InstalledAt.Before(out[j].InstalledAt)
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	return out, nil
}

func (f *fakeStore) LiveModuleTimerRankBefore(_ context.Context, accountID, timerID uuid.UUID, installedAt time.Time) (int, error) {
	if f.errTimerRank != nil {
		return 0, f.errTimerRank
	}
	rank := 0
	for _, t := range f.timers {
		if t.accountID != accountID || t.removed {
			continue
		}
		if t.installedAt.Before(installedAt) ||
			(t.installedAt.Equal(installedAt) && t.id.String() < timerID.String()) {
			rank++
		}
	}
	return rank, nil
}

func (f *fakeStore) MarkModuleTimerChargeAttempted(_ context.Context, timerID uuid.UUID, at time.Time) (int64, error) {
	// Mirrors the SQL: UPDATE ... SET charge_attempted_at = COALESCE(charge_attempted_at, $2)
	// WHERE id = $1 AND grace_resolved = false. A timer already resolved (e.g. by a
	// concurrent wallet settlement) matches 0 rows → the Stripe leg aborts stale
	// (Job 3 hardening). An unresolved timer matches 1 row whether or not the marker
	// was already set (COALESCE keeps the first-write instant), so a crash-recovery
	// retry still re-charges.
	t, ok := f.timers[timerID]
	if !ok || t.graceResolved {
		return 0, nil
	}
	if t.chargeAttemptedAt.IsZero() {
		t.chargeAttemptedAt = at // first-write-wins, mirroring COALESCE
	}
	return 1, nil
}

func (f *fakeStore) ModuleTimerStillPending(_ context.Context, timerID uuid.UUID) (bool, error) {
	t, ok := f.timers[timerID]
	if !ok {
		return false, nil
	}
	return !t.removed && !t.graceResolved, nil
}

func (f *fakeStore) MarkAppProrationAttempted(_ context.Context, appID uuid.UUID, _ time.Time) error {
	if app, ok := f.apps[appID]; ok && !app.ProrationAttempted {
		app.ProrationAttempted = true // first-write-wins
		f.apps[appID] = app
	}
	return nil
}

func (f *fakeStore) MarkModuleTimerIncluded(_ context.Context, timerID uuid.UUID) error {
	if f.errMarkIncluded != nil {
		return f.errMarkIncluded
	}
	if t, ok := f.timers[timerID]; ok && !t.graceResolved {
		t.graceResolved = true
	}
	return nil
}

func (f *fakeStore) MarkModuleTimerCharged(_ context.Context, timerID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error {
	if f.errMarkTimerCharged != nil {
		return f.errMarkTimerCharged
	}
	if t, ok := f.timers[timerID]; ok && !t.graceResolved {
		t.graceResolved = true
		t.graceCharged = true
		t.graceChargedAt = chargedAt
		t.graceInvoiceID = invoiceID
		t.graceInvoiceItemID = invoiceItemID
	}
	return nil
}

// DrawModuleOverageFromWallet models the pgxStore's atomic wallet-settled module
// overage (billing-engine Job 3, mirrors the DrawCreationProrationFromWallet fake):
// it re-checks the terminal timer state UNDER the "lock", then draws from the wallet
// sources in the SAME consumption order (a credits account spends through zero into
// its unsecured remainder; a standard account that cannot fully cover draws NOTHING
// and returns ModuleOverageWalletShort), and only on a full cover arms the SAME
// per-timer guard the Stripe leg arms — exactly what the real single tx does.
func (f *fakeStore) DrawModuleOverageFromWallet(_ context.Context, timerID uuid.UUID, mc cycle.ModuleOverageWalletCharge) (cycle.ModuleOverageWalletOutcome, string, error) {
	f.moduleOverageDrawCalls++
	if f.errDrawModuleOverageWallet != nil {
		return 0, "", f.errDrawModuleOverageWallet
	}
	if mc.AmountMicros <= 0 {
		return cycle.ModuleOverageWalletLockedNoCharge, "", nil
	}
	if f.beforeModuleOverageDraw != nil {
		hook := f.beforeModuleOverageDraw
		f.beforeModuleOverageDraw = nil
		hook(f, timerID)
	}
	t, ok := f.timers[timerID]
	if !ok || t.removed || t.graceResolved {
		return cycle.ModuleOverageWalletLockedStale, "", nil
	}
	if !t.chargeAttemptedAt.IsZero() {
		return cycle.ModuleOverageWalletDeferToStripe, "", nil
	}
	if len(f.moduleOverageWalletOutcomes) > 0 {
		outcome := f.moduleOverageWalletOutcomes[0]
		f.moduleOverageWalletOutcomes = f.moduleOverageWalletOutcomes[1:]
		if outcome != cycle.ModuleOverageWalletLockedCharged {
			return outcome, "", nil
		}
	}

	sources := make([]*fakeWalletSource, 0, len(f.walletSources))
	for _, source := range f.walletSources {
		if source.remaining > 0 && (source.expiresAt.IsZero() || source.expiresAt.After(time.Now())) {
			sources = append(sources, source)
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		a, b := sources[i], sources[j]
		tier := func(source *fakeWalletSource) int {
			switch {
			case source.typ == "grant" && !source.expiresAt.IsZero():
				return 0
			case source.typ == "grant", source.typ == "preallocation", source.typ == "refund", source.typ == "adjustment":
				return 1
			default:
				return 2
			}
		}
		if ta, tb := tier(a), tier(b); ta != tb {
			return ta < tb
		}
		if !a.expiresAt.Equal(b.expiresAt) {
			if a.expiresAt.IsZero() {
				return false
			}
			if b.expiresAt.IsZero() {
				return true
			}
			return a.expiresAt.Before(b.expiresAt)
		}
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		return a.id.String() < b.id.String()
	})

	// Standard mode cannot fully cover from its spendable lots → unsettled (no
	// draw, never Stripe). Credits mode always fully covers via the unsecured
	// remainder below.
	if f.walletMode == cycle.CreditBillingModeStandard {
		var available int64
		for _, source := range sources {
			available += source.remaining
		}
		if available < mc.AmountMicros {
			return cycle.ModuleOverageWalletShort, "", nil
		}
	}

	left := mc.AmountMicros
	for _, source := range sources {
		if left == 0 {
			break
		}
		consume := source.remaining
		if consume > left {
			consume = left
		}
		source.remaining -= consume
		left -= consume
		f.moduleOverageDrawn[timerID] += consume
		f.walletDrawOrder = append(f.walletDrawOrder, source.id)
	}
	if left > 0 {
		if f.walletMode != cycle.CreditBillingModeCredits {
			return cycle.ModuleOverageWalletShort, "", nil
		}
		f.moduleOverageDrawn[timerID] += left
		f.walletUnallocated += left
		left = 0
	}

	// Full cover — arm the per-timer guard (first-write-wins on grace_resolved,
	// like the real MarkModuleTimerCharged WHERE grace_resolved = false).
	t.graceResolved = true
	t.graceCharged = true
	t.graceChargedAt = mc.ChargedAt
	t.graceInvoiceID = mc.Ref
	t.graceInvoiceItemID = ""
	return cycle.ModuleOverageWalletLockedCharged, mc.Ref, nil
}

// liveTimersForAccountFIFO returns the account's live timers ordered (installed_at
// ASC, id ASC) — the FIFO order the rank/over predicates read.
func (f *fakeStore) liveTimersForAccountFIFO(accountID uuid.UUID) []*fakeTimer {
	var out []*fakeTimer
	for _, t := range f.timers {
		if t.accountID == accountID && !t.removed {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].installedAt.Equal(out[j].installedAt) {
			return out[i].installedAt.Before(out[j].installedAt)
		}
		return out[i].id.String() < out[j].id.String()
	})
	return out
}

func (f *fakeStore) CountOngoingOverModuleTimers(_ context.Context, accountID uuid.UUID, includedModules int, periodEnd time.Time) (int, error) {
	if f.errCountOngoingOver != nil {
		return 0, f.errCountOngoingOver
	}
	// Live FIFO: the first includedModules are "included"; the rest are "over".
	// Count the "over" tail owed a precharge for the new period opening at
	// periodEnd: installed before it, grace elapsed before it — IMMUTABLE
	// cutoffs only (wave 2, D1: resolution state is sweep-ordering-dependent
	// and deliberately not part of the predicate), mirroring the SQL.
	n := 0
	for rank, t := range f.liveTimersForAccountFIFO(accountID) {
		if rank >= includedModules &&
			t.installedAt.Before(periodEnd) && t.graceExpiresAt.Before(periodEnd) {
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) CoCreatedOverModuleTimers(_ context.Context, accountID, appID uuid.UUID, createdAt time.Time, includedModules int) ([]uuid.UUID, error) {
	if f.errCoCreatedOver != nil {
		return nil, f.errCoCreatedOver
	}
	var out []uuid.UUID
	for rank, t := range f.liveTimersForAccountFIFO(accountID) {
		if rank >= includedModules && t.appID == appID && !t.graceResolved && t.installedAt.Equal(createdAt) {
			out = append(out, t.id)
		}
	}
	return out, nil
}

// --- helpers --------------------------------------------------------------

func requireCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "want *billing.Error, got %T", err)
	require.Equal(t, want, be.Code)
}

var (
	periodStart = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	periodEnd   = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
)

func rawAgg(app, mod uuid.UUID, metric string, kind cycle.Kind, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{AppID: app, ModuleID: mod, Metric: metric, Kind: kind, BillableQuantity: qty}
}

// rawAggModel is rawAgg with the AI pricing-dimension model set (migration 018).
func rawAggModel(app, mod uuid.UUID, metric string, kind cycle.Kind, model, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{AppID: app, ModuleID: mod, Metric: metric, Kind: kind, Model: model, BillableQuantity: qty}
}

// rawAggVersion is rawAgg with the version-attribution dimension set
// (migration 023). Unlike model, version never selects a different price —
// it is carried straight through onto the aggregate for reporting only.
func rawAggVersion(app, mod uuid.UUID, metric string, kind cycle.Kind, version, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{AppID: app, ModuleID: mod, Metric: metric, Kind: kind, ModuleVersion: version, BillableQuantity: qty}
}

// --- RollupPeriod: pricing + aggregation ----------------------------------

func TestRollupPeriod_CustomMetricNoMarkup(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000 // $0.05/unit

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)

	a := resp.Aggregates[0]
	require.Equal(t, 10, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 500_000, a.RawCostMicros) // 10 × 50_000
	require.EqualValues(t, 500_000, a.ChargedMicros) // no markup → charged == raw
	require.EqualValues(t, 500_000, resp.TotalChargedMicros)
}

func TestRollupPeriod_ReservedMetricInfraMarkup(t *testing.T) {
	// A reserved infra.* / platform.* name takes the 12/10 (1.2×) markup plane.
	// As of PR #10a the platform-infra ingest (RecordInfraUsage) records these,
	// so they DO reach the rollup; the plane logic prices them at cost × 1.2.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "infra.compute.ms", usage.KindSum, "100")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 1_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 100_000, a.RawCostMicros) // 100 × 1_000
	require.EqualValues(t, 120_000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_InfraEgressUnderSentinelPricesAt12Over10(t *testing.T) {
	// PR #10a foundation contract: an infra.egress.bytes event recorded by
	// RecordInfraUsage is stamped under the platform-infra SENTINEL module_id
	// (usage.PlatformInfraModuleID()); migration 017 seeds the matching
	// metric_definitions row under the SAME sentinel with the per-unit COGS, so
	// the rollup's price-lookup resolves a non-zero cost and the reserved-name
	// branch marks it up cost × 12/10. This proves an infra event prices at
	// 12/10 (NOT 10/10) end-to-end through the sentinel.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.egress.bytes", usage.KindSum, "1000")}
	store.prices[priceKey(sentinel, "infra.egress.bytes")] = 2 // seeded per-byte COGS (micros)

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, sentinel, a.ModuleID)
	require.Equal(t, 12, a.MarkupNum) // reserved-name markup plane, NOT 10/10
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 2_000, a.RawCostMicros) // 1000 × 2
	require.EqualValues(t, 2_400, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_WalltimeMSPricesAt12Over10(t *testing.T) {
	// Catalog hygiene (migration 019): the re-chartered infra.compute.walltime.ms
	// is a reserved infra.* name, so it takes the 12/10 (1.2×) platform-infra
	// markup plane exactly like the old infra.compute.ms did, pricing from its
	// migration-019 sentinel row (the renamed 017 seed, placeholder 1 µ$/ms).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.compute.walltime.ms", usage.KindSum, "100")}
	store.prices[priceKey(sentinel, "infra.compute.walltime.ms")] = 1 // re-chartered placeholder COGS

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, sentinel, a.ModuleID)
	require.Equal(t, 12, a.MarkupNum) // reserved-name markup plane
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 100, a.RawCostMicros) // 100 × 1
	require.EqualValues(t, 120, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_EgressBytesRetiredZeroPriceNoLoudFail(t *testing.T) {
	// Catalog hygiene (migration 019): infra.egress.bytes is RETIRED to an
	// unpriced reporting parent via an explicit price=0 (NOT NULL). Because the
	// metric is still INGESTED (cmd/infra-egress-sync), its events reach the
	// rollup. A NULL price would set priced=false and trip the reserved-metric
	// loud-fail in service.go; an explicit 0 sets priced=true → charged=0 and the
	// cycle SUCCEEDS. The fake store models the price=0 catalog row as a present
	// (ok=true) entry, exactly like the seeded row.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.egress.bytes", usage.KindSum, "1000000")}
	store.prices[priceKey(sentinel, "infra.egress.bytes")] = 0 // retired: present-but-zero, NOT absent/NULL

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err, "a deliberately-unpriced (price=0) retired metric must roll up to 0, not loud-fail")

	a := resp.Aggregates[0]
	require.Equal(t, sentinel, a.ModuleID)
	require.Equal(t, 12, a.MarkupNum) // still on the reserved markup plane
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros) // 1_000_000 × 0 × 1.2 = 0
	require.EqualValues(t, 0, resp.TotalChargedMicros)
}

func TestRollupPeriod_PlatformReservedPrefix(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "platform.tokens", usage.KindSum, "5")}
	store.prices[priceKey(mod, "platform.tokens")] = 2_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Equal(t, 12, resp.Aggregates[0].MarkupNum)
	require.EqualValues(t, 12_000, resp.Aggregates[0].ChargedMicros) // 5×2_000×1.2
}

func TestRollupPeriod_NullPriceZeroCharge(t *testing.T) {
	// A metered-but-unpriced metric (no catalog price) prices to 0.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindCount, "42")}
	// no price registered → unpriced

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
}

func TestRollupPeriod_InfraMissingCatalogErrors(t *testing.T) {
	// A reserved infra metric with NO seeded price (migration 017 missing or
	// rolled back) MUST fail the cycle loudly — NOT silently price to 0 like an
	// unpriced custom metric. This guards the infra revenue-leak path: the
	// platform incurred the cloud COGS, so a zero charge is never acceptable.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.compute.ms", usage.KindSum, "100")}
	// deliberately register NO price for the infra metric

	_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	requireCode(t, err, billing.CodeInternal)
}

func TestRollupPeriod_FractionalQuantityRoundHalfUp(t *testing.T) {
	// A time-weighted integral can be fractional (byte-hours). raw_cost =
	// round_half_up(quantity × unit_price). 2.5 × 3 = 7.5 → 8 (half-up).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "myapp.objects.byte_hours", usage.KindTimeWeighted, "2.5")}
	store.prices[priceKey(mod, "myapp.objects.byte_hours")] = 3

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 8, resp.Aggregates[0].RawCostMicros)
	require.EqualValues(t, 8, resp.Aggregates[0].ChargedMicros)
}

func TestRollupPeriod_HalfUpExactBoundary(t *testing.T) {
	// Exactly .5 rounds UP deterministically. 0.5 × 1 = 0.5 → 1.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "m", usage.KindTimeWeighted, "0.5")}
	store.prices[priceKey(mod, "m")] = 1

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Aggregates[0].RawCostMicros)
}

func TestRollupPeriod_InfraMarkupSingleRound(t *testing.T) {
	// B1 regression: the 12/10 markup must round ONCE over the whole product,
	// not twice (round raw_cost, then round raw_cost×12/10). For qty=0.1,
	// price=13: single-pass charged = round_half_up(0.1×13×1.2) =
	// round_half_up(1.56) = 2. A two-step path gives round_half_up(1.3)=1 then
	// round_half_up(1.2)=1 — under-billing by 1 micro.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "infra.compute.ms", usage.KindTimeWeighted, "0.1")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 13

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 1, a.RawCostMicros) // round_half_up(0.1×13)=round(1.3)=1
	require.EqualValues(t, 2, a.ChargedMicros) // round_half_up(0.1×13×12/10)=round(1.56)=2
}

func TestRollupPeriod_OverflowRejected(t *testing.T) {
	// B2 regression: a quantity × price that exceeds int64 micros must error,
	// not silently wrap to a wrong (possibly negative) charge. 1e12 × 50_000_000
	// = 5e19 > int64 max (~9.22e18).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "1000000000000")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000_000

	_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	requireCode(t, err, billing.CodeInternal)
}

func TestRollupPeriod_KindsCarriedThrough(t *testing.T) {
	// Each aggregate snapshots the kind it rolled up under.
	store := newFakeStore()
	app := uuid.New()
	m1, m2, m3 := uuid.New(), uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{
		rawAgg(app, m1, "a", usage.KindCount, "3"),
		rawAgg(app, m2, "b", usage.KindPeak, "9"),
		rawAgg(app, m3, "c", usage.KindTimeWeighted, "4"),
	}
	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	kinds := map[string]cycle.Kind{}
	for _, a := range resp.Aggregates {
		kinds[a.Metric] = a.Kind
	}
	require.Equal(t, usage.KindCount, kinds["a"])
	require.Equal(t, usage.KindPeak, kinds["b"])
	require.Equal(t, usage.KindTimeWeighted, kinds["c"])
}

func TestRollupPeriod_NoEventsEmpty(t *testing.T) {
	store := newFakeStore() // no raws (no-sample period → 0 aggregates)
	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Empty(t, resp.Aggregates)
	require.EqualValues(t, 0, resp.TotalChargedMicros)
	require.Empty(t, store.aggregates)
}

// --- RollupPeriod: idempotency --------------------------------------------

func TestRollupPeriod_IdempotentReRun(t *testing.T) {
	// Re-running the same period upserts the IDENTICAL aggregate, never a
	// duplicate (the fake keys on (period, app, module, metric) like the DB
	// UNIQUE constraint).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000
	acct := uuid.New()
	svc := cycle.NewService(store, nil)

	first, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)
	second, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)

	require.Len(t, store.aggregates, 1, "re-run upserts, never duplicates")
	require.Equal(t, first.Aggregates[0].ChargedMicros, second.Aggregates[0].ChargedMicros)
	require.Equal(t, first.TotalChargedMicros, second.TotalChargedMicros)
}

// --- RollupPeriod: validation + error propagation -------------------------

func TestRollupPeriod_RejectsNilAccount(t *testing.T) {
	_, err := cycle.NewService(newFakeStore(), nil).RollupPeriod(context.Background(), uuid.Nil, periodStart, periodEnd)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestRollupPeriod_RejectsBadWindow(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end time.Time
	}{
		{"zero start", time.Time{}, periodEnd},
		{"zero end", periodStart, time.Time{}},
		{"end before start", periodEnd, periodStart},
		{"equal", periodStart, periodStart},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cycle.NewService(newFakeStore(), nil).RollupPeriod(context.Background(), uuid.New(), tc.start, tc.end)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

func TestRollupPeriod_PropagatesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"open", func(f *fakeStore) { f.errOpen = boom }},
		{"raw", func(f *fakeStore) { f.errRaw = boom }},
		{"price", func(f *fakeStore) {
			f.raws = []cycle.RawAggregate{rawAgg(uuid.New(), uuid.New(), "m", usage.KindSum, "1")}
			f.errPrice = boom
		}},
		{"upsert", func(f *fakeStore) {
			f.raws = []cycle.RawAggregate{rawAgg(uuid.New(), uuid.New(), "m", usage.KindSum, "1")}
			f.errUpsert = boom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			tc.setup(store)
			_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}

// --- SettleDevelopers: margin-share math ----------------------------------

func TestSettleDevelopers_PrivateThirtyPercent(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	require.Len(t, resp.Settlements, 1)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPrivate, s.MarginShareClass)
	require.EqualValues(t, 300_000, s.PlatformTakeMicros)  // 30% of 1_000_000
	require.EqualValues(t, 700_000, s.DeveloperOwedMicros) // remainder
	require.EqualValues(t, 0, s.InfraMicros)
}

func TestSettleDevelopers_PublishedFifteenPercent(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPublished

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPublished, s.MarginShareClass)
	require.EqualValues(t, 150_000, s.PlatformTakeMicros) // 15%
	require.EqualValues(t, 850_000, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_UnknownVisibilityDefaultsPrivate(t *testing.T) {
	// No visibility row → default to private (30%, the higher take) so the
	// platform never under-collects on a lagging publish.
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	// no visibility registered

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPrivate, s.MarginShareClass)
	require.EqualValues(t, 300_000, s.PlatformTakeMicros)
}

func TestSettleDevelopers_ZeroIncomeZeroOwed(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 0}}
	store.visibility[mod] = usage.VisibilityPublished

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.EqualValues(t, 0, s.PlatformTakeMicros)
	require.EqualValues(t, 0, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_RoundHalfUpTake(t *testing.T) {
	// 30% of 5 = 1.5 → take rounds half-up to 2; owed = 5 − 2 = 3.
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 5}}
	store.visibility[mod] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.EqualValues(t, 2, s.PlatformTakeMicros)
	require.EqualValues(t, 3, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_TakePlusOwedEqualsIncome(t *testing.T) {
	// Invariant: with infra=0, take + owed == income exactly (no money lost).
	store := newFakeStore()
	for _, income := range []int64{1, 7, 333_333, 1_000_001, 999_999_999} {
		mod := uuid.New()
		store.incomes = append(store.incomes, cycle.ModuleIncome{ModuleID: mod, IncomeMicros: income})
		store.visibility[mod] = usage.VisibilityPrivate
	}
	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	for _, s := range resp.Settlements {
		require.Equal(t, s.IncomeMicros, s.PlatformTakeMicros+s.DeveloperOwedMicros,
			"take + owed must equal income for module %s", s.ModuleID)
	}
}

func TestSettleDevelopers_MultipleModules(t *testing.T) {
	store := newFakeStore()
	mPub, mPriv := uuid.New(), uuid.New()
	store.incomes = []cycle.ModuleIncome{
		{ModuleID: mPub, IncomeMicros: 1_000_000},
		{ModuleID: mPriv, IncomeMicros: 1_000_000},
	}
	store.visibility[mPub] = usage.VisibilityPublished
	store.visibility[mPriv] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	require.Len(t, store.settlements, 2)
	byMod := map[uuid.UUID]cycle.ModuleSettlement{}
	for _, s := range resp.Settlements {
		byMod[s.ModuleID] = s
	}
	require.EqualValues(t, 150_000, byMod[mPub].PlatformTakeMicros)
	require.EqualValues(t, 300_000, byMod[mPriv].PlatformTakeMicros)
}

func TestSettleDevelopers_IdempotentReRun(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPrivate
	acct := uuid.New()
	svc := cycle.NewService(store, nil)

	_, err := svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	_, err = svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	require.Len(t, store.settlements, 1, "re-run upserts, never duplicates")
}

func TestSettleDevelopers_Validation(t *testing.T) {
	_, err := cycle.NewService(newFakeStore(), nil).SettleDevelopers(context.Background(), uuid.Nil, uuid.New())
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = cycle.NewService(newFakeStore(), nil).SettleDevelopers(context.Background(), uuid.New(), uuid.Nil)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSettleDevelopers_PropagatesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"income", func(f *fakeStore) { f.errIncome = boom }},
		{"visibility", func(f *fakeStore) {
			f.incomes = []cycle.ModuleIncome{{ModuleID: uuid.New(), IncomeMicros: 1}}
			f.errVis = boom
		}},
		{"settle", func(f *fakeStore) {
			f.incomes = []cycle.ModuleIncome{{ModuleID: uuid.New(), IncomeMicros: 1}}
			f.errSettle = boom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			tc.setup(store)
			_, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}

// --- end-to-end: rollup feeds settlement ----------------------------------

func TestRollupThenSettle_IncomeFromAggregates(t *testing.T) {
	// The realistic flow: RollupPeriod writes aggregates, then SettleDevelopers
	// reads the per-module charged income from them. Here we wire the fake's
	// income to mirror what the rollup charged for the module.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "20")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000
	acct := uuid.New()
	svc := cycle.NewService(store, nil)

	roll, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 1_000_000, roll.TotalChargedMicros) // 20 × 50_000

	// Feed the rolled income (as the DB ModuleIncome query would) and settle.
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: roll.TotalChargedMicros}}
	store.visibility[mod] = usage.VisibilityPublished
	set, err := svc.SettleDevelopers(context.Background(), acct, roll.PeriodID)
	require.NoError(t, err)
	require.EqualValues(t, 150_000, set.Settlements[0].PlatformTakeMicros)
	require.EqualValues(t, 850_000, set.Settlements[0].DeveloperOwedMicros)
}

// --- RollupPeriod: per-model AI token pricing (migration 018) --------------

// The roster model ids the producer (infra-metrics PR #2) stamps on AI events.
const (
	modelHaiku  = "anthropic.claude-haiku-4-5-20251001-v1:0"
	modelSonnet = "anthropic.claude-sonnet-4-6"
)

func TestRollupPeriod_AIInputTokensPerModelPrice(t *testing.T) {
	// An infra.ai.input.tokens event carrying a model is priced from the PER-MODEL
	// side-table (metric_model_prices), NOT the sentinel metric_definitions
	// fallback. Sonnet input = 3000 µ$/1k; the catalog fallback is the cheaper
	// Haiku rate (1000) — proving the per-model price is what resolves. Quantity
	// is in 1k-token units (design §3 rule 5). 2 (×1k) × 3000 × 12/10 = 7200.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelSonnet, "2")}
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000 // cheaper catalog fallback, must NOT win

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 3000, a.UnitPriceMicros, "per-model price must win over the catalog fallback")
	require.Equal(t, 12, a.MarkupNum) // infra.* → 12/10 plane unchanged
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 6000, a.RawCostMicros) // 2 × 3000
	require.EqualValues(t, 7200, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_AIInputTokensDistinctPerModel(t *testing.T) {
	// Two models on the same metric resolve to DIFFERENT prices in one rollup —
	// the whole point of the side-table. Haiku in = 1000, Sonnet in = 3000.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelHaiku, "1"),
		rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelSonnet, "1"),
	}
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelHaiku)] = 1000
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	byModel := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byModel[a.Model] = a
	}
	require.EqualValues(t, 1000, byModel[modelHaiku].UnitPriceMicros)
	require.EqualValues(t, 3000, byModel[modelSonnet].UnitPriceMicros)
	require.EqualValues(t, 1200, byModel[modelHaiku].ChargedMicros)  // 1×1000×1.2
	require.EqualValues(t, 3600, byModel[modelSonnet].ChargedMicros) // 1×3000×1.2
}

func TestRollupPeriod_AITokensFallbackToDefinitionWhenNoModelPrice(t *testing.T) {
	// A model with NO per-model price row falls back to the catalog (sentinel
	// metric_definitions) fallback price — it does NOT zero-charge. Fallback =
	// 1000 µ$/1k. 3 × 1000 × 1.2 = 3600.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, "some-future-model", "3")}
	// no modelPrices row for "some-future-model"
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000 // catalog fallback

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 1000, a.UnitPriceMicros, "missing per-model price falls back to the catalog row")
	require.EqualValues(t, 3600, a.ChargedMicros)
}

func TestRollupPeriod_RetiredModelPriceFailsLoud(t *testing.T) {
	// A per-model price that EXISTS but was RETIRED (active=false) must NOT
	// silently fall back to the cheaper catalog floor and under-bill — it fails
	// the cycle loud. This is the asymmetry the money review flagged: a MISSING
	// row falls back (legitimate unpriced-model), a RETIRED row fails. Here Sonnet
	// input is retired and the cheaper Haiku-floor catalog price (1000) is present
	// as the would-be silent fallback; the rollup must error instead of using it.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelSonnet, "2")}
	store.inactiveModelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = true
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000 // would-be silent fallback — must NOT be used

	_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.Error(t, err, "a retired per-model price must fail the cycle, not silently under-bill")
	require.Contains(t, err.Error(), "RETIRED")
}

func TestRollupPeriod_InfraAINoModelUsesDefinition(t *testing.T) {
	// A model-less AI event (model == "") resolves straight from the catalog
	// fallback, never the per-model table. Even with a Sonnet per-model price
	// present, an empty-model row must use the catalog fallback (1000).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.ai.input.tokens", usage.KindSum, "4")} // model ""
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, "", a.Model)
	require.EqualValues(t, 1000, a.UnitPriceMicros)
	require.EqualValues(t, 4800, a.ChargedMicros) // 4×1000×1.2
}

func TestRollupPeriod_AITokensMarkupIs12Over10(t *testing.T) {
	// AI token metrics are infra.* → they take the reserved 12/10 markup plane,
	// unchanged by the per-model price source.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.output.tokens", usage.KindSum, modelSonnet, "10")}
	store.modelPrices[modelPriceKey("infra.ai.output.tokens", modelSonnet)] = 15000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 150_000, a.RawCostMicros) // 10 × 15000
	require.EqualValues(t, 180_000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_CacheWriteVsCacheReadPriceDifference(t *testing.T) {
	// The cache-class split is the whole reason cache_write/cache_read are
	// separate metrics: write ≈ 1.25× input, read ≈ 0.1× input — pricing read as
	// input over-bills ~10×. Sonnet cache_write = 3750, cache_read = 300. They
	// resolve to distinct per-model prices in one rollup.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		rawAggModel(app, sentinel, "infra.ai.cache_write.tokens", usage.KindSum, modelSonnet, "2"),
		rawAggModel(app, sentinel, "infra.ai.cache_read.tokens", usage.KindSum, modelSonnet, "2"),
	}
	store.modelPrices[modelPriceKey("infra.ai.cache_write.tokens", modelSonnet)] = 3750
	store.modelPrices[modelPriceKey("infra.ai.cache_read.tokens", modelSonnet)] = 300

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	byMetric := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byMetric[a.Metric] = a
	}
	require.EqualValues(t, 3750, byMetric["infra.ai.cache_write.tokens"].UnitPriceMicros)
	require.EqualValues(t, 300, byMetric["infra.ai.cache_read.tokens"].UnitPriceMicros)
	require.EqualValues(t, 9000, byMetric["infra.ai.cache_write.tokens"].ChargedMicros) // 2×3750×1.2
	require.EqualValues(t, 720, byMetric["infra.ai.cache_read.tokens"].ChargedMicros)   // 2×300×1.2
}

func TestRollupPeriod_AIRequestsZeroCharge(t *testing.T) {
	// infra.ai.requests is unpriced observability (price 0, no per-model rows):
	// it charges 0 regardless of the 12/10 plane. It still aggregates (the count
	// is retained for rate/abuse signal) but never bills.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.requests", usage.KindCount, modelSonnet, "5")}
	store.prices[priceKey(sentinel, "infra.ai.requests")] = 0 // seeded price 0 (observability)

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
}

// --- module_version attribution dimension (migration 023) ----------------

func TestRollupPeriod_DistinctPerModuleVersion(t *testing.T) {
	// Two versions of the same (module, metric) are DISTINCT aggregate rows
	// (the widened idempotency key), but — unlike model — version never
	// selects a different price: both charge at the SAME catalog rate.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{
		rawAggVersion(app, mod, "orders.placed", usage.KindSum, "1.0.0", "10"),
		rawAggVersion(app, mod, "orders.placed", usage.KindSum, "2.0.0", "5"),
	}
	store.prices[priceKey(mod, "orders.placed")] = 50_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2, "two versions must roll up into two distinct aggregate rows")

	byVersion := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byVersion[a.ModuleVersion] = a
	}
	require.EqualValues(t, 50_000, byVersion["1.0.0"].UnitPriceMicros)
	require.EqualValues(t, 50_000, byVersion["2.0.0"].UnitPriceMicros, "version never changes the resolved price")
	require.EqualValues(t, 500_000, byVersion["1.0.0"].RawCostMicros) // 10 × 50_000
	require.EqualValues(t, 250_000, byVersion["2.0.0"].RawCostMicros) // 5 × 50_000
	require.EqualValues(t, 750_000, resp.TotalChargedMicros, "both versions' charges sum into the period total")
}

func TestRollupPeriod_NoModuleVersionRollsUpUnderEmptyString(t *testing.T) {
	// An event that never carried a version rolls up with ModuleVersion == ""
	// (the rollup's COALESCE(module_version, '') — mirrors the pre-023 shape)
	// rather than erroring or being dropped.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)
	require.Equal(t, "", resp.Aggregates[0].ModuleVersion)
}

func TestRollupPeriod_ModuleVersionAndModelBothDistinctDimensions(t *testing.T) {
	// The two independent dimensions (model, migration 018; module_version,
	// migration 023) compose: two versions of the SAME AI model are still
	// distinct aggregate rows, and only the model selects the price.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		{AppID: app, ModuleID: sentinel, Metric: "infra.ai.input.tokens", Kind: usage.KindSum, Model: modelSonnet, ModuleVersion: "1.0.0", BillableQuantity: "1"},
		{AppID: app, ModuleID: sentinel, Metric: "infra.ai.input.tokens", Kind: usage.KindSum, Model: modelSonnet, ModuleVersion: "2.0.0", BillableQuantity: "1"},
	}
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2)
	for _, a := range resp.Aggregates {
		require.Equal(t, modelSonnet, a.Model)
		require.EqualValues(t, 3000, a.UnitPriceMicros, "module_version never affects the per-model price")
	}
}

// --- P1 producer-target catalog seed rollup (migration 020 / infra-metrics PR #4) ---
//
// Each P1 metric is a reserved infra.* name → the 12/10 (1.2×) markup plane,
// priced from its sentinel metric_definitions row at the chosen rule-5 unit. The
// seeded prices mirror migration 020. These prove (a) each new metric prices at
// cost × 12/10 applied ONCE, and (b) a single per-1k / per-GiB UNIT value
// resolves to a non-zero charge — the regression guard that the rule-5 unit
// choice avoids the sub-micro floor-to-0.

func TestRollupPeriod_P1RequestCountPricesAt12Over10(t *testing.T) {
	// §2.7 per-request fee, per-unit (1.2 µ$/req ≥ 1 → seeded 1 µ$/request).
	// 1000 requests × 1 × 12/10 = 1200.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.request.count", usage.KindCount, "1000")}
	store.prices[priceKey(sentinel, "infra.request.count")] = 1

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 1000, a.RawCostMicros) // 1000 × 1
	require.EqualValues(t, 1200, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_P1McpToolCallPricesAt12Over10(t *testing.T) {
	// §2.7 per-call fee, per-unit (1.5 µ$/call ≥ 1 → seeded 1 µ$/call).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.mcp.tool_call.count", usage.KindCount, "100")}
	store.prices[priceKey(sentinel, "infra.mcp.tool_call.count")] = 1

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 100, a.RawCostMicros) // 100 × 1
	require.EqualValues(t, 120, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_P1CronCountPricesAt12Over10(t *testing.T) {
	// §2.2 scheduler fire, per-unit (1 µ$/fire ≥ 1 → seeded 1 µ$/fire).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.cron.count", usage.KindCount, "50")}
	store.prices[priceKey(sentinel, "infra.cron.count")] = 1

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 50, a.RawCostMicros) // 50 × 1
	require.EqualValues(t, 60, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_P1EventCountPer1kNoFloorTo0(t *testing.T) {
	// §2.2 fanout delivery: 0.4 µ$/delivery floors to 0 per-unit; rule 5 prices
	// PER 1K (400 µ$/1k). The producer emits value = deliveries/1000. Assert a
	// non-trivial volume bills NON-ZERO: 5 (×1k) = 5000 deliveries → 5 × 400 ×
	// 12/10 = 2400 (the per-1k unit survives the floor, unlike a per-delivery seed
	// of floor(0.4)=0 µ$ which would charge nothing for any volume).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.event.count", usage.KindCount, "5")}
	store.prices[priceKey(sentinel, "infra.event.count")] = 400

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 2000, a.RawCostMicros) // 5 × 400
	require.EqualValues(t, 2400, a.ChargedMicros) // × 1.2 — NOT floored to 0
}

func TestRollupPeriod_P1EventBytesPerGiBNoFloorTo0(t *testing.T) {
	// §2.2 event-bus payload: named bytes, priced/emitted PER GiB (rule 5). The
	// conservative placeholder is 1 µ$/GiB (≥ 1, so a non-zero seed; finance pins
	// the real ~1,000,000 µ$/GiB). Producer value is in GiB. 10 GiB × 1 × 12/10 =
	// 12 — non-zero, proving the GiB unit survives the per-byte floor.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.event.bytes", usage.KindSum, "10")}
	store.prices[priceKey(sentinel, "infra.event.bytes")] = 1

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 10, a.RawCostMicros) // 10 × 1
	require.EqualValues(t, 12, a.ChargedMicros) // × 1.2 — NOT floored to 0
}

func TestRollupPeriod_P1EgressApiBytesPerGiBNoFloorTo0(t *testing.T) {
	// §2.5 non-CDN egress: named bytes, priced/emitted PER GiB (rule 5; per-byte
	// 0.0000838 µ$ floors to 0). Seeded 90000 µ$/GiB (≈$0.09/GiB). Producer value
	// in GiB: a SINGLE GiB → 1 × 90000 × 12/10 = 108000, definitively non-zero —
	// the regression guard that the GiB unit avoids the silent per-byte zero.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.egress.api.bytes", usage.KindSum, "1")}
	store.prices[priceKey(sentinel, "infra.egress.api.bytes")] = 90000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 90000, a.RawCostMicros)  // 1 GiB × 90000
	require.EqualValues(t, 108000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_P1StoragePutListPer1kNoFloorTo0(t *testing.T) {
	// §2.4 S3 tier-1 ops: 0.005 µ$/op floors to 0 per-op; rule 5 prices PER 1K
	// (5 µ$/1k). Producer value = ops/1000. 2 (×1k) = 2000 ops each → 2 × 5 ×
	// 12/10 = 12 per metric, proving the per-1k unit survives the floor.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		rawAgg(app, sentinel, "infra.storage.put.count", usage.KindCount, "2"),
		rawAgg(app, sentinel, "infra.storage.list.count", usage.KindCount, "2"),
	}
	store.prices[priceKey(sentinel, "infra.storage.put.count")] = 5
	store.prices[priceKey(sentinel, "infra.storage.list.count")] = 5

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	byMetric := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byMetric[a.Metric] = a
	}
	require.Equal(t, 12, byMetric["infra.storage.put.count"].MarkupNum)
	require.Equal(t, 10, byMetric["infra.storage.put.count"].MarkupDen)
	require.Equal(t, 12, byMetric["infra.storage.list.count"].MarkupNum)
	require.Equal(t, 10, byMetric["infra.storage.list.count"].MarkupDen)
	require.EqualValues(t, 10, byMetric["infra.storage.put.count"].RawCostMicros)  // 2 × 5
	require.EqualValues(t, 12, byMetric["infra.storage.put.count"].ChargedMicros)  // × 1.2
	require.EqualValues(t, 10, byMetric["infra.storage.list.count"].RawCostMicros) // 2 × 5
	require.EqualValues(t, 12, byMetric["infra.storage.list.count"].ChargedMicros) // × 1.2
}
