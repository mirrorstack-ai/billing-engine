package predicate

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

var (
	windowStart = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	evalNow     = time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
)

const kind intent.ChargeKind = "usage.cycle"

func sealedIntent(t *testing.T) intent.ChargeIntent {
	t.Helper()
	sealed, err := intent.Seal(intent.Draft{
		Payer:             intent.Subject{Kind: "org", ID: "org-1"},
		Currency:          "USD",
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25)},
		Kind:              kind,
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-2026-05", AmountMicros: 1_250,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1"},
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return sealed
}

func standingAuth(t *testing.T) intent.BillingAuthorization {
	t.Helper()
	auth, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kind},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000,
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:     "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	return auth
}

// permittedState is a state in which every clause passes, including the
// unbuilt ones. It is what the executor will produce once every record
// exists, and it exists here so the refusal tests can each turn exactly
// one thing off.
func permittedState(t *testing.T) SealedState {
	t.Helper()
	sealed := sealedIntent(t)
	return SealedState{
		Intent:          sealed,
		State:           StateEligible,
		Now:             evalNow,
		BuildIdentified: true,
		Authorization:   standingAuth(t),
		Mode:            AuthorityStandingAuto,
		Notice: NoticeReceipt{
			DeliveredBytesDigest: sealed.Digest(),
			Policy:               "email/v1",
			TerminalStatus:       "delivered",
			EligibilityNotBefore: evalNow.Add(-24 * time.Hour),
			RevocationPathFresh:  true,
		},
		Funding: FundingPlan{
			Frozen:                  true,
			GrossMicros:             sealed.TotalMicros(),
			WalletAllocationMicros:  0,
			ProviderRemainderMicros: sealed.TotalMicros(),
		},
		TaxIndependentlyReproducible: true,
		PolicyDigestsMatch:           true,
		TimeReady:                    true,
		ClaimAvailable:               true,
		Unbuilt: UnbuiltEvidence{
			ProofHeadCurrent: true, ProofsApplied: true, CommercialIdentity: true,
			MerchantOfRecord: true, SourceAllocation: true, CreditLotsReserved: true,
			ExposureReservation: true, FundingMatchesAccepted: true,
			RailSupportsPlan: true, ProviderAutonomy: true, FirstStepMatchesPlan: true,
			InstrumentBinding: true, EnclaveReady: true, AttemptFrozen: true,
		},
	}
}

func TestFullyEvidencedStateIsPermitted(t *testing.T) {
	verdict := Evaluate(permittedState(t))
	if !verdict.Permitted {
		t.Fatalf("a fully evidenced state was refused: %v", verdict.Refused)
	}
}

// The zero value must refuse, and refuse for every reason. A caller who
// forgets to populate the state gets no execution — the same inversion
// as the collection-authority grant in internal/account/credit, and the
// reason this package has no fetching of its own.
func TestZeroStateRefusesEverything(t *testing.T) {
	verdict := Evaluate(SealedState{})
	if verdict.Permitted {
		t.Fatal("an empty state was permitted to charge a customer")
	}

	// Three clauses are legitimately satisfied by an empty state, and
	// saying which is more useful than asserting that everything
	// refuses:
	//
	//   - the two notice clauses are conditional on standing mode, and
	//     an empty state names no mode. What refuses instead is
	//     ClauseAuthorityEvidence, which is the clause that should:
	//     a state naming neither gate is unclear, not doubly
	//     authorized.
	//   - ClauseNoPriorSettlement is a negative. Nothing has settled
	//     against an intent that does not exist, and that is a true
	//     answer rather than an accidental pass.
	//
	// The rail clauses are skipped because an empty state has no
	// provider remainder.
	satisfiedByAbsence := map[Clause]bool{
		ClauseNoticeDelivered:   true,
		ClauseNoticeWaitElapsed: true,
		ClauseNoPriorSettlement: true,
	}
	for _, clause := range AllClauses {
		if providerClauses[clause] || satisfiedByAbsence[clause] {
			continue
		}
		if !verdict.RefusedClause(clause) {
			t.Errorf("clause %s passed on an empty state", clause)
		}
	}
	if !verdict.RefusedClause(ClauseAuthorityEvidence) {
		t.Error("an empty state named no authority gate and was not refused for it")
	}
}

// Each clause must be able to refuse on its own. A clause that cannot
// fail is decoration, and docs/SECURITY.md §2 is written on the premise
// that these gates are real.
func TestEveryClauseCanRefuseAlone(t *testing.T) {
	breakers := map[Clause]func(*SealedState){
		ClauseIntentImmutable:       func(s *SealedState) { s.Intent = intent.ChargeIntent{} },
		ClauseIntentStateEligible:   func(s *SealedState) { s.State = StateProposed },
		ClauseWithinExecutionWindow: func(s *SealedState) { s.Now = windowEnd.Add(time.Hour) },
		ClauseBuildIdentified:       func(s *SealedState) { s.BuildIdentified = false },
		ClauseAuthorizationValid:    func(s *SealedState) { s.Authorization = intent.BillingAuthorization{} },
		ClauseAuthorityEvidence:     func(s *SealedState) { s.Mode = "" },
		ClauseNoticeDelivered:       func(s *SealedState) { s.Notice.TerminalStatus = "queued" },
		ClauseNoticeWaitElapsed:     func(s *SealedState) { s.Notice.EligibilityNotBefore = evalNow.Add(time.Hour) },
		ClauseWithinCeilings:        func(s *SealedState) { s.PriorSpendMicros = 5_000_000 },
		ClauseFundingPlanBalances:   func(s *SealedState) { s.Funding.ProviderRemainderMicros++ },
		// Broken by the reproduction failing, not by an unsealed intent.
		// An earlier version reused the unsealed-intent breaker, so this
		// clause was never shown to refuse for its own reason and could
		// have been a no-op.
		ClauseTaxFinal:          func(s *SealedState) { s.TaxIndependentlyReproducible = false },
		ClausePolicyPublished:   func(s *SealedState) { s.PolicyDigestsMatch = false },
		ClauseTimeReadiness:     func(s *SealedState) { s.TimeReady = false },
		ClauseNoPriorSettlement: func(s *SealedState) { s.PriorSettlementExists = true },
		ClauseClaimAvailable:    func(s *SealedState) { s.ClaimAvailable = false },

		ClauseProofHeadCurrent:       func(s *SealedState) { s.Unbuilt.ProofHeadCurrent = false },
		ClauseProofsApplied:          func(s *SealedState) { s.Unbuilt.ProofsApplied = false },
		ClauseCommercialIdentity:     func(s *SealedState) { s.Unbuilt.CommercialIdentity = false },
		ClauseMerchantOfRecord:       func(s *SealedState) { s.Unbuilt.MerchantOfRecord = false },
		ClauseSourceAllocation:       func(s *SealedState) { s.Unbuilt.SourceAllocation = false },
		ClauseCreditLotsReserved:     func(s *SealedState) { s.Unbuilt.CreditLotsReserved = false },
		ClauseExposureReservation:    func(s *SealedState) { s.Unbuilt.ExposureReservation = false },
		ClauseFundingMatchesAccepted: func(s *SealedState) { s.Unbuilt.FundingMatchesAccepted = false },
		ClauseRailSupportsPlan:       func(s *SealedState) { s.Unbuilt.RailSupportsPlan = false },
		ClauseProviderAutonomy:       func(s *SealedState) { s.Unbuilt.ProviderAutonomy = false },
		ClauseFirstStepMatchesPlan:   func(s *SealedState) { s.Unbuilt.FirstStepMatchesPlan = false },
		ClauseInstrumentBinding:      func(s *SealedState) { s.Unbuilt.InstrumentBinding = false },
		ClauseEnclaveReady:           func(s *SealedState) { s.Unbuilt.EnclaveReady = false },
		ClauseAttemptFrozen:          func(s *SealedState) { s.Unbuilt.AttemptFrozen = false },
	}

	for _, clause := range AllClauses {
		breaker, ok := breakers[clause]
		if !ok {
			t.Errorf("clause %s has no test that makes it refuse; it may not be a real gate", clause)
			continue
		}
		t.Run(string(clause), func(t *testing.T) {
			state := permittedState(t)
			breaker(&state)

			verdict := Evaluate(state)
			if verdict.Permitted {
				t.Fatalf("breaking %s still permitted the charge", clause)
			}
			if !verdict.RefusedClause(clause) {
				t.Errorf("breaking %s refused for %v instead", clause, verdict.Refused)
			}
		})
	}
}

// The window is sealed into the intent, so it is part of what the
// customer was shown. Nothing else in the conjunction compares time to
// the document, so without this clause an eligible intent could be
// settled arbitrarily long after the customer stopped expecting it.
func TestExecutionWindowBinds(t *testing.T) {
	cases := map[string]struct {
		now         time.Time
		wantRefused bool
	}{
		"before the window opens": {windowStart.Add(-time.Second), true},
		"exactly at the open":     {windowStart, false},
		"inside":                  {evalNow, false},
		"exactly at the close":    {windowEnd, false},
		"after the window closes": {windowEnd.Add(time.Second), true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			state := permittedState(t)
			state.Now = tc.now
			// Keep the notice wait satisfied so this clause is the only
			// thing the time change can break.
			state.Notice.EligibilityNotBefore = windowStart.Add(-24 * time.Hour)

			verdict := Evaluate(state)
			if got := verdict.RefusedClause(ClauseWithinExecutionWindow); got != tc.wantRefused {
				t.Fatalf("at %v the window clause refused = %v, want %v (refusals: %v)",
					tc.now, got, tc.wantRefused, verdict.Refused)
			}
		})
	}
}

// A zero evaluation instant must refuse rather than compare as "before
// everything". A caller that forgot to set Now is a caller whose
// verdict means nothing.
func TestZeroNowRefusesTheWindow(t *testing.T) {
	state := permittedState(t)
	state.Now = time.Time{}

	if verdict := Evaluate(state); !verdict.RefusedClause(ClauseWithinExecutionWindow) {
		t.Fatal("an unset evaluation instant satisfied the execution window")
	}
}

// INV-005: notice is delivery, not sending. "Queue acceptance is not
// enough: handing a message to a queue proves only that we tried."
func TestOnlyDestinationDeliveredStatusesCount(t *testing.T) {
	for status, wantDelivered := range map[string]bool{
		"delivered": true,
		"relayed":   true,
		"queued":    false,
		"sent":      false,
		"accepted":  false,
		"bounced":   false,
		"":          false,
		"DELIVERED": false, // an allow-list is exact; a case variant is a status nobody vetted
	} {
		t.Run(status, func(t *testing.T) {
			state := permittedState(t)
			state.Notice.TerminalStatus = status

			verdict := Evaluate(state)
			if got := !verdict.RefusedClause(ClauseNoticeDelivered); got != wantDelivered {
				t.Errorf("status %q counted as delivered = %v, want %v", status, got, wantDelivered)
			}
		})
	}
}

// The notice must carry the bytes that will be collected against.
func TestNoticeForADifferentDocumentDoesNotCount(t *testing.T) {
	state := permittedState(t)
	state.Notice.DeliveredBytesDigest = "some-other-digest"

	if verdict := Evaluate(state); !verdict.RefusedClause(ClauseNoticeDelivered) {
		t.Fatal("a notice delivering different bytes satisfied the notice clause")
	}
}

// docs/DESIGN.md §4: "A bare accepted: true carrying no disclosure
// digest has no effect at all."
func TestCustomerPresentAcceptanceMustNameTheDocument(t *testing.T) {
	sealed := sealedIntent(t)
	base := func() SealedState {
		s := permittedState(t)
		s.Mode = AuthorityCustomerPresent
		s.Acceptance = AcceptanceReceipt{
			DisclosureDigest: sealed.Digest(),
			Payer:            sealed.Payer(),
			Audience:         "web-account",
			Nonce:            "n-1",
			ExpiresAt:        evalNow.Add(time.Hour),
			ReplayIdentity:   "r-1",
		}
		return s
	}

	if verdict := Evaluate(base()); !verdict.Permitted {
		t.Fatalf("a well-formed customer-present acceptance was refused: %v", verdict.Refused)
	}

	for name, break_ := range map[string]func(*SealedState){
		"no digest at all":   func(s *SealedState) { s.Acceptance.DisclosureDigest = "" },
		"another document":   func(s *SealedState) { s.Acceptance.DisclosureDigest = "other" },
		"another payer":      func(s *SealedState) { s.Acceptance.Payer = intent.Subject{Kind: "org", ID: "org-2"} },
		"no nonce":           func(s *SealedState) { s.Acceptance.Nonce = "" },
		"no audience":        func(s *SealedState) { s.Acceptance.Audience = "" },
		"no replay identity": func(s *SealedState) { s.Acceptance.ReplayIdentity = "" },
		"expired":            func(s *SealedState) { s.Acceptance.ExpiresAt = evalNow.Add(-time.Hour) },
		"no expiry":          func(s *SealedState) { s.Acceptance.ExpiresAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			state := base()
			break_(&state)
			if verdict := Evaluate(state); verdict.Permitted {
				t.Fatalf("an acceptance with %s was permitted", name)
			}
		})
	}
}

// The two gates are mutually exclusive, and a customer-present charge
// does not wait for a notice: the customer has just been shown the
// document.
func TestCustomerPresentDoesNotRequireNotice(t *testing.T) {
	sealed := sealedIntent(t)
	state := permittedState(t)
	state.Mode = AuthorityCustomerPresent
	state.Acceptance = AcceptanceReceipt{
		DisclosureDigest: sealed.Digest(), Payer: sealed.Payer(),
		Audience: "web-account", Nonce: "n-1",
		ExpiresAt: evalNow.Add(time.Hour), ReplayIdentity: "r-1",
	}
	// No notice receipt at all.
	state.Notice = NoticeReceipt{}

	if verdict := Evaluate(state); !verdict.Permitted {
		t.Fatalf("a customer-present charge was made to wait for a notice: %v", verdict.Refused)
	}
}

// A wallet-only intent moves no money at a rail, so rail evidence is
// not required of it — but everything else still is.
func TestWalletOnlyIntentSkipsRailClausesOnly(t *testing.T) {
	state := permittedState(t)
	state.Funding.WalletAllocationMicros = state.Funding.GrossMicros
	state.Funding.ProviderRemainderMicros = 0
	state.Unbuilt.RailSupportsPlan = false
	state.Unbuilt.ProviderAutonomy = false
	state.Unbuilt.FirstStepMatchesPlan = false
	state.Unbuilt.InstrumentBinding = false
	state.Unbuilt.EnclaveReady = false
	state.Unbuilt.AttemptFrozen = false

	if verdict := Evaluate(state); !verdict.Permitted {
		t.Fatalf("a wallet-only intent was refused for rail evidence: %v", verdict.Refused)
	}

	state.ClaimAvailable = false
	if verdict := Evaluate(state); verdict.Permitted {
		t.Fatal("a wallet-only intent skipped a non-rail clause too")
	}
}

// The funding plan must account for the whole obligation and no more,
// and it must be this intent's obligation.
func TestFundingPlanMustBalanceAgainstTheSealedTotal(t *testing.T) {
	for name, break_ := range map[string]func(*SealedState){
		"not frozen":         func(s *SealedState) { s.Funding.Frozen = false },
		"gross below total":  func(s *SealedState) { s.Funding.GrossMicros-- },
		"gross above total":  func(s *SealedState) { s.Funding.GrossMicros++ },
		"parts exceed gross": func(s *SealedState) { s.Funding.WalletAllocationMicros++ },
		"parts under gross":  func(s *SealedState) { s.Funding.ProviderRemainderMicros-- },
		"negative wallet":    func(s *SealedState) { s.Funding.WalletAllocationMicros = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			state := permittedState(t)
			break_(&state)
			if verdict := Evaluate(state); !verdict.RefusedClause(ClauseFundingPlanBalances) {
				t.Fatalf("a funding plan with %s satisfied the balance clause", name)
			}
		})
	}
}

// Refusals come back together. An operator fixing one condition and
// being refused for the next is how one incident becomes an afternoon.
func TestRefusalsAreReportedTogether(t *testing.T) {
	state := permittedState(t)
	state.BuildIdentified = false
	state.TimeReady = false
	state.PolicyDigestsMatch = false
	state.ClaimAvailable = false

	verdict := Evaluate(state)
	for _, want := range []Clause{
		ClauseBuildIdentified, ClauseTimeReadiness, ClausePolicyPublished, ClauseClaimAvailable,
	} {
		if !verdict.RefusedClause(want) {
			t.Errorf("refusals %v missing %s", verdict.Refused, want)
		}
	}
}

// An unrecognised clause refuses. INV-004: "A validator that permits
// whatever it was not taught to refuse is the §1 shape, arriving one
// field at a time."
func TestUnknownClauseRefuses(t *testing.T) {
	if satisfied(Clause("a_clause_nobody_wrote"), permittedState(t)) {
		t.Fatal("an unrecognised clause was satisfied")
	}
}

// docs/DESIGN.md §4 keeps the two authority gates mutually exclusive.
// A state claiming both is refused rather than treated as extra
// assurance: the standing gate requires a delivered notice and its
// wait, the customer-present one does not, so a state carrying both
// would let a caller present an acceptance and take the branch that
// skips the notice. That is the notice control removed by setting one
// extra field.
func TestStandingModeRefusesAFreshAcceptance(t *testing.T) {
	sealed := sealedIntent(t)
	state := permittedState(t) // standing mode, notice delivered

	if verdict := Evaluate(state); !verdict.Permitted {
		t.Fatalf("fixture is wrong, standing alone was refused: %v", verdict.Refused)
	}

	state.Acceptance = AcceptanceReceipt{
		DisclosureDigest: sealed.Digest(), Payer: sealed.Payer(),
		Audience: "web-account", Nonce: "n-1",
		ExpiresAt: evalNow.Add(time.Hour), ReplayIdentity: "r-1",
	}

	verdict := Evaluate(state)
	if verdict.Permitted {
		t.Fatal("a state claiming both authority gates was permitted")
	}
	if !verdict.RefusedClause(ClauseAuthorityEvidence) {
		t.Errorf("refusals = %v, want %v", verdict.Refused, ClauseAuthorityEvidence)
	}
}
