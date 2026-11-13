package paymentfact

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func validFact() New {
	return New{
		Rail:              RailStripe,
		RailRef:           "in_test_1",
		Mode:              ModeLive,
		SourceKind:        SourceEngineSettlement,
		PayerAccountID:    uuid.New(),
		Currency:          "usd",
		NetAmountMicros:   900_000,
		TaxAmountMicros:   100_000,
		GrossAmountMicros: 1_000_000,
		PaidAt:            time.Now(),
		InvoiceDecision:   DecisionNotApplicable,
		DecisionReason:    "foreign_seller_usd",
	}
}

func TestValidateAcceptsAWellFormedFact(t *testing.T) {
	require.NoError(t, validFact().validate())
}

func TestValidateRejectsAnUnknownRail(t *testing.T) {
	n := validFact()
	n.Rail = "paypal"
	require.ErrorIs(t, n.validate(), ErrUnknownRail)
}

func TestValidateRejectsAnUnknownMode(t *testing.T) {
	n := validFact()
	n.Mode = "staging"
	require.ErrorIs(t, n.validate(), ErrUnknownMode)
}

func TestValidateRejectsAnUnknownSourceKind(t *testing.T) {
	n := validFact()
	n.SourceKind = "webhook"
	require.ErrorIs(t, n.validate(), ErrUnknownSourceKind)
}

func TestValidateRejectsAnUnknownDecision(t *testing.T) {
	n := validFact()
	n.InvoiceDecision = "issued"
	require.ErrorIs(t, n.validate(), ErrUnknownDecision)
}

func TestValidateRejectsAnEmptyRailRef(t *testing.T) {
	n := validFact()
	n.RailRef = ""
	require.ErrorIs(t, n.validate(), ErrIncomplete)
}

func TestValidateRejectsANilPayerAccount(t *testing.T) {
	n := validFact()
	n.PayerAccountID = uuid.Nil
	require.ErrorIs(t, n.validate(), ErrIncomplete)
}

func TestValidateRejectsAmountsThatDisagree(t *testing.T) {
	n := validFact()
	n.GrossAmountMicros = n.NetAmountMicros + n.TaxAmountMicros + 1
	require.ErrorIs(t, n.validate(), ErrAmountsDisagree)
}

// 🔴 The pending/reason pairing is an iff, not two independent rules: a
// decided fact with no reason hides why, and an undecided fact with a reason
// states a verdict nobody reached.
func TestValidateRejectsAPendingFactWithAReason(t *testing.T) {
	n := validFact()
	n.InvoiceDecision = DecisionPending
	n.DecisionReason = "foreign_seller_usd"
	require.ErrorIs(t, n.validate(), ErrIncomplete)
}

func TestValidateRejectsADecidedFactWithNoReason(t *testing.T) {
	n := validFact()
	n.InvoiceDecision = DecisionDeferred
	n.DecisionReason = ""
	require.ErrorIs(t, n.validate(), ErrIncomplete)
}

func TestValidateAcceptsAPendingFactWithNoReason(t *testing.T) {
	n := validFact()
	n.InvoiceDecision = DecisionPending
	n.DecisionReason = ""
	require.NoError(t, n.validate())
}

// StatementPaidMarkDue backs the #267 hook: a paid-mark must not fire before
// the statement's total is reached, and must fire the instant it is.
func TestStatementPaidMarkDue(t *testing.T) {
	require.False(t, StatementPaidMarkDue(0, 10_000_000), "nothing recorded yet")
	require.False(t, StatementPaidMarkDue(9_999_999, 10_000_000), "one cent short")
	require.True(t, StatementPaidMarkDue(10_000_000, 10_000_000), "exactly the total")
	require.True(t, StatementPaidMarkDue(10_000_001, 10_000_000), "overpaid")
	require.False(t, StatementPaidMarkDue(0, 0), "a zero-total statement is not a paid one")
}
