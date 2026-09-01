//go:build integration

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// groupCollector records what a group collection was asked for.
type groupCollector struct {
	calls  [][]Debit
	result CollectResult
	err    error
}

func (c *groupCollector) CollectGroup(_ context.Context, debits []Debit) (CollectResult, error) {
	c.calls = append(c.calls, debits)
	return c.result, c.err
}

// A whole group settles, and the provider is asked ONCE.
func TestAGroupSettlesAsOneCollection(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	a := readyN(t, s, 1)
	b := readyN(t, s, 2)
	require.NoError(t, s.SaveIntentGroup(ctx, "run:1", []string{a.Digest(), b.Digest()}))

	collector := &groupCollector{result: CollectResult{Succeeded: true, Reference: "in_group"}}
	outs, err := newExecutor(t, s, &recordingCollector{}, fullyEvidencedEnv()).
		ExecuteGroup(ctx, collector, []string{a.Digest(), b.Digest()})
	require.NoError(t, err)

	require.Len(t, collector.calls, 1, "the group was collected in more than one call")
	require.Len(t, collector.calls[0], 2, "the collection did not carry both intents")
	require.Len(t, outs, 2)
	for _, o := range outs {
		require.True(t, o.Settled, "a member of a settled group did not settle")
	}

	for _, d := range []string{a.Digest(), b.Digest()} {
		var outcome *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
			d).Scan(&outcome))
		require.NotNil(t, outcome)
		require.Equal(t, "succeeded", *outcome)
	}
}

// 🔴 One refused member refuses the whole group, and NOTHING is claimed.
//
// Collecting the rest would invoice part of a charge: the customer pays some
// of what they owe, and nothing in the system records that a group was split.
func TestOneRefusedMemberRefusesTheWholeGroup(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	a := readyN(t, s, 3)
	b := readyN(t, s, 4)

	// Break exactly one member: its notice evidence goes away, so
	// ClauseNoticeDelivered refuses it and nothing else.
	_, err := pool.Exec(ctx, `DELETE FROM ms_billing.notice_receipts WHERE intent_digest = $1`, b.Digest())
	require.NoError(t, err)

	collector := &groupCollector{result: CollectResult{Succeeded: true}}
	outs, err := newExecutor(t, s, &recordingCollector{}, fullyEvidencedEnv()).
		ExecuteGroup(ctx, collector, []string{a.Digest(), b.Digest()})
	require.NoError(t, err)

	require.Empty(t, collector.calls, "a group with a refused member reached the provider")
	for _, o := range outs {
		require.False(t, o.Permitted, "a member of a refused group was permitted")
		require.NotEmpty(t, o.Refused, "a refusal named no clause")
	}

	var claims int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.intent_settlement_claims WHERE intent_digest = ANY($1)`,
		[]string{a.Digest(), b.Digest()}).Scan(&claims))
	require.Zero(t, claims,
		"a refused group left a claim behind, so the permitted member is now stuck: "+
			"claimed, uncollected, and invisible to the next pass")
}

// 🔴 An ambiguous answer retains EVERY claim.
//
// Releasing the ones this pass happened to look at first would let a later
// attempt charge a customer who may already have been charged — Execute's
// reason for retaining one claim, multiplied across the group.
func TestAnAmbiguousGroupRetainsEveryClaim(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	a := readyN(t, s, 5)
	b := readyN(t, s, 6)

	collector := &groupCollector{err: errors.New("timeout after dispatch")}
	outs, err := newExecutor(t, s, &recordingCollector{}, fullyEvidencedEnv()).
		ExecuteGroup(ctx, collector, []string{a.Digest(), b.Digest()})
	require.NoError(t, err)

	for _, o := range outs {
		require.True(t, o.Unresolved, "an ambiguous group member was not reported unresolved")
	}

	var claims, outcomes int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*), count(outcome) FROM ms_billing.intent_settlement_claims
		  WHERE intent_digest = ANY($1)`,
		[]string{a.Digest(), b.Digest()}).Scan(&claims, &outcomes))
	require.Equal(t, 2, claims, "an ambiguous group released a claim")
	require.Zero(t, outcomes, "an ambiguous group recorded an outcome nobody knows")
}

// 🔴 A group is offered for execution only when EVERY member is a candidate.
//
// One member already claimed or terminal holds the rest back — otherwise the
// next pass would collect the remainder as if it were the whole charge.
func TestAnIncompleteGroupIsNotOffered(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	a := readyN(t, s, 7)
	b := readyN(t, s, 8)
	require.NoError(t, s.SaveIntentGroup(ctx, "run:2", []string{a.Digest(), b.Digest()}))

	groups, err := s.PendingExecutionGrouped(ctx, 10)
	require.NoError(t, err)
	require.Len(t, groups, 1, "a whole group was not offered as one")
	require.Len(t, groups[0], 2)

	// Claim one member out from under the group.
	require.NoError(t, s.ClaimSettlement(ctx, b.Digest(), "somebody-else"))

	groups, err = s.PendingExecutionGrouped(ctx, 10)
	require.NoError(t, err)
	for _, g := range groups {
		for _, d := range g {
			require.NotEqual(t, a.Digest(), d,
				"the remainder of a broken group was offered for execution; collecting it "+
					"would invoice part of a charge")
		}
	}
}

// An ungrouped intent comes back as a group of one, so callers have a single
// shape and the ordinary path is not a special case.
func TestAnUngroupedIntentIsAGroupOfOne(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	solo := readyN(t, s, 9)

	groups, err := s.PendingExecutionGrouped(ctx, 10)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, []string{solo.Digest()}, groups[0])
}

// An intent may belong to at most one group. A document that could settle on
// two invoices is a double charge waiting for a retry.
func TestAnIntentCannotJoinTwoGroups(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	only := readyN(t, s, 10)
	require.NoError(t, s.SaveIntentGroup(ctx, "run:3", []string{only.Digest()}))
	// A second grouping is a no-op, not a second row.
	require.NoError(t, s.SaveIntentGroup(ctx, "run:4", []string{only.Digest()}))

	var groupID string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT group_id FROM ms_billing.intent_groups WHERE intent_digest = $1`,
		only.Digest()).Scan(&groupID))
	require.Equal(t, "run:3", groupID, "a re-grouping overwrote the original")
}

// A grouping is append-only: correcting one means superseding the intents.
func TestAGroupingCannotBeEditedOrDeleted(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	one := readyN(t, s, 11)
	require.NoError(t, s.SaveIntentGroup(ctx, "run:5", []string{one.Digest()}))

	for name, stmt := range map[string]string{
		"update": `UPDATE ms_billing.intent_groups SET group_id = 'other' WHERE intent_digest = $1`,
		"delete": `DELETE FROM ms_billing.intent_groups WHERE intent_digest = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(ctx, stmt, one.Digest())
			require.Error(t, err, "a grouping was %sd", name)
			require.Contains(t, err.Error(), "append-only")
		})
	}
}

// A group with no collector, or no members, must not be attempted.
func TestAnUncollectableGroupIsRefused(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	exec := newExecutor(t, s, &recordingCollector{}, fullyEvidencedEnv())

	_, err := exec.ExecuteGroup(context.Background(), nil, []string{"d1"})
	require.ErrorIs(t, err, ErrGroupNotCollectable)

	_, err = exec.ExecuteGroup(context.Background(), &groupCollector{}, nil)
	require.ErrorIs(t, err, ErrGroupNotCollectable)
}
