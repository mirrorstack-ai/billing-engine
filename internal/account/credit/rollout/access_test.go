package rollout

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReadOnlySelectedAccessIncludesShadowAndEnforceButNothingElse(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	foreignID := uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")

	shadow := ReadOnlySelectedAccess(
		selectedGateController(ModeShadow, accountID, nil),
	)
	require.True(t, shadow(accountID))
	require.False(t, shadow(foreignID))

	enforce := ReadOnlySelectedAccess(
		selectedGateController(ModeEnforce, accountID, nil),
	)
	require.True(t, enforce(accountID))
	require.False(t, enforce(foreignID))

	off := ReadOnlySelectedAccess(NewController(offPolicy(ComponentAPI), nil))
	require.False(t, off(accountID))
	require.False(t, ReadOnlySelectedAccess(nil)(accountID))
}
