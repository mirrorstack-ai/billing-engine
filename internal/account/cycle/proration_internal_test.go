package cycle

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAppLineLabel(t *testing.T) {
	appID := uuid.MustParse("38b0785f-8128-42f6-8d5b-516da9709c9c")

	require.Equal(t, "My App (app "+appID.String()+")", appLineLabel("My App", appID))
	require.Equal(t, "app "+appID.String(), appLineLabel("", appID))
}
