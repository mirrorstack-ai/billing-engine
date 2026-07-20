package credit

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEstimateKeyIncludesAccountAndUTCPeriod(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	period := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	require.Equal(t, "billing:credit-estimate:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1782864000", estimateKey(accountID, period))
}

func TestNewRedisCounterWithoutURLIsUnavailable(t *testing.T) {
	counter, err := NewRedisCounter("")
	require.ErrorIs(t, err, ErrUnavailable)
	require.Nil(t, counter)
}
