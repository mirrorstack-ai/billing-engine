package newebpayadapter

import "testing"

func TestLimitConstants(t *testing.T) {
	if MaxNoticeBodySize != 64*1024 {
		t.Fatalf("MaxNoticeBodySize = %d, want %d", MaxNoticeBodySize, 64*1024)
	}
	if MaxTradeInfoLen != 8*1024 {
		t.Fatalf("MaxTradeInfoLen = %d, want %d", MaxTradeInfoLen, 8*1024)
	}
	if MaxTradeInfoLen >= MaxNoticeBodySize {
		t.Fatalf("MaxTradeInfoLen (%d) should be well under MaxNoticeBodySize (%d)", MaxTradeInfoLen, MaxNoticeBodySize)
	}
}
