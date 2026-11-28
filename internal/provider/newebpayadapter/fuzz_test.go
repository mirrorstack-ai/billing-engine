package newebpayadapter

import "testing"

func FuzzDecrypt(f *testing.F) {
	f.Add(vecQueryCipherHex)
	f.Add("")
	f.Add("zz")
	f.Add("00")
	f.Add(vecQueryCipherHex[:len(vecQueryCipherHex)-1])
	f.Fuzz(func(t *testing.T, tradeInfoHex string) {
		// No panic on any input is the whole assertion: a crash here is a
		// crash in a notice handler fed attacker-controlled bytes.
		_, _ = Decrypt(vecKey(t), vecIV(t), tradeInfoHex)
	})
}

func FuzzVerifyNotice(f *testing.F) {
	f.Add(string(noticeBody(validNoticeFields())))
	f.Add("")
	f.Add("MerchantID=A&MerchantID=B")
	f.Add("TradeInfo=zz&TradeSha=zz&MerchantID=" + vecMerchantID)
	f.Fuzz(func(t *testing.T, body string) {
		_, _ = VerifyNotice(vecCreds(t), []byte(body))
	})
}
