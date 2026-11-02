package newebpayadapter

// Known-answer vectors generated with openssl; see testdata/README.md for
// the exact commands that produce every value below from the fake
// key/IV/plaintext pairs also given there. None of this is a real
// NewebPay store credential.

const (
	vecKeyRaw = "FAKEFAKEFAKEFAKEFAKEFAKEFAKE1234"
	vecIVRaw  = "FAKEFAKEFAKE5678"

	vecOtherKeyRaw = "OTHERKEYOTHERKEYOTHERKEYOTHERKEY"
	vecOtherIVRaw  = "OTHERKEYOTHERKEY"

	vecMerchantID      = "MS00000001"
	vecMerchantOrderNo = "TEST000000000001"
	vecAmt             = 100

	// TradeInfo query-string vector.
	vecQueryPlain     = "MerchantID=MS00000001&RespondType=JSON&TimeStamp=1700000000&Version=2.0&MerchantOrderNo=TEST000000000001&Amt=100&ItemDesc=Test+Item&NotifyURL=https%3A%2F%2Fexample.test%2Fnotify&ReturnURL=https%3A%2F%2Fexample.test%2Freturn"
	vecQueryCipherHex = "c5c3cf57ac0443b0291743e806a3780e174b8cd54322409f4b790b75da2ccd5ca5efc3966d3c3707fa5c87c1f9c9eb71f7beb47c363ac227ccfae12a5102f997e6433feb48d30a72f2103a0b54d22407cc0324beb9ffe5415b794827204deab9d5a063db9ac8eb09279a4ab7b60f30e9e00699d02137b3ba95363268370d36075241915816992fe28d26243a692b0af6adb7a4f2e6f684e11af05ddce0cf6cd560f44ff596d6d8a8ee638488ed7ea0b563333db446195d8eaa217511ef69632a84118266de5d5454fec5c3052703194ca39ddc365399e2a3886648fb9c115425"
	vecQueryTradeSha  = "CD113AB40234A02361D42E12EF4B092E2311A7566B44E40CEE444131BA49AE83"

	// The notice ciphertext below, TradeSha signed under the OTHER fake
	// store's key instead of the real one: a legitimate signature that
	// authenticates the wrong store.
	vecNoticeOtherStoreTradeSha = "500A3977C5E362E65705772F7E8A841021FCF59C459A0E4E82684844E8A12D45"

	// CheckValue vector: Amt=100, MerchantID=MS00000001, MerchantOrderNo=TEST000000000001.
	vecCheckValue = "0E6EFD3BC7FBF31F9B92A2B5019B7D3CCF6B99B03FFF4C0184A76F781C0076B7"

	// Notice (success) vector.
	vecNoticePlain     = `{"Status":"SUCCESS","Message":"Success","Result":{"MerchantID":"MS00000001","Amt":"100","TradeNo":"25110200000001","MerchantOrderNo":"TEST000000000001","PaymentType":"CREDIT","PayTime":"2026-11-02 12:00:00","IP":"127.0.0.1","EscrowBank":"TestBank"}}`
	vecNoticeCipherHex = "41c2237766771de4da8c4ee607acbf2ef0f32e58b7289ee06bf749d42247d72ce4b10f56ced95ecd97a8756c4287d4b90ea1c621d76fd971698a627b8c9c5d52ba7508de6a988068677b56518cb6514fc5657a5d603c756c621062bf9573e2b73f65f073a77ad22cac75c23d08415f8e52c33fe1b46292f7401826fef044e71a1c025493a9b1a7ea337a031f53155935b6c028fcb5344842a68146d4c722fa9b65aae0a371224ced5b00963b8e0e3202542e8d2113ef8df1c053c760d85eef10392f4f03d9281999d47a6a50d8cb5b0bbe1107250db77c511c892c245c9d7d29024391e46a5662f17007537e4e8810b3a7aed91500a0a6c764b3601a7594d544"
	vecNoticeTradeSha  = "C572A36C47FFCB1017D4DF4773FCC438B8DEE272C2A3B53E799AEEA875D26BCA"

	// Notice (Status not SUCCESS) vector.
	vecNoticeFailCipherHex = "4a84b940a536a2e7b2ab03f8654381af55b8557cccd6bbc42b02915554184f20af259be92d3371024e3e5ee36410b3960c05318f89c4ea21a75b6ea3f9ec12ae"
	vecNoticeFailTradeSha  = "BE1DD21C7527E0B67C252FCBA409C1390A8EC9BD624C60E1EA524AC4C1F8227B"

	// Notice (inner MerchantID != outer) vector.
	vecNoticeMismatchCipherHex = "41c2237766771de4da8c4ee607acbf2ef0f32e58b7289ee06bf749d42247d72ce4b10f56ced95ecd97a8756c4287d4b90ea1c621d76fd971698a627b8c9c5d523ef8fad2893172de23dea151ac521927cf5f211c6582dac060169320c398946fdb65d8f30d9e9b5aff4bd63da8a5299658c74e22c940bbc2cd6406637f70f928fc7406dbd0c39efd5e3cf4b1ce561c3af27c4c6ba4367ff3dcc8082add4f677830447e55cfa7d10222f59ee25280531906a07c964e7b6fba748fc5a29b1ad4fde6d7bffc4c70051ac836e30895eaa23224222583f6a123342668f176c2c17811400daa8c3e44bc02cb1e8638b70fc243b3e1d3a2e284876aad50e57dd52a0809"
	vecNoticeMismatchTradeSha  = "1088F7CB11DB4102000A913791E22436542482BC9E66E4F894E4B8049375DFA6"
)

func vecKey(tb testingTB) HashKey {
	tb.Helper()
	k, err := NewHashKey([]byte(vecKeyRaw))
	if err != nil {
		tb.Fatalf("NewHashKey: %v", err)
	}
	return k
}

func vecIV(tb testingTB) HashIV {
	tb.Helper()
	iv, err := NewHashIV([]byte(vecIVRaw))
	if err != nil {
		tb.Fatalf("NewHashIV: %v", err)
	}
	return iv
}

// testingTB is the sliver of *testing.T / *testing.B this file needs, so
// the vector helpers work from both.
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
}
