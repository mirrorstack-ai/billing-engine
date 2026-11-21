package newebpayadapter

// MaxNoticeBodySize is the largest raw notice body VerifyNotice will
// parse. NewebPay's own notice bodies are a handful of form fields, so
// 64 KiB is generous headroom, not a tight fit — the limit exists to
// bound work done before any cryptographic check runs.
const MaxNoticeBodySize = 64 * 1024

// MaxTradeInfoLen is the largest hex-encoded TradeInfo field VerifyNotice
// will accept. 8 KiB decodes to 4 KiB of plaintext, far more than the
// notice JSON payload NDNF-1.2.5 describes ever needs.
const MaxTradeInfoLen = 8 * 1024
