package newebpayadapter

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"unicode/utf8"
)

// MPGVersion is the MPG API version every signed form carries, inside
// TradeInfo and as the posted Version field. One constant, so a form can
// never say one version in TradeInfo and another beside it. It matches the
// known-answer vectors in testdata; re-check it against the current MPG
// manual before the first sandbox payment.
const MPGVersion = "2.0"

// The MPG limits a form is checked against before anything is encrypted.
const (
	maxMerchantOrderNoLen = 30
	maxItemDescRunes      = 50
	maxCallbackURLLen     = 200
)

// The two LangType values the payment page is offered in. Empty means
// LangZhTW.
const (
	LangZhTW = "zh-tw"
	LangEn   = "en"
)

var (
	merchantFieldPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

	// ErrInvalidMerchantID is returned when Credentials.MerchantID is empty
	// or carries a character outside [A-Za-z0-9_].
	ErrInvalidMerchantID = errors.New("newebpayadapter: invalid MerchantID")
	// ErrInvalidMerchantOrderNo is returned when MerchantOrderNo is empty,
	// longer than 30 characters or outside [A-Za-z0-9_].
	ErrInvalidMerchantOrderNo = errors.New("newebpayadapter: invalid MerchantOrderNo")
	// ErrInvalidItemDesc is returned when ItemDesc is empty or longer than
	// 50 characters.
	ErrInvalidItemDesc = errors.New("newebpayadapter: invalid ItemDesc")
	// ErrInvalidCallbackURL is returned when NotifyURL or ReturnURL is not
	// an absolute https URL on port 443 of at most 200 characters.
	ErrInvalidCallbackURL = errors.New("newebpayadapter: invalid callback URL")
	// ErrReturnURLIsNotifyURL is returned when ReturnURL equals NotifyURL:
	// the browser return is not the notice and must never be taken for it.
	ErrReturnURLIsNotifyURL = errors.New("newebpayadapter: ReturnURL equals NotifyURL")
	// ErrInvalidLangType is returned for a LangType other than "", "zh-tw"
	// or "en".
	ErrInvalidLangType = errors.New("newebpayadapter: invalid LangType")
	// ErrInvalidTimeStamp is returned for a TimeStamp that is not positive.
	ErrInvalidTimeStamp = errors.New("newebpayadapter: invalid TimeStamp")
)

// MPGRequest is one card payment form, in typed fields. The caller owns
// every value here, the clock included: TimeStamp is unix seconds from
// the caller's clock (NewebPay refuses a stamp far off its own), never
// read here.
type MPGRequest struct {
	Environment     Environment
	TimeStamp       int64
	MerchantOrderNo string
	// Amt is the integer NT$ amount the payer is charged.
	Amt       int64
	ItemDesc  string
	NotifyURL string
	ReturnURL string
	LangType  string
}

// MPGForm is what the browser posts, as a top-level form POST, to
// ActionURL. NewebPay answers an iframe, a proxy or a server-side POST
// with MPG02005, so these fields go to a page the payer's browser submits.
type MPGForm struct {
	ActionURL  string
	MerchantID string
	TradeInfo  string
	TradeSha   string
	Version    string
}

// Values returns the four posted form fields.
func (f MPGForm) Values() url.Values {
	return url.Values{
		"MerchantID": {f.MerchantID},
		"TradeInfo":  {f.TradeInfo},
		"TradeSha":   {f.TradeSha},
		"Version":    {f.Version},
	}
}

// SignMPGForm checks r, builds TradeInfo with CREDIT=1 (a card payment)
// and RespondType=JSON, encrypts it and signs it. Every check runs before
// any key is used, so a refused request never touches key material.
func SignMPGForm(creds Credentials, r MPGRequest) (MPGForm, error) {
	actionURL, err := r.Environment.MPGURL()
	if err != nil {
		return MPGForm{}, err
	}
	if err := r.validate(creds.MerchantID); err != nil {
		return MPGForm{}, err
	}
	lang := r.LangType
	if lang == "" {
		lang = LangZhTW
	}
	plain := BuildTradeInfo(TradeInfoRequest{
		MerchantID:      creds.MerchantID,
		RespondType:     "JSON",
		TimeStamp:       r.TimeStamp,
		Version:         MPGVersion,
		MerchantOrderNo: r.MerchantOrderNo,
		Amt:             r.Amt,
		ItemDesc:        r.ItemDesc,
		NotifyURL:       r.NotifyURL,
		ReturnURL:       r.ReturnURL,
		LangType:        lang,
		Credit:          true,
	})
	tradeInfo, err := Encrypt(creds.HashKey, creds.HashIV, plain)
	if err != nil {
		return MPGForm{}, err
	}
	return MPGForm{
		ActionURL:  actionURL,
		MerchantID: creds.MerchantID,
		TradeInfo:  tradeInfo,
		TradeSha:   TradeSha(creds.HashKey, creds.HashIV, tradeInfo),
		Version:    MPGVersion,
	}, nil
}

func (r MPGRequest) validate(merchantID string) error {
	if !merchantFieldPattern.MatchString(merchantID) {
		return ErrInvalidMerchantID
	}
	if len(r.MerchantOrderNo) > maxMerchantOrderNoLen || !merchantFieldPattern.MatchString(r.MerchantOrderNo) {
		return fmt.Errorf("%w: %q", ErrInvalidMerchantOrderNo, r.MerchantOrderNo)
	}
	if r.Amt <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidAmount, r.Amt)
	}
	if r.TimeStamp <= 0 {
		return ErrInvalidTimeStamp
	}
	if n := utf8.RuneCountInString(r.ItemDesc); n == 0 || n > maxItemDescRunes || !utf8.ValidString(r.ItemDesc) {
		return ErrInvalidItemDesc
	}
	switch r.LangType {
	case "", LangZhTW, LangEn:
	default:
		return fmt.Errorf("%w: %q", ErrInvalidLangType, r.LangType)
	}
	for _, u := range []string{r.NotifyURL, r.ReturnURL} {
		if err := checkCallbackURL(u); err != nil {
			return err
		}
	}
	if r.ReturnURL == r.NotifyURL {
		return ErrReturnURLIsNotifyURL
	}
	return nil
}

// checkCallbackURL accepts an absolute https URL with a host, on the
// default port or an explicit 443, of at most 200 characters, with no
// user info and no fragment.
func checkCallbackURL(raw string) error {
	if raw == "" || len(raw) > maxCallbackURLLen {
		return fmt.Errorf("%w: length %d", ErrInvalidCallbackURL, len(raw))
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("%w: %q", ErrInvalidCallbackURL, raw)
	}
	if p := u.Port(); p != "" && p != "443" {
		return fmt.Errorf("%w: port %s", ErrInvalidCallbackURL, p)
	}
	return nil
}
