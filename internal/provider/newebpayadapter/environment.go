package newebpayadapter

import "fmt"

// Environment selects which NewebPay host a URL is built against.
type Environment string

// The two environments NewebPay operates. There is no default: an
// Environment value outside this set is always an error, never silently
// treated as one or the other — sending a live form to the sandbox host
// or vice versa is the kind of mistake that should fail loudly rather
// than charge (or fail to charge) the wrong store.
const (
	Sandbox Environment = "sandbox"
	Live    Environment = "live"
)

var environmentHosts = map[Environment]string{
	Sandbox: "ccore.newebpay.com",
	Live:    "core.newebpay.com",
}

func (e Environment) host() (string, error) {
	host, ok := environmentHosts[e]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownEnvironment, string(e))
	}
	return host, nil
}

// MPGURL returns the MPG gateway form target for e:
// https://<host>/MPG/mpg_gateway.
func (e Environment) MPGURL() (string, error) {
	host, err := e.host()
	if err != nil {
		return "", err
	}
	return "https://" + host + "/MPG/mpg_gateway", nil
}

// QueryTradeInfoURL returns the trade-query endpoint for e:
// https://<host>/API/QueryTradeInfo.
func (e Environment) QueryTradeInfoURL() (string, error) {
	host, err := e.host()
	if err != nil {
		return "", err
	}
	return "https://" + host + "/API/QueryTradeInfo", nil
}
