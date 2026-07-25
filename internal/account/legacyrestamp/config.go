package legacyrestamp

import (
	"fmt"
)

const (
	EnvRestampMode = "CREDIT_WALLET_LEGACY_RESTAMP"
	EnvMaster      = "CREDIT_WALLET_ENABLED"
	EnvWorkerMode  = "CREDIT_WALLET_WORKER_MODE"
	EnvWorkerBPS   = "CREDIT_WALLET_WORKER_BPS"
	EnvCoreSHA     = "CORE_MANIFEST_SHA"
	EnvBillingSHA  = "BILLING_ENGINE_SHA"
)

// Environment is the complete, auditable input to the explicit rollback
// restamp mode.
type Environment struct {
	RestampMode string
	Master      string
	WorkerMode  string
	WorkerBPS   string
	CoreSHA     string
	BillingSHA  string
}

type Config struct {
	Enabled    bool
	CoreSHA    string
	BillingSHA string
}

// ParseEnvironment has only two safe outcomes:
//   - an empty restamp value selects the ordinary billing-cycle worker;
//   - exact "1" plus exact OFF controls and full immutable SHAs selects restamp.
//
// Every other non-empty or inconsistent value is an operator-intent error. The
// billing-cycle binary must exit instead of silently running money/sweep work.
func ParseEnvironment(env Environment) (Config, error) {
	if env.RestampMode == "" {
		return Config{}, nil
	}
	if env.RestampMode != "1" {
		return Config{}, fmt.Errorf(
			"%s must be empty or exact 1",
			EnvRestampMode,
		)
	}
	if env.Master != "0" {
		return Config{}, fmt.Errorf(
			"restamp requires %s=0",
			EnvMaster,
		)
	}
	if env.WorkerMode != "off" {
		return Config{}, fmt.Errorf(
			"restamp requires %s=off",
			EnvWorkerMode,
		)
	}
	if env.WorkerBPS != "0" {
		return Config{}, fmt.Errorf(
			"restamp requires %s=0",
			EnvWorkerBPS,
		)
	}
	if !fullLowerSHA(env.CoreSHA) {
		return Config{}, fmt.Errorf(
			"restamp requires a full lowercase %s",
			EnvCoreSHA,
		)
	}
	if !fullLowerSHA(env.BillingSHA) {
		return Config{}, fmt.Errorf(
			"restamp requires a full lowercase %s",
			EnvBillingSHA,
		)
	}
	return Config{
		Enabled:    true,
		CoreSHA:    env.CoreSHA,
		BillingSHA: env.BillingSHA,
	}, nil
}

func fullLowerSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
