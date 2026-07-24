// Package rollout contains the fail-closed, account-scoped credit-wallet
// rollout policy shared by API/webhook and billing-worker construction roots.
// It selects a cohort only; callers must make the decision before constructing
// or invoking any wallet graph.
package rollout

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	// Version is part of every cohort hash. Changing it would reshuffle every
	// percentage cohort and therefore requires a separately reviewed rollout.
	Version = "credit-wallet-v1"

	envMaster     = "CREDIT_WALLET_ENABLED"
	envAllowlist  = "CREDIT_WALLET_ALLOWLIST"
	envManifest   = "CORE_MANIFEST_SHA"
	envBillingSHA = "BILLING_ENGINE_SHA"
)

// Component separates API/webhook and scheduled-worker cohorts. A percentage
// change for one component cannot move an account into the other component.
type Component string

const (
	ComponentAPI    Component = "api"
	ComponentWorker Component = "worker"
)

// Mode is the selected account behavior. Shadow is strictly read-only and may
// emit comparison telemetry, while Enforce permits the reviewed wallet path.
type Mode string

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnforce Mode = "enforce"
)

// Config is the complete startup-time policy input. Active policies require
// exact release identities so every observation can be attributed to one
// immutable deployment.
type Config struct {
	MasterEnabled   bool
	SchemaReady     bool
	Component       Component
	Mode            string
	BasisPoints     string
	Allowlist       string
	CoreManifestSHA string
	BillingSHA      string
}

// Policy is immutable after startup.
type Policy struct {
	component       Component
	mode            Mode
	basisPoints     uint16
	allowlist       map[uuid.UUID]struct{}
	coreManifestSHA string
	billingSHA      string
	rolloutID       string
}

// Decision is the stable account-scoped result. CohortBucket is safe for
// structured diagnostics but must never be used as a metric dimension.
type Decision struct {
	Component       Component
	Mode            Mode
	BasisPoints     uint16
	CohortBucket    uint16
	Selected        bool
	Allowlisted     bool
	CoreManifestSHA string
	BillingSHA      string
	RolloutID       string
}

// Shadowed reports whether this selected account may run the read-only wallet
// comparison path. It never authorizes a mutation.
func (d Decision) Shadowed() bool {
	return d.Selected && d.Mode == ModeShadow
}

// Enforced is the sole positive wallet-mutation authorization.
func (d Decision) Enforced() bool {
	return d.Selected && d.Mode == ModeEnforce
}

// FromEnv loads one component policy. Missing or malformed active
// configuration resolves to an off policy; it never partially enables.
func FromEnv(component Component, schemaReady bool) Policy {
	prefix := "CREDIT_WALLET_"
	switch component {
	case ComponentAPI:
		prefix += "API_"
	case ComponentWorker:
		prefix += "WORKER_"
	default:
		return offPolicy(component)
	}
	return Parse(Config{
		MasterEnabled:   masterEnabled(os.Getenv(envMaster)),
		SchemaReady:     schemaReady,
		Component:       component,
		Mode:            os.Getenv(prefix + "MODE"),
		BasisPoints:     os.Getenv(prefix + "BPS"),
		Allowlist:       os.Getenv(envAllowlist),
		CoreManifestSHA: os.Getenv(envManifest),
		BillingSHA:      os.Getenv(envBillingSHA),
	})
}

// Parse validates a policy atomically. Explicit off, a disabled master switch,
// or an unready schema does not require the active-only fields.
func Parse(cfg Config) Policy {
	if !cfg.MasterEnabled || !cfg.SchemaReady {
		return offPolicy(cfg.Component)
	}
	if cfg.Component != ComponentAPI && cfg.Component != ComponentWorker {
		return offPolicy(cfg.Component)
	}
	mode := Mode(cfg.Mode)
	if mode == ModeOff {
		return offPolicy(cfg.Component)
	}
	if mode != ModeShadow && mode != ModeEnforce {
		return offPolicy(cfg.Component)
	}

	bps, ok := parseBasisPoints(cfg.BasisPoints)
	if !ok {
		return offPolicy(cfg.Component)
	}
	allowlist, ok := parseAllowlist(cfg.Allowlist)
	if !ok || !fullSHA(cfg.CoreManifestSHA) || !fullSHA(cfg.BillingSHA) {
		return offPolicy(cfg.Component)
	}
	policy := Policy{
		component: cfg.Component, mode: mode, basisPoints: bps,
		allowlist: allowlist, coreManifestSHA: cfg.CoreManifestSHA,
		billingSHA: cfg.BillingSHA,
	}
	policy.rolloutID = rolloutID(
		policy.coreManifestSHA,
		policy.billingSHA,
		policy.component,
		policy.mode,
		policy.basisPoints,
	)
	return policy
}

// Decide returns the immutable cohort decision for accountID.
func (p Policy) Decide(accountID uuid.UUID) Decision {
	decision := Decision{
		Component: p.component, Mode: p.mode, BasisPoints: p.basisPoints,
		CoreManifestSHA: p.coreManifestSHA, BillingSHA: p.billingSHA,
		RolloutID: p.rolloutID,
	}
	if p.mode == ModeOff || accountID == uuid.Nil {
		return decision
	}
	decision.CohortBucket = Bucket(p.component, accountID)
	_, decision.Allowlisted = p.allowlist[accountID]
	decision.Selected = decision.Allowlisted || decision.CohortBucket < p.basisPoints
	return decision
}

// Bucket implements the fixed credit-wallet-v1 selector: SHA-256 over the
// literal concatenation of version, component, and canonical account UUID;
// the first 64 bits are interpreted big-endian and reduced to 10,000 bps.
func Bucket(component Component, accountID uuid.UUID) uint16 {
	sum := sha256.Sum256([]byte(Version + string(component) + accountID.String()))
	return uint16(binary.BigEndian.Uint64(sum[:8]) % 10_000)
}

func offPolicy(component Component) Policy {
	return Policy{
		component: component, mode: ModeOff,
		allowlist: map[uuid.UUID]struct{}{},
		rolloutID: "off",
	}
}

func masterEnabled(raw string) bool {
	switch raw {
	case "1", "true", "TRUE", "True":
		return true
	default:
		return false
	}
}

func parseBasisPoints(raw string) (uint16, bool) {
	if raw == "" {
		return 0, false
	}
	if strings.TrimSpace(raw) != raw {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 16)
	if err != nil || value > 10_000 || strconv.FormatUint(value, 10) != raw {
		return 0, false
	}
	return uint16(value), true
}

func parseAllowlist(raw string) (map[uuid.UUID]struct{}, bool) {
	out := make(map[uuid.UUID]struct{})
	if raw == "" {
		return out, true
	}
	for _, field := range strings.Split(raw, ",") {
		token := strings.TrimSpace(field)
		id, err := uuid.Parse(token)
		if err != nil || id.String() != token || id == uuid.Nil {
			return nil, false
		}
		if _, duplicate := out[id]; duplicate {
			return nil, false
		}
		out[id] = struct{}{}
	}
	return out, true
}

func fullSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// rolloutID is the bounded-cardinality identity of one exact rollout stage.
// It deliberately excludes account and cohort bucket, so every selected
// evaluation in the same immutable release/configuration shares one metric
// dimension value. Ninety-six bits keeps the dimension compact while making a
// collision between the small number of reviewed release stages negligible.
func rolloutID(
	coreManifestSHA string,
	billingSHA string,
	component Component,
	mode Mode,
	basisPoints uint16,
) string {
	payload := strings.Join([]string{
		coreManifestSHA,
		billingSHA,
		string(component),
		string(mode),
		strconv.FormatUint(uint64(basisPoints), 10),
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:12])
}
