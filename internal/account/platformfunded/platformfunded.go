// Package platformfunded answers one question: is this org one of the
// platform's own orgs (PORG), whose funding the platform carries itself?
//
// The answer is DERIVED, never stored: no column, table, setter route, RPC or
// operator identity exists for it. Membership is a pure function of one config
// list (PLATFORM_ORG_IDS, set by infrastructure as code) read once at process
// start, so nothing a request carries can add an org to the set.
//
// It only FILLS A GAP: an org whose funding already resolves follows the
// ordinary rules. It never opens an account, card, invoice or charge — a
// platform-funded org's apps register as UNBILLED roster rows (account_id NULL,
// migration 041), which every charge leg skips. Every use is logged under
// AuditEvent with the gate that relied on it and the org id.
package platformfunded

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

// EnvOrgIDs is the config key holding PORG: comma-separated org uuids.
// Unset or empty means no org is platform-funded (the production default).
const EnvOrgIDs = "PLATFORM_ORG_IDS"

// MaxOrgs bounds the list: PORG is the platform's own orgs, so a longer list
// is a misconfiguration, not a feature.
const MaxOrgs = 16

// AuditEvent is the stable log event name of one use of the attribute.
const AuditEvent = "platform_funded.used"

// The gates that consult the set; each is the "gate" field of its audit line.
const (
	GateRegisterApp  = "register_app"
	GateQuoteChange  = "quote_change"
	GateServiceState = "service_status"
)

// Set is the platform-org set. The zero value is empty and a Set is immutable
// after Parse: no method adds to it.
type Set struct {
	orgs map[uuid.UUID]struct{}
}

// Parse reads a comma-separated list of org uuids. It fails closed and is
// all-or-nothing: any malformed, nil-uuid or surplus entry returns the EMPTY
// set and an error, so a config typo can never widen the set.
func Parse(raw string) (Set, error) {
	orgs := make(map[uuid.UUID]struct{})
	for i, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := uuid.Parse(part)
		if err != nil || id == uuid.Nil {
			return Set{}, fmt.Errorf("%s: entry %d is not a non-nil uuid", EnvOrgIDs, i)
		}
		orgs[id] = struct{}{}
		if len(orgs) > MaxOrgs {
			return Set{}, fmt.Errorf("%s: more than %d orgs", EnvOrgIDs, MaxOrgs)
		}
	}
	return Set{orgs: orgs}, nil
}

// FromProcessEnv reads PORG from the process environment, for a binary's main.
// A rejected list is logged at ERROR and yields the empty set — fail closed,
// never a crash that would take a billing Lambda down over a config typo.
func FromProcessEnv() Set {
	set, err := Parse(os.Getenv(EnvOrgIDs))
	if err != nil {
		slog.Error("platform org list rejected: no org is platform-funded", "error", err)
		return Set{}
	}
	if len(set.orgs) > 0 {
		slog.Info("platform org set loaded", "orgs", len(set.orgs))
	}
	return set
}

// Funded reports whether orgID is in PORG. The nil uuid never is.
func (s Set) Funded(orgID uuid.UUID) bool {
	_, ok := s.orgs[orgID]
	return ok && orgID != uuid.Nil
}

// Len is the number of orgs in the set.
func (s Set) Len() int { return len(s.orgs) }

// Audit records one use of the attribute. Call it only on the path that
// actually relies on the attribute, never on a lookup that came back false.
func Audit(ctx context.Context, gate string, orgID uuid.UUID) {
	slog.InfoContext(ctx, "platform_funded used: platform carries this org's funding, nothing is charged",
		"event", AuditEvent, "gate", gate, "org_id", orgID.String())
}
