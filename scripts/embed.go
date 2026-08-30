// Package scripts embeds the operator SQL that ships with the binaries.
//
// The preconditions are embedded rather than read from disk for two reasons.
// Under Lambda the working directory is /var/task and a relative path does not
// resolve; and embedding guarantees that what a binary executes is what a
// reviewer reads in the repository, with no second copy to drift.
package scripts

import _ "embed"

// LegacyDropPreconditions is scripts/legacy-drop-preconditions.sql.
//
// Read-only by construction and asserted so by a test in
// internal/architecture. It answers, per legacy collector, whether production
// still holds state that a deletion would strand.
//
//go:embed legacy-drop-preconditions.sql
var LegacyDropPreconditions string

// BillingCensus is scripts/billing-census.sql.
//
// Read-only by construction and asserted so by the same test. It answers how
// much billing history production holds at all — the question that decides
// whether DESIGN §11's shadow gate is VACUOUS (the rail never carried
// traffic) or UNMET (history exists and the diagnostic cannot see it).
//
//go:embed billing-census.sql
var BillingCensus string
