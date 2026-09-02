// Package buildinfo carries the identity of the running binary.
//
// docs/VERIFICATION.md §2: "You cannot tie a charge to a revision of
// this repository until the running service says which revision it is."
// At the 78b5c69 baseline it would not — the one unauthenticated route
// returned a fixed body carrying no identity, and the build stamped
// nothing, passing only -ldflags="-s -w".
//
// The values here are set at link time. They are deliberately plain
// strings with an "unknown" default rather than a build-time failure,
// because a developer running `go run` must still get a working binary
// — and because "unknown" is itself the answer §2 requires the service
// to give when it does not know.
package buildinfo

import "strings"

// Unknown is the literal a field carries when the build did not stamp
// it. docs/VERIFICATION.md §2 names this value, so it is part of the
// wire contract rather than a placeholder.
const Unknown = "unknown"

// Stamped at link time by .github/workflows/publish.yml. See Info for
// what each one means.
var (
	commit      = Unknown
	artifact    = Unknown
	role        = Unknown
	environment = Unknown
)

// Info is the identity a binary reports about itself.
type Info struct {
	// Commit is the Git commit the binary was built from.
	Commit string `json:"commit"`
	// Artifact is the published artifact name, which is what ties the
	// answer to a specific upload rather than to a source revision that
	// may have been built more than once.
	Artifact string `json:"artifact"`
	// Role is which binary this is. A deployment runs several from one
	// commit, and "which revision" is only half the question.
	Role string `json:"role"`
	// Environment holds no secret; it distinguishes a sandbox answer
	// from a production one.
	Environment string `json:"environment"`
	// Identified is false when any field above is unknown.
	Identified bool `json:"identified"`
}

// Current returns the running binary's identity.
func Current() Info {
	return Info{
		Commit:      commit,
		Artifact:    artifact,
		Role:        role,
		Environment: environment,
		Identified:  identified(),
	}
}

// Identified reports whether the build stamped a usable identity.
//
// docs/VERIFICATION.md §2: "An executor whose build identity reads
// `unknown` must refuse to execute." That refusal is enforced by the
// executor, not here; this is the predicate it asks. The check is on
// the commit and artifact only, since those are what tie an answer to
// a revision — a missing environment name makes a report less useful,
// not unverifiable.
func Identified() bool { return identified() }

func identified() bool {
	return isStamped(commit) && isStamped(artifact)
}

func isStamped(v string) bool {
	v = strings.TrimSpace(v)
	return v != "" && v != Unknown
}
