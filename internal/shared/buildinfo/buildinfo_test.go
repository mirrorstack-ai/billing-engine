package buildinfo

import "testing"

// An unstamped build must say so rather than report a plausible-looking
// identity, because docs/VERIFICATION.md §2 makes "unknown" the trigger
// for an executor to refuse to execute. A default that looked stamped
// would disarm that refusal on every developer build.
func TestUnstampedBuildIsNotIdentified(t *testing.T) {
	if Identified() {
		t.Fatal("a build with no link-time stamp reported itself as identified")
	}
	info := Current()
	if info.Commit != Unknown || info.Artifact != Unknown {
		t.Fatalf("unstamped build reported commit=%q artifact=%q, want both %q",
			info.Commit, info.Artifact, Unknown)
	}
	if info.Identified {
		t.Fatal("Info.Identified disagrees with Identified()")
	}
}

func TestIsStamped(t *testing.T) {
	cases := map[string]bool{
		"78b5c69fa1":          true,
		"billing-account-api": true,
		Unknown:               false,
		"":                    false,
		"   ":                 false,
		"  unknown  ":         false,
	}
	for value, want := range cases {
		if got := isStamped(value); got != want {
			t.Errorf("isStamped(%q) = %v, want %v", value, got, want)
		}
	}
}

// identified() requires both commit and artifact, because either alone
// leaves a real ambiguity: a commit with no artifact cannot be tied to
// the upload that is running, and an artifact with no commit cannot be
// tied to source.
func TestIdentifiedNeedsBothCommitAndArtifact(t *testing.T) {
	restore := func(c, a string) { commit, artifact = c, a }
	defer restore(commit, artifact)

	cases := []struct {
		commit, artifact string
		want             bool
	}{
		{"78b5c69", "billing-account-api", true},
		{"78b5c69", Unknown, false},
		{Unknown, "billing-account-api", false},
		{Unknown, Unknown, false},
	}
	for _, tc := range cases {
		commit, artifact = tc.commit, tc.artifact
		if got := identified(); got != tc.want {
			t.Errorf("identified() with commit=%q artifact=%q = %v, want %v",
				tc.commit, tc.artifact, got, tc.want)
		}
	}
}
