package predicate

import (
	"testing"
	"time"
)

func TestZZRefuteRevokeZero(t *testing.T) {
	state := permittedState(t)
	base := Evaluate(state)
	t.Logf("baseline: Permitted=%v refused=%v", base.Permitted, base.Refused)
	state.Authorization = state.Authorization.Revoke(time.Time{})
	v := Evaluate(state)
	t.Logf("after Revoke(zero): Permitted=%v refused=%v", v.Permitted, v.Refused)
	d := state.Authorization.Permits(state.Intent, state.AuthorizationKind, state.Now, 0)
	t.Logf("Permits: %+v", d)
	if v.Permitted {
		t.Fatal("FINDING CONFIRMED: Revoke(zero) still permits")
	}
}
