package credit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCoordinatorIfReadyDoesNotConstructOnFlagOffOrSchemaUnready(t *testing.T) {
	for _, name := range []string{"flag off", "schema unready"} {
		t.Run(name, func(t *testing.T) {
			buildCalls := 0
			coordinator := NewCoordinatorIfReady(false, func() *Coordinator {
				buildCalls++
				panic("credit runtime must remain structurally dark")
			})

			require.Nil(t, coordinator)
			require.Zero(t, buildCalls)
		})
	}
}

func TestNewCoordinatorIfReadyConstructsExactlyOnceWhenCapabilityReady(t *testing.T) {
	buildCalls := 0
	want := &Coordinator{}
	got := NewCoordinatorIfReady(true, func() *Coordinator {
		buildCalls++
		return want
	})

	require.Same(t, want, got)
	require.Equal(t, 1, buildCalls)
}
