package credit

// NewCoordinatorIfReady is the structural production-construction gate for the
// optional credit runtime. ready must already include both the fail-closed env
// flag and the migration-048 schema capability probe.
//
// The builder is deliberately a closure: when ready is false it is not invoked,
// so Redis counters, migration-048 stores/projections, and notifiers cannot be
// constructed accidentally on the legacy or schema-unready path.
func NewCoordinatorIfReady(ready bool, build func() *Coordinator) *Coordinator {
	if !ready {
		return nil
	}
	if build == nil {
		panic("credit.NewCoordinatorIfReady: build must not be nil when ready")
	}
	return build()
}
