package paymentfact

// StatementPaidMarkDue is the one non-obvious rule behind the manual
// bank-transfer writer: "writes a manual_paid_mark fact when the recorded
// payments reach the statement's NT$ total" (billing-engine#313).
//
// billing-engine#267 (persisted statements, the operator paid-mark verb) has
// not merged in this tree — this function is the documented hook that writer
// adopts: it owns the threshold decision, so the operator verb only has to
// call RecordTx when this returns true, and a statement that is short by one
// cent writes nothing rather than an early fact nobody can idempotently
// replace.
//
// recordedMicros is deliberately a caller-supplied total, not a query this
// package runs itself — #267's persisted statement and its recorded-payments
// table do not exist yet, so there is nothing in this schema to query.
func StatementPaidMarkDue(recordedMicros, statementTotalMicros int64) bool {
	return recordedMicros >= statementTotalMicros && statementTotalMicros > 0
}
