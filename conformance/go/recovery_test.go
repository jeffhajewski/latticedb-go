package conformance

import "testing"

type RecoveryHarness interface {
	SimulateCrash(dbPath string) error
}

var testRecoveryHarness RecoveryHarness

func currentRecoveryHarness(t *testing.T) RecoveryHarness {
	t.Helper()
	if testRecoveryHarness == nil {
		t.Skip("conformance recovery harness not configured")
	}
	return testRecoveryHarness
}
