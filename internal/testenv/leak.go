//go:build integration

package testenv

import (
	"testing"

	"go.uber.org/goleak"
)

// NoLeaks fails t if goroutines started during the test are still running after all of
// t's other cleanups. Call it first in the test so its cleanup runs last. Goroutines that
// existed before (testcontainers, shared clients) are ignored.
//
// goleak sees every goroutine in the process, so do not combine NoLeaks with t.Parallel.
func NoLeaks(t *testing.T) {
	t.Helper()
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
}
