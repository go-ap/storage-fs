//go:build conformance

package fs

import (
	"testing"

	"git.sr.ht/~mariusor/lw"
	conformance "github.com/go-ap/storage-conformance-suite"
)

func initStorage(t *testing.T) conformance.ActivityPubStorage {
	l := lw.Dev(lw.SetOutput(t.Output()))
	storage, err := New(Config{Path: t.TempDir(), Logger: l, EnableOptimizedFiltering: true})
	if err != nil {
		t.Fatalf("unable to initialize storage: %s", err)
	}
	return storage
}

func Test_Conformance(t *testing.T) {
	conformance.Suite(
		conformance.TestActivityPub, conformance.TestMetadata,
		conformance.TestKey, conformance.TestOAuth, conformance.TestPassword,
	).Run(t, initStorage(t))
}
