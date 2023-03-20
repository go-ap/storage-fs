package fs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	testCWD = filepath.Dir(filename)

	if st := m.Run(); st != 0 {
		os.Exit(st)
	}
}
