package fs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

var tempDir = new(testing.T).TempDir()

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	testCWD = filepath.Dir(filename)

	defer os.RemoveAll(tempDir)

	if st := m.Run(); st != 0 {
		os.Exit(st)
	}
}
