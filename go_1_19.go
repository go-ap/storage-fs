//go:build !go1.20

package fs

import "path/filepath"

var skipAll = filepath.SkipDir
