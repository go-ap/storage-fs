package fs

import (
	"os"

	"github.com/go-ap/cache"
)

func Clean(conf Config) error {
	return os.RemoveAll(conf.Path)
}

func Bootstrap(conf Config) error {
	if _, err := os.Stat(conf.Path); err != nil {
		if !os.IsNotExist(err) {
			return err
		} else {
			return os.MkdirAll(conf.Path, defaultDirPerm)
		}
	}
	return nil
}

func (r *repo) Reset() {
	r.cache = cache.New(true)
	if r.index != nil {
		r.index = newBitmap()
	}
}
