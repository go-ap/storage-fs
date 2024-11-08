package fs

import (
	"os"

	"github.com/go-ap/cache"
)

func Clean(conf Config) error {
	return os.RemoveAll(conf.Path)
}

func Bootstrap(_ Config) error {
	return nil
}

func (r *repo) Reset() {
	r.cache = cache.New(true)
	if r.index != nil {
		r.index = newBitmap()
	}
}
