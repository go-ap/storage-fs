//go:build !mmap

package fs

import (
	"encoding/gob"
	"os"

	"github.com/go-ap/errors"
)

func (r *repo) writeBinFile(path string, bmp any) error {
	f, err := r.root.OpenFile(path, defaultNewFileFlags, defaultFilePerm)
	if err != nil {
		r.logger.Warnf("%s not found", path)
		return errors.NewNotFound(asPathErr(err, r.path), "not found")
	}
	defer func() {
		if err := f.Close(); err != nil {
			r.logger.Warnf("Unable to close file: %s", asPathErr(err, r.path))
		}
	}()
	return gob.NewEncoder(f).Encode(bmp)
}

func (r *repo) loadBinFromFile(path string, bmp any) (err error) {
	f, err := r.root.OpenFile(path, os.O_RDONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	defer func() {
		err = f.Close()
	}()
	if err = gob.NewDecoder(f).Decode(bmp); err != nil {
		return err
	}
	return nil
}
