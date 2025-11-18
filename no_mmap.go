//go:build !mmap

package fs

import (
	"encoding/gob"
	"os"

	"github.com/go-ap/errors"
)

func writeBinFile(root *os.Root, path string, bmp any) error {
	if root == nil {
		return errNotOpen
	}
	f, err := root.OpenFile(path, defaultNewFileFlags, defaultFilePerm)
	if err != nil {
		return errors.NewNotFound(err, "not found")
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(bmp)
}

func loadBinFromFile(root *os.Root, path string, bmp any) (err error) {
	if root == nil {
		return errNotOpen
	}
	f, err := root.OpenFile(path, os.O_RDONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewDecoder(f).Decode(bmp)
}
