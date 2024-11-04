package fs

import (
	"encoding/gob"
	"io/fs"
	"os"
	"path/filepath"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters/index"
)

const _indexDirName = ".index"

func (r *repo) indexStoragePath() string {
	return filepath.Join(r.path, _indexDirName)
}

func getIndexKey(typ index.Type) string {
	switch typ {
	case index.ByType:
		return ".type.igob"
	case index.ByName:
		return ".name.igob"
	case index.ByPreferredUsername:
		return ".preferredUsername.igob"
	case index.BySummary:
		return ".summary.igob"
	case index.ByContent:
		return ".content.igob"
	case index.ByActor:
		return ".actor.igob"
	case index.ByObject:
		return ".object.igob"
	case index.ByRecipients:
		return ".recipients.igob"
	case index.ByAttributedTo:
		return ".attributedTo.igob"
	}
	return ""
}

const _refName = ".ref.gob"

func saveIndex(r *repo) error {
	if r.index == nil {
		return nil
	}

	idxPath := r.indexStoragePath()
	_ = mkDirIfNotExists(idxPath)
	writeFile := func(path string, bmp any) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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

	errs := make([]error, 0, len(r.index.Indexes))
	for typ, bmp := range r.index.Indexes {
		if err := writeFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := writeFile(filepath.Join(idxPath, _refName), r.index.Ref); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func loadIndex(r *repo) error {
	if r.index == nil {
		return nil
	}
	loadFromFile := func(path string, bmp any) error {
		f, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			r.logger.Warnf("Unable to %s", asPathErr(err, r.path))
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				r.logger.Warnf("Unable to close file: %s", asPathErr(err, r.path))
			}
		}()
		return gob.NewDecoder(f).Decode(bmp)
	}

	errs := make([]error, 0, len(r.index.Indexes))
	idxPath := r.indexStoragePath()
	for typ, bmp := range r.index.Indexes {
		if err := loadFromFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := loadFromFile(filepath.Join(idxPath, _refName), &r.index.Ref); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *repo) addToIndex(it vocab.Item) error {
	if r.index == nil {
		return nil
	}
	in := r.index

	errs := make([]error, 0)
	switch {
	case vocab.ActivityTypes.Contains(it.GetType()):
		if err := in.Indexes[index.ByActor].Add(it); err != nil {
			errs = append(errs, err)
		}
		if err := in.Indexes[index.ByObject].Add(it); err != nil {
			errs = append(errs, err)
		}
	case vocab.IntransitiveActivityTypes.Contains(it.GetType()):
		if err := in.Indexes[index.ByActor].Add(it); err != nil {
			errs = append(errs, err)
		}
	}
	if err := in.Add(it); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *repo) Reindex() (err error) {
	if err = r.Open(); err != nil {
		return err
	}
	defer r.Close()

	if err = loadIndex(r); err != nil {
		r.logger.Warnf("Unable to load indexes: %s", err)
	}
	defer func() {
		err = saveIndex(r)
	}()

	err = fs.WalkDir(os.DirFS(r.path), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if d.Name() != objectKey {
			return nil
		}
		it, err := loadItemFromPath(filepath.Join(r.path, path))
		if err != nil {
			return nil
		}
		if err = r.addToIndex(it); err != nil {
			r.logger.Warnf("Unable to add item %s to index: %s", it.GetLink(), err)
		}
		r.logger.Debugf("Indexed: %s", it.GetLink())
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
