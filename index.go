package fs

import (
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/filters/index"
)

type bitmaps struct {
	w   sync.RWMutex
	ref map[uint32]string
	all map[index.Type]index.Indexable
}

var genericIndexTypes = []index.Type{
	index.ByType,
	index.ByRecipients, index.ByAttributedTo,
	index.ByName, index.BySummary, index.ByContent,
}

var allIndexTypes = append(genericIndexTypes,
	index.ByPreferredUsername, index.ByActor, index.ByObject, index.ByCollection)

func newBitmap(typ ...index.Type) *bitmaps {
	if len(typ) == 0 {
		typ = allIndexTypes
	}
	b := bitmaps{
		ref: make(map[uint32]string),
		all: make(map[index.Type]index.Indexable),
	}
	for _, tt := range typ {
		switch tt {
		case index.ByType:
			b.all[tt] = index.TokenBitmap(index.ExtractType)
		case index.ByName:
			b.all[tt] = index.TokenBitmap(index.ExtractName)
		case index.ByPreferredUsername:
			b.all[tt] = index.TokenBitmap(index.ExtractPreferredUsername)
		case index.BySummary:
			b.all[tt] = index.TokenBitmap(index.ExtractSummary)
		case index.ByContent:
			b.all[tt] = index.TokenBitmap(index.ExtractContent)
		case index.ByActor:
			b.all[tt] = index.TokenBitmap(index.ExtractActor)
		case index.ByObject:
			b.all[tt] = index.TokenBitmap(index.ExtractObject)
		case index.ByRecipients:
			b.all[tt] = index.TokenBitmap(index.ExtractRecipients)
		case index.ByAttributedTo:
			b.all[tt] = index.TokenBitmap(index.ExtractAttributedTo)
		case index.ByCollection:
			b.all[tt] = index.CollectionBitmap()
		}
	}
	return &b
}

// searchIndex does a fast search for the received filters.
func (r *repo) searchIndex(ff ...filters.Check) (vocab.ItemCollection, error) {
	if len(ff) == 0 {
		return nil, errors.Errorf("nil filters for index search")
	}

	i := r.index
	i.w.RLock()
	defer i.w.RUnlock()

	bmp := filters.Checks(ff).IndexMatch(i.all)

	result := make(vocab.ItemCollection, 0, bmp.GetCardinality())
	bmp.Iterate(func(x uint32) bool {
		if ipath, ok := i.ref[x]; ok {
			fullPath := getObjectKey(filepath.Join(r.path, ipath))
			it, err := r.loadItem(fullPath, r.iriFromPath(fullPath))
			if err != nil {
				return true
			}
			result = append(result, it)
		}
		return true
	})

	return result, nil
}

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
	case index.ByCollection:
		return ".items.igob"
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

	r.index.w.Lock()
	defer r.index.w.Unlock()

	errs := make([]error, 0, len(r.index.all))
	for typ, bmp := range r.index.all {
		if err := writeFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := writeFile(filepath.Join(idxPath, _refName), r.index.ref); err != nil {
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

	r.index.w.RLock()
	defer r.index.w.RUnlock()

	errs := make([]error, 0, len(r.index.all))
	idxPath := r.indexStoragePath()
	for typ, bmp := range r.index.all {
		if err := loadFromFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := loadFromFile(filepath.Join(idxPath, _refName), &r.index.ref); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

var cacheDisabled = errors.NotImplementedf("index is disabled")

func (r *repo) addToIndex(it vocab.Item, path string) error {
	if r.index == nil {
		return cacheDisabled
	}
	if vocab.IsNil(it) {
		return errors.NotFoundf("nil item")
	}
	in := r.index

	errs := make([]error, 0)
	switch {
	case vocab.ActivityTypes.Contains(it.GetType()):
		if _, err := in.all[index.ByActor].Add(it); err != nil {
			errs = append(errs, err)
		}
		if _, err := in.all[index.ByObject].Add(it); err != nil {
			errs = append(errs, err)
		}
	case vocab.IntransitiveActivityTypes.Contains(it.GetType()):
		if _, err := in.all[index.ByActor].Add(it); err != nil {
			errs = append(errs, err)
		}
	case vocab.CollectionTypes.Contains(it.GetType()):
		if _, err := in.all[index.ByCollection].Add(it); err != nil {
			errs = append(errs, err)
		}
	case vocab.ActorTypes.Contains(it.GetType()):
		if _, err := in.all[index.ByPreferredUsername].Add(it); err != nil {
			errs = append(errs, err)
		}
	}

	var itemRef uint32
	// NOTE(marius): all objects should get added to these indexes
	for _, gi := range genericIndexTypes {
		ir, err := in.all[gi].Add(it)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		itemRef = ir
	}
	in.ref[itemRef] = path

	return errors.Join(errs...)
}

func (r *repo) iriFromPath(p string) vocab.IRI {
	p = strings.Trim(strings.TrimSuffix(strings.Replace(p, r.path, "", 1), objectKey), "/")
	return vocab.IRI(fmt.Sprintf("https://%s", p))
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

	root := os.DirFS(r.path)
	err = fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if d.Name() != objectKey {
			return nil
		}

		var it vocab.Item
		dir := filepath.Dir(path)
		maybeCol := vocab.CollectionPath(filepath.Base(dir))
		if storageCollectionPaths.Contains(maybeCol) {
			it, err = r.loadCollectionFromPath(filepath.Join(r.path, path))
		} else {
			iri := r.iriFromPath(dir)
			it, err = r.loadItem(filepath.Join(r.path, path), iri)
		}
		if err != nil || vocab.IsNil(it) {
			return nil
		}
		if err = r.addToIndex(it, dir); err != nil {
			if errors.IsNotImplemented(err) {
				return fs.SkipAll
			}
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
