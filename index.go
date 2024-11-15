package fs

import (
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
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
	index.ByPreferredUsername, index.ByActor, index.ByObject /*, index.ByCollection*/)

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
		}
	}
	return &b
}

// searchIndex does a fast search for the received filters.
func (r *repo) searchIndex(col vocab.Item, ff ...filters.Check) (vocab.ItemCollection, error) {
	if r.index == nil {
		return nil, cacheDisabled
	}

	if len(ff) == 0 {
		return nil, errors.Errorf("nil filters for index search")
	}

	i := r.index

	i.w.RLock()
	defer i.w.RUnlock()

	idxPath := r.collectionIndexStoragePath(col.GetLink())

	bmp := filters.Checks(ff).IndexMatch(i.all)
	colBmp := roaring.Bitmap{}
	if err := r.loadBitmapFromFile(idxPath, &colBmp); err == nil {
		bmp.And(&colBmp)
	}

	result := make(vocab.ItemCollection, 0, bmp.GetCardinality())
	colBmp.Iterate(func(x uint32) bool {
		if ip, ok := i.ref[x]; ok {
			it, err := loadItemFromPath(getObjectKey(filepath.Join(r.path, ip)))
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

func (r *repo) collectionIndexStoragePath(col vocab.IRI) string {
	return filepath.Join(r.itemStoragePath(col), _indexDirName)
}

func getIndexKey(typ index.Type) string {
	switch typ {
	case index.ByType:
		return ".type.gob"
	case index.ByName:
		return ".name.gob"
	case index.ByPreferredUsername:
		return ".preferredUsername.gob"
	case index.BySummary:
		return ".summary.gob"
	case index.ByContent:
		return ".content.gob"
	case index.ByActor:
		return ".actor.gob"
	case index.ByObject:
		return ".object.gob"
	case index.ByRecipients:
		return ".recipients.gob"
	case index.ByAttributedTo:
		return ".attributedTo.gob"
	case index.ByCollection:
		return ".items.gob"
	}
	return ""
}

const _refName = ".ref.gob"

func (r *repo) writeBitmapFile(path string, bmp any) error {
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

func saveIndex(r *repo) error {
	if r.index == nil {
		return nil
	}

	idxPath := r.indexStoragePath()
	_ = mkDirIfNotExists(idxPath)
	r.index.w.Lock()
	defer r.index.w.Unlock()

	errs := make([]error, 0, len(r.index.all))
	for typ, bmp := range r.index.all {
		if err := r.writeBitmapFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := r.writeBitmapFile(filepath.Join(idxPath, _refName), r.index.ref); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (r *repo) loadBitmapFromFile(path string, bmp any) error {
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

func loadIndex(r *repo) error {
	if r.index == nil {
		return nil
	}

	r.index.w.Lock()
	defer r.index.w.Unlock()

	errs := make([]error, 0, len(r.index.all))
	idxPath := r.indexStoragePath()
	for typ, bmp := range r.index.all {
		if err := r.loadBitmapFromFile(filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := r.loadBitmapFromFile(filepath.Join(idxPath, _refName), &r.index.ref); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

var cacheDisabled = errors.NotImplementedf("index is disabled")

func (r *repo) onCollectionIndex(col, it vocab.Item, fn func(*roaring.Bitmap, uint32)) error {
	if r.index == nil {
		return cacheDisabled
	}
	hashFn := index.HashFn
	if hashFn == nil {
		return cacheDisabled
	}

	idxPath := r.collectionIndexStoragePath(col.GetLink())

	bmp := roaring.Bitmap{}
	if err := r.loadBitmapFromFile(idxPath, &bmp); err != nil {
		bmp = *roaring.New()
	}

	fn(&bmp, hashFn(it.GetLink()))

	return r.writeBitmapFile(idxPath, &bmp)
}

func (r *repo) removeFromCollectionIndex(col, it vocab.Item) error {
	return r.onCollectionIndex(col, it, (*roaring.Bitmap).Remove)
}

func (r *repo) addToCollectionIndex(col, it vocab.Item) error {
	return r.onCollectionIndex(col, it, (*roaring.Bitmap).Add)
}

func (r *repo) removeFromObjectIndex(it vocab.Item, path string) error {
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
	case vocab.ActorTypes.Contains(it.GetType()):
		if _, err := in.all[index.ByPreferredUsername].Add(it); err != nil {
			errs = append(errs, err)
		}
	}

	type remover interface {
		Remove(vocab.LinkOrIRI) error
	}
	// NOTE(marius): all objects should get added to these indexes
	for _, gi := range allIndexTypes {
		if rem, ok := in.all[gi].(remover); ok {
			if err := rem.Remove(it); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}

	return errors.Join(errs...)
}

func (r *repo) addToObjectIndex(it vocab.Item, path string) error {
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
		if d.Type().IsDir() {
			return nil
		}
		if d.Name() != objectKey {
			return nil
		}

		var it vocab.Item
		dir := filepath.Dir(path)
		maybeCol := filepath.Base(dir)
		iri := r.iriFromPath(dir)
		if storageCollectionPaths.Contains(vocab.CollectionPath(maybeCol)) {
			it, err = r.loadCollectionFromPath(filepath.Join(r.path, path), iri)
			_ = vocab.OnCollectionIntf(it, func(col vocab.CollectionInterface) error {
				for _, ob := range col.Collection() {
					if err = r.addToCollectionIndex(it, ob); err != nil {
						if errors.IsNotImplemented(err) {
							return fs.SkipAll
						}
						r.logger.Warnf("Unable to add item %s to index: %s", iri, err)
					}
				}
				return nil
			})
		} else {
			it, err = r.loadItemFromPath(filepath.Join(r.path, path))
		}
		if err != nil || vocab.IsNil(it) {
			return nil
		}
		if err = r.addToObjectIndex(it, dir); err != nil {
			if errors.IsNotImplemented(err) {
				return fs.SkipAll
			}
			r.logger.Warnf("Unable to add item %s to index: %s", iri, err)
		}
		r.logger.Debugf("Indexed: %s", it.GetLink())
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
