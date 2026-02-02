package fs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/filters/index"
)

type bitmaps struct {
	w   sync.RWMutex
	ref map[uint64]string
	all map[index.Type]index.Indexable
}

var genericIndexTypes = []index.Type{
	index.ByID, index.ByType,
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
		ref: make(map[uint64]string),
		all: make(map[index.Type]index.Indexable),
	}
	for _, tt := range typ {
		switch tt {
		case index.ByID:
			b.all[tt] = index.All()
		case index.ByType:
			b.all[tt] = index.NewTokenIndex(index.ExtractType)
		case index.ByName:
			b.all[tt] = index.NewTokenIndex(index.ExtractName)
		case index.ByPreferredUsername:
			b.all[tt] = index.NewTokenIndex(index.ExtractPreferredUsername)
		case index.BySummary:
			b.all[tt] = index.NewTokenIndex(index.ExtractSummary)
		case index.ByContent:
			b.all[tt] = index.NewTokenIndex(index.ExtractContent)
		case index.ByActor:
			b.all[tt] = index.NewTokenIndex(index.ExtractActor)
		case index.ByObject:
			b.all[tt] = index.NewTokenIndex(index.ExtractObject)
		case index.ByRecipients:
			b.all[tt] = index.NewTokenIndex(index.ExtractRecipients)
		case index.ByAttributedTo:
			b.all[tt] = index.NewTokenIndex(index.ExtractAttributedTo)
		}
	}
	return &b
}

// searchIndex does a fast search for the received filters.
func (r *repo) searchIndex(col vocab.Item, ff ...filters.Check) (vocab.ItemCollection, error) {
	if r.index == nil {
		return nil, indexDisabled
	}

	if len(ff) == 0 {
		return nil, errors.Errorf("nil filters for index search")
	}

	i := r.index

	i.w.RLock()
	defer i.w.RUnlock()

	idxPath := r.collectionIndexStoragePath(col.GetLink())

	bmp := filters.Checks(ff).IndexMatch(i.all)
	colBmp := roaring64.New()
	_ = loadBinFromFile(r.root, idxPath, colBmp)
	bmp.And(colBmp)
	if bmp.IsEmpty() {
		return nil, nil
	}

	result := make(vocab.ItemCollection, 0, bmp.GetCardinality())
	it := bmp.Iterator()
	for it.HasNext() {
		x := it.Next()
		if ip, ok := i.ref[x]; ok {
			prefix := r.root.Name()
			if !strings.Contains(ip, prefix) {
				ip = filepath.Join(prefix, ip)
			}
			ob, err := loadRawFromPath(r.root, getObjectKey(ip))
			if err != nil {
				continue
			}
			result = append(result, ob)
		}
	}

	return result, nil
}

const _indexDirName = ".index"

func (r *repo) collectionIndexStoragePath(col vocab.IRI) string {
	return filepath.Join(iriPath(col), _indexDirName)
}

func getIndexKey(typ index.Type) string {
	switch typ {
	case index.ByID:
		return ".all.gob"
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
	case index.ByInReplyTo:
	case index.ByPublished:
	case index.ByUpdated:
	default:
	}
	return ""
}

const _refName = ".ref.gob"

func (r *repo) saveIndex() error {
	if r.root == nil || r.index == nil {
		return nil
	}

	_ = mkDirIfNotExists(r.root, _indexDirName)

	return saveIndex(r.root, r.index, _indexDirName)
}

func saveIndex(root *os.Root, idx *bitmaps, idxPath string) error {
	if idx == nil {
		return nil
	}

	_ = mkDirIfNotExists(root, idxPath)

	idx.w.RLock()
	defer idx.w.RUnlock()

	errs := make([]error, 0, len(idx.all))
	for typ, bmp := range idx.all {
		ip := filepath.Join(idxPath, getIndexKey(typ))
		if err := writeBinFile(root, ip, bmp); err != nil {
			errs = append(errs, err)
		}
	}
	if err := writeBinFile(root, filepath.Join(idxPath, _refName), idx.ref); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (r *repo) loadIndex() error {
	if r == nil || r.index == nil {
		return nil
	}

	r.index.w.Lock()
	defer r.index.w.Unlock()

	errs := make([]error, 0, len(r.index.all))
	idxPath := _indexDirName
	for typ, bmp := range r.index.all {
		if err := loadBinFromFile(r.root, filepath.Join(idxPath, getIndexKey(typ)), bmp); err != nil {
			// NOTE(marius): if the root is not open, there's no need to try further
			if errors.Is(err, errNotOpen) {
				return err
			}
			errs = append(errs, err)
		}
	}
	if err := loadBinFromFile(r.root, filepath.Join(idxPath, _refName), &r.index.ref); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

var indexDisabled = errors.NotImplementedf("index is disabled")

func onCollectionBitmap(bmp *roaring64.Bitmap, it vocab.Item, fn func(*roaring64.Bitmap, uint64)) error {
	if bmp == nil {
		return indexDisabled
	}
	hashFn := index.HashFn
	if hashFn == nil {
		return indexDisabled
	}
	if fn != nil && !vocab.IsNil(it) {
		fn(bmp, hashFn(it.GetLink()))
	}
	return nil
}

func (r *repo) removeFromIndex(it vocab.Item, path string) error {
	if r == nil || r.index == nil {
		return indexDisabled
	}
	if vocab.IsNil(it) {
		return errors.NotFoundf("nil item")
	}
	in := r.index
	errs := make([]error, 0)
	typ := it.GetType()
	switch {
	case vocab.ActivityTypes.Match(typ):
		if iact, ok := in.all[index.ByActor]; ok {
			_ = iact.Add(it)
		}
		if iob, ok := in.all[index.ByObject]; ok {
			_ = iob.Add(it)
		}
	case vocab.IntransitiveActivityTypes.Match(typ):
		if iact, ok := in.all[index.ByActor]; ok {
			_ = iact.Add(it)
		}
	case vocab.ActorTypes.Match(typ):
		if ipu, ok := in.all[index.ByPreferredUsername]; ok {
			_ = ipu.Add(it)
		}
	}

	type remover interface {
		Remove(vocab.LinkOrIRI) error
	}
	// NOTE(marius): all objects should get added to these indexes
	for _, gi := range allIndexTypes {
		i, ok := in.all[gi]
		if !ok {
			continue
		}
		rem, ok := i.(remover)
		if !ok {
			continue
		}
		if err := rem.Remove(it); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return errors.Join(errs...)
}

func (r *repo) addToIndex(it vocab.Item, path string) error {
	if r == nil || r.index == nil {
		return indexDisabled
	}
	if vocab.IsNil(it) {
		return errors.NotFoundf("nil item")
	}
	in := r.index

	in.w.Lock()
	defer in.w.Unlock()

	typ := it.GetType()
	switch {
	case vocab.ActivityTypes.Match(typ):
		if iact, ok := in.all[index.ByActor]; ok {
			_ = iact.Add(it)
		}
		if iob, ok := in.all[index.ByObject]; ok {
			_ = iob.Add(it)
		}
	case vocab.IntransitiveActivityTypes.Match(typ):
		if iact, ok := in.all[index.ByActor]; ok {
			_ = iact.Add(it)
		}
	case vocab.ActorTypes.Match(typ):
		if ipu, ok := in.all[index.ByPreferredUsername]; ok {
			_ = ipu.Add(it)
		}
	}

	var itemRef uint64
	// NOTE(marius): all objects should get added to these indexes
	for _, gi := range genericIndexTypes {
		if ig, ok := in.all[gi]; ok {
			itemRef = ig.Add(it)
		}
	}
	in.ref[itemRef] = path

	return nil
}

func (r *repo) iriFromPath(p string) vocab.IRI {
	p = strings.Trim(strings.TrimSuffix(strings.Replace(p, r.root.Name(), "", 1), objectKey), "/")
	return vocab.IRI(fmt.Sprintf("https://%s", p))
}

func (r *repo) collectionBitmapOp(fn func(*roaring64.Bitmap, uint64), items ...vocab.Item) func(col vocab.CollectionInterface) error {
	return func(col vocab.CollectionInterface) error {
		iri := col.GetLink()
		idxPath := r.collectionIndexStoragePath(iri)

		bmp := roaring64.New()
		if err := loadBinFromFile(r.root, idxPath, bmp); err != nil {
			//r.logger.Warnf("Unable to load collection index %s: %s", iri, err)
		}

		wasEmpty := bmp.GetCardinality() == 0

		// NOTE(marius): this is terrible, we're using the same function for indexing a full collection
		// but also to add a single item to the collection index.
		if len(items) == 0 {
			items = col.Collection()
		}

		for _, ob := range items {
			if err := onCollectionBitmap(bmp, ob, fn); err != nil {
				if errors.IsNotImplemented(err) {
					return fs.SkipAll
				}
				r.logger.Warnf("Unable to add item %s to index: %s", iri, err)
			}
		}

		// NOTE(marius): if there was nothing in the bitmap, and we didn't add
		// anything either, we don't save the collection file.
		if isEmpty := bmp.GetCardinality() == 0; isEmpty {
			if wasEmpty {
				return nil
			}
			// NOTE(marius): if the collection wasn't empty and we removed the last item from it,
			// we can remove the collection index file.
			return os.RemoveAll(idxPath)
		}

		return writeBinFile(r.root, idxPath, bmp)
	}
}

func (r *repo) Reindex() (err error) {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if err = r.loadIndex(); err != nil {
		return indexDisabled
	}
	defer func() {
		err = r.saveIndex()
	}()

	root := r.root.FS()
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
			it, err = r.loadCollectionFromPath(filepath.Join(path), iri)
			if err == nil {
				err = vocab.OnCollectionIntf(it, r.collectionBitmapOp((*roaring64.Bitmap).Add))
			}
		} else {
			it, err = r.loadItemFromPath(filepath.Join(path))
		}
		if err != nil || vocab.IsNil(it) {
			return nil
		}
		if err = r.addToIndex(it, dir); err != nil {
			if errors.IsNotImplemented(err) {
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
