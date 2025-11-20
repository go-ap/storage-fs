package fs

import (
	"bytes"
	"encoding/json"
	xerrors "errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"git.sr.ht/~mariusor/lw"
	"github.com/RoaringBitmap/roaring/roaring64"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

var errNotOpen = errors.Newf("repository not open")

var emptyLogger = lw.Dev()

type ItemFn func(vocab.Item) error
type Config struct {
	Path        string
	CacheEnable bool
	UseIndex    bool
	Logger      lw.Logger
}

var errMissingPath = errors.Newf("missing path in config")

// New returns a new repo repository
func New(c Config) (*repo, error) {
	if c.Path == "" {
		return nil, errMissingPath
	}
	p, err := getAbsStoragePath(c.Path)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(p, defaultDirPerm); err != nil {
		return nil, err
	}

	b := repo{
		path:   p,
		logger: emptyLogger,
		cache:  cache.New(c.CacheEnable),
	}
	if c.Logger != nil {
		b.logger = c.Logger
	}
	if c.UseIndex {
		b.index = newBitmap()
	}
	return &b, nil
}

type repo struct {
	path   string
	root   *os.Root
	index  *bitmaps
	cache  cache.CanStore
	logger lw.Logger
}

// Open
func (r *repo) Open() error {
	root, err := os.OpenRoot(r.path)
	if err != nil {
		return err
	}
	r.root = root
	return nil
}

func (r *repo) close() error {
	if r.root != nil {
		return r.root.Close()
	}
	return nil
}

// Close
func (r *repo) Close() {
	_ = r.close()
}

// Load
func (r *repo) Load(i vocab.IRI, f ...filters.Check) (vocab.Item, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	it, err := r.loadFromIRI(i, f...)
	if err != nil {
		return nil, err
	}
	return it, nil
}

// Create
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if vocab.IsNil(col) {
		return col, errors.Newf("Unable to operate on nil element")
	}
	if len(col.GetLink()) == 0 {
		return col, errors.Newf("Invalid collection, it does not have a valid IRI")
	}
	return saveCollection(r, col)
}

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if vocab.IsNil(it) {
		return nil, errors.Newf("Unable to save nil element")
	}

	return save(r, it)
}

// RemoveFrom removes the items from the colIRI collection.
func (r *repo) RemoveFrom(colIRI vocab.IRI, items ...vocab.Item) error {
	// NOTE(marius): We make sure the collection exists (unless it's a hidden collection)
	itPath := iriPath(colIRI)
	col, err := r.loadItemFromPath(getObjectKey(itPath))
	if err != nil && !isHiddenCollectionKey(itPath) {
		return err
	}

	linkPath := iriPath(colIRI)
	for _, it := range items {
		fullLink := path.Join(linkPath, url.PathEscape(iriPath(it.GetLink())))
		err = onCollection(r, col, it, func(p string) error {
			return r.root.RemoveAll(fullLink)
		})
		if err != nil {
			return err
		}
	}

	if orderedCollectionTypes.Contains(col.GetType()) {
		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			if c.TotalItems <= uint(len(items)) {
				c.TotalItems = 0
			} else {
				c.TotalItems -= uint(len(items))
			}
			c.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Contains(col.GetType()) {
		err = vocab.OnCollection(col, func(c *vocab.Collection) error {
			if c.TotalItems <= uint(len(items)) {
				c.TotalItems = 0
			} else {
				c.TotalItems -= uint(len(items))
			}
			c.Items = nil
			return nil
		})
	}

	if _, err = save(r, col); err != nil {
		return err
	}

	for _, it := range items {
		err = vocab.OnCollectionIntf(col, r.collectionBitmapOp((*roaring64.Bitmap).Remove, it))
		if err != nil && !errors.Is(err, indexDisabled) {
			r.logger.Errorf("unable to remote item %s from collection index: %s", it.GetLink(), err)
		}

		r.removeFromCache(it.GetLink())
	}
	return nil
}

func isSymLink(fi os.FileInfo) bool {
	if fi == nil {
		return false
	}
	return fi.Mode()&os.ModeSymlink == os.ModeSymlink
}

func iriPath(iri vocab.IRI) string {
	u, err := iri.URL()
	if err != nil {
		return ""
	}

	pieces := make([]string, 0)
	pieces = append(pieces, "./")
	if h := u.Host; h != "" {
		pieces = append(pieces, h)
	}
	if p := u.Path; p != "" && p != "/" {
		pieces = append(pieces, p)
	}
	//if u.ForceQuery || u.RawQuery != "" {
	//	pieces = append(pieces, url.PathEscape(u.RawQuery))
	//}
	if u.Fragment != "" {
		pieces = append(pieces, strings.ReplaceAll(u.Fragment, "#", ""))
	}
	return filepath.Join(pieces...)
}

func saveCollection(r *repo, col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	it, err := save(r, col)
	if err != nil {
		return nil, err
	}

	err = vocab.OnOrderedCollection(it, func(c *vocab.OrderedCollection) error {
		col = c
		return nil
	})
	return col, err
}

func createCollection(r *repo, colIRI vocab.IRI, owner vocab.Item) (vocab.CollectionInterface, error) {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if !vocab.IsNil(owner) {
		col.AttributedTo = owner.GetLink()
	}
	return saveCollection(r, &col)
}

var orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
var collectionTypes = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}

// AddTo adds the items to the colIRI collection.
func (r *repo) AddTo(colIRI vocab.IRI, items ...vocab.Item) error {
	// NOTE(marius): We make sure the collection exists (unless it's a hidden collection)
	itPath := iriPath(colIRI)
	col, err := r.loadItemFromPath(getObjectKey(itPath))
	if err != nil && !isHiddenCollectionKey(itPath) {
		return err
	}
	if col == nil && isHiddenCollectionKey(itPath) {
		// NOTE(marius): for hidden collections we might not have the __raw file on disk, so we just try to create it
		// Here we assume the owner can be inferred from the collection IRI, but that's just a FedBOX implementation
		// detail. We should find a different way to pass collection owner - maybe the processing package checks for
		// existence of the blocked collection, and explicitly creates it if it doesn't.
		maybeOwner, _ := vocab.Split(colIRI)
		if col, err = createCollection(r, colIRI, maybeOwner); err != nil {
			return err
		}
	}

	linkPath := iriPath(colIRI)
	for _, it := range items {
		if vocab.IsIRI(it) {
			it, err = r.loadOneFromIRI(it.GetLink())
			if err != nil {
				return errors.NewNotFound(err, "invalid item to add to collection")
			}
		}

		// we create a symlink to the persisted object in the current collection
		err = onCollection(r, col, it, func(p string) error {
			if err := mkDirIfNotExists(r.root, p); err != nil {
				return errors.Annotatef(err, "Unable to create collection folder %s", p)
			}
			// NOTE(marius): if 'it' IRI belongs to the 'col' collection we can skip symlinking it
			if it.GetLink().Contains(col.GetLink(), true) {
				return nil
			}

			fullLink := path.Join(linkPath, url.PathEscape(iriPath(it.GetLink())))
			if fi, _ := r.root.Stat(fullLink); fi != nil {
				if isSymLink(fi) {
					return nil
				}
			}

			itOriginalPath := iriPath(it.GetLink())
			relativePath, err := filepath.Rel(linkPath, itOriginalPath)
			if err != nil {
				return err
			}
			// NOTE(marius): we can't use hard links as we're linking to folders :(
			// This would have been tremendously easier (as in, not having to compute paths) with hard-links.
			if err = r.root.Symlink(relativePath, fullLink); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return errors.Annotatef(err, "unable to symlink object into collection")
		}
	}

	if orderedCollectionTypes.Contains(col.GetType()) {
		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			c.TotalItems += uint(len(items))
			c.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Contains(col.GetType()) {
		err = vocab.OnCollection(col, func(c *vocab.Collection) error {
			c.TotalItems += uint(len(items))
			c.Items = nil
			return nil
		})
	}

	if _, err = save(r, col); err != nil {
		return err
	}

	for _, it := range items {
		err = vocab.OnCollectionIntf(col, r.collectionBitmapOp((*roaring64.Bitmap).Add, it))
		if err != nil && !errors.IsNotImplemented(err) {
			r.logger.Debugf("unable to add item %s to collection index: %s", it.GetLink(), err)
		}
	}
	return nil
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if vocab.IsNil(it) {
		return nil
	}
	return r.delete(it)
}

func (r *repo) delete(it vocab.Item) error {
	if vocab.IsItemCollection(it) {
		if col, ok := it.(vocab.ItemCollection); ok {
			for _, it := range col {
				if err := deleteItem(r, it); err != nil {
					r.logger.Debugf("Unable to remove item %s", it.GetLink())
				}
			}
		}
	}
	return deleteItem(r, it.GetLink())
}

var storageCollectionPaths = append(filters.FedBOXCollections, append(vocab.OfActor, vocab.OfObject...)...)

func isStorageCollectionKey(p string) bool {
	lst := vocab.CollectionPath(filepath.Base(p))
	return storageCollectionPaths.Contains(lst)
}

func isHiddenCollectionKey(p string) bool {
	lst := vocab.CollectionPath(filepath.Base(p))
	return filters.HiddenCollections.Contains(lst)
}

// createCollections
func createCollections(r *repo, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		_ = vocab.OnActor(it, func(p *vocab.Actor) error {
			p.Inbox, _ = createCollectionInPath(r, p.Inbox, p)
			p.Outbox, _ = createCollectionInPath(r, p.Outbox, p)
			p.Followers, _ = createCollectionInPath(r, p.Followers, p)
			p.Following, _ = createCollectionInPath(r, p.Following, p)
			p.Liked, _ = createCollectionInPath(r, p.Liked, p)
			// NOTE(marius): shadow creating hidden collections for Blocked and Ignored items
			_, _ = createCollectionInPath(r, filters.BlockedType.Of(p), p)
			_, _ = createCollectionInPath(r, filters.IgnoredType.Of(p), p)
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		o.Replies, _ = createCollectionInPath(r, o.Replies, o)
		o.Likes, _ = createCollectionInPath(r, o.Likes, o)
		o.Shares, _ = createCollectionInPath(r, o.Shares, o)
		return nil
	})
}

const (
	objectKey   = "__raw"
	metaDataKey = "__meta_data"
)

func getMetadataKey(p string) string {
	return path.Join(p, metaDataKey)
}

func getObjectKey(p string) string {
	return path.Join(p, objectKey)
}

func createCollectionInPath(r *repo, it, owner vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}
	itPath := iriPath(it.GetLink())

	colObject, err := r.loadItemFromPath(getObjectKey(itPath))
	if colObject == nil {
		it, err = createCollection(r, it.GetLink(), owner)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "saving collection object is not done")
	}

	return it.GetLink(), asPathErr(mkDirIfNotExists(r.root, itPath))
}

func (r *repo) removeFromCache(iri vocab.IRI) {
	if r.cache == nil {
		return
	}
	r.cache.Delete(iri.GetLink())
}

func getAbsStoragePath(p string) (string, error) {
	if !filepath.IsAbs(p) {
		var err error
		p, err = filepath.Abs(p)
		if err != nil {
			return "", err
		}
	}
	if fi, err := os.Stat(p); err != nil {
		return "", err
	} else if !fi.IsDir() {
		return "", errors.NotValidf("path %s is invalid for storage", p)
	}
	return p, nil
}

func deleteItem(r *repo, it vocab.Item) error {
	if it.GetLink() == "" {
		return nil
	}
	itemPath := iriPath(it.GetLink())

	if err := r.root.RemoveAll(itemPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	r.removeFromCache(it.GetLink())
	return nil
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	if err := createCollections(r, it); err != nil {
		return it, errors.Annotatef(err, "could not create object's collections")
	}
	_ = r.loadIndex()

	defer func() {
		_ = r.saveIndex()
	}()

	writeSingleObjFn := func(it vocab.Item) (vocab.Item, error) {
		itPath := iriPath(it.GetLink())
		_ = mkDirIfNotExists(r.root, itPath)

		entryBytes, err := encodeItemFn(it)
		if err != nil {
			return it, errors.Annotatef(err, "could not marshal object")
		}

		if err = putRaw(r.root, getObjectKey(itPath), entryBytes); err != nil {
			return it, err
		}

		if err = r.addToIndex(it, itPath); err != nil && !errors.IsNotImplemented(err) {
			r.logger.Errorf("unable to add item %s to index: %s", it.GetLink(), err)
		}

		r.setToCache(it)
		return it, nil
	}

	if vocab.IsItemCollection(it) {
		err := vocab.OnItemCollection(it, func(col *vocab.ItemCollection) error {
			m := make([]error, 0)
			for i, ob := range *col {
				saved, err := writeSingleObjFn(ob)
				if err == nil {
					(*col)[i] = saved
				} else {
					m = append(m, err)
				}
			}
			if len(m) > 0 {
				return xerrors.Join(m...)
			}
			return nil
		})
		return it, err
	}
	return writeSingleObjFn(it)
}

func onCollection(r *repo, col vocab.Item, it vocab.Item, fn func(p string) error) error {
	if vocab.IsNil(it) {
		return errors.Newf("Unable to operate on nil element")
	}
	if vocab.IsNil(col) || len(col.GetLink()) == 0 {
		return errors.Newf("Unable to find collection")
	}
	if len(it.GetLink()) == 0 {
		return errors.Newf("Invalid collection, it does not have a valid IRI")
	}

	itPath := iriPath(col.GetLink())
	if err := fn(itPath); err != nil {
		if os.IsExist(err) {
			return errors.NewConflict(err, "%s already exists in collection %s", it.GetID(), itPath)
		} else if !os.IsNotExist(err) {
			return errors.Annotatef(err, "Unable to save entries to collection %s", itPath)
		}
	}
	r.removeFromCache(col.GetLink())
	return nil
}

func loadRaw(root *os.Root, itPath string) ([]byte, error) {
	fi, err := root.Open(itPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = fi.Close()
	}()

	return io.ReadAll(fi)
}

func loadFromRaw(raw []byte) (vocab.Item, error) {
	if raw == nil || len(raw) == 0 {
		// TODO(marius): log this instead of stopping the iteration and returning an error
		return nil, errors.Errorf("empty raw item")
	}
	return decodeItemFn(raw)
}

func (r *repo) loadOneFromIRI(i vocab.IRI) (vocab.Item, error) {
	col, err := r.loadItemFromPath(getObjectKey(iriPath(i)))
	if err != nil {
		return nil, err
	}
	if col == nil {
		return nil, errors.NotFoundf("nothing found")
	}
	if vocab.IsIRI(col) {
		return nil, errors.Conflictf("%s could not be loaded from disk", col)
	}
	if col.IsCollection() {
		var result vocab.Item
		_ = vocab.OnCollectionIntf(col, func(col vocab.CollectionInterface) error {
			result = col.Collection().First()
			return nil
		})
		if vocab.IsIRI(result) && result.GetLink().Equals(i.GetLink(), false) {
			// NOTE(marius): this covers the case where we ended up with the same IRI
			return nil, errors.NotFoundf("nothing found")
		}
		return result, nil
	}
	return col, nil
}

func loadFilteredPropsForActor(r *repo, fil ...filters.Check) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r, fil...))
	}
}

func loadFilteredPropsForObject(r *repo, fil ...filters.Check) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				ob, err := r.loadItemFromPath(getObjectKey(iriPath(t.GetLink())))
				if err != nil {
					continue
				}
				if ob = filters.TagChecks(fil...).Run(ob); ob == nil {
					continue
				}
				(*col)[i] = ob
			}
			return nil
		})
	}
}

func dereferenceItemAndFilter(r *repo, ob vocab.Item, fil ...filters.Check) (vocab.Item, error) {
	if vocab.IsNil(ob) {
		return ob, nil
	}

	if !vocab.IsIRI(ob) {
		return ob, nil
	}
	itPath := iriPath(ob.GetLink())
	o, err := r.loadItemFromPath(getObjectKey(itPath), fil...)
	if err != nil {
		return ob, nil
	}

	return o, nil
}

func loadFilteredPropsForActivity(r *repo, fil ...filters.Check) func(a *vocab.Activity) error {
	objectChecks := filters.ObjectChecks(fil...)
	return func(a *vocab.Activity) error {
		var err error
		if !vocab.IsNil(a.Object) {
			if a.ID.Equals(a.Object.GetLink(), false) {
				//r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
				return errors.BadGatewayf("invalid activity with id %s, referencing itself as an object: %s", a.ID, a.Object.GetLink())
			}
			if a.Object, err = dereferenceItemAndFilter(r, a.Object, objectChecks...); err != nil {
				return err
			}
		}
		intransitiveChecks := filters.IntransitiveActivityChecks(fil...)
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r, intransitiveChecks...))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo, fil ...filters.Check) func(a *vocab.IntransitiveActivity) error {
	//actorChecks := filters.ActorChecks(fil...)
	targetChecks := filters.TargetChecks(fil...)
	return func(a *vocab.IntransitiveActivity) error {
		var err error
		//if !vocab.IsNil(a.Actor) {
		//	if a.ID.Equal(a.Actor.GetLink(), false) {
		//		//r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
		//		return errors.BadGatewayf("invalid activity with id %s, referencing itself as an actor: %s", a.ID, a.Actor.GetLink())
		//	}
		//	if a.Actor, err = dereferenceItemAndFilter(r, a.Actor, actorChecks...); err != nil {
		//		return err
		//	}
		//}
		if !vocab.IsNil(a.Target) {
			if a.ID.Equals(a.Target.GetLink(), false) {
				//r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original object. (%s)", a.Type, a.ID)
				return errors.BadGatewayf("invalid activity with id %s, referencing itself as a target: %s", a.ID, a.Target.GetLink())
			}
			if a.Target, err = dereferenceItemAndFilter(r, a.Target, targetChecks...); err != nil {
				return err
			}
		}
		return vocab.OnObject(a, loadFilteredPropsForObject(r))
	}
}

func sanitizePath(p string) string {
	p = strings.TrimSuffix(p, objectKey)
	p = strings.TrimSuffix(p, metaDataKey)
	return strings.Trim(p, "/")
}

func asPathErr(err error) error {
	if err == nil {
		return nil
	}
	pe := new(fs.PathError)
	if ok := errors.As(err, &pe); ok {
		pe.Path = sanitizePath(pe.Path)
		return pe
	}
	return err
}

func getOriginalIRI(root *os.Root, p string) (vocab.Item, error) {
	// NOTE(marius): if the __raw file wasn't found, but the path corresponds to a valid symlink,
	// we can interpret that as an IRI (usually referencing an external object) and return that.
	dir := path.Dir(p)
	fi, err := root.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, nil
	}
	original, err := root.Readlink(dir)
	if err != nil {
		return nil, nil
	}
	original = strings.TrimLeft(path.Clean(original), "../")
	pieces := strings.Split(original, "/")
	if len(pieces) == 0 {
		return nil, nil
	}
	upath := ""
	host := pieces[0]
	// NOTE(marius): this heuristic of trying to see if the path we received is of type activities/UUID
	// is not very good, and it might lead to problems down the line.
	// Currently, it prevents returning invalid IRIs when an item in an inbox points to a valid folder in /activities,
	// but there is no __raw document there. The result before this fix was an IRI of type https://activities/UUID
	if filters.FedBOXCollections.Contains(vocab.CollectionPath(host)) {
		// directory is local, but has no __raw file
		return nil, errors.NotFoundf("invalid path %s", p)
	}
	if len(pieces) > 1 {
		upath = path.Join(pieces[1:]...)
	}
	u := url.URL{Scheme: "https", Host: host, Path: upath}
	return vocab.IRI(u.String()), nil
}

func (r *repo) loadFromCache(iri vocab.IRI) vocab.Item {
	if r.cache == nil {
		return nil
	}
	return r.cache.Load(iri.GetLink())
}

func loadRawFromPath(root *os.Root, p string) (vocab.Item, error) {
	raw, err := loadRaw(root, p)
	if err != nil {
		if os.IsNotExist(err) && !isStorageCollectionKey(filepath.Dir(p)) {
			return getOriginalIRI(root, p)
		}
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	it, err := loadFromRaw(raw)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(it) {
		return nil, errors.NotFoundf("not found")
	}
	return it, nil
}

// loadItemFromPath
func (r *repo) loadItemFromPath(p string, fil ...filters.Check) (vocab.Item, error) {
	if r.root == nil {
		return nil, errNotOpen
	}
	cachedIt := r.loadFromCache(r.iriFromPath(p))

	var it vocab.Item
	if cachedIt != nil {
		it = cachedIt
	}

	var err error
	if vocab.IsNil(it) || vocab.IsIRI(it) {
		if it, err = loadRawFromPath(r.root, p); err != nil {
			return nil, asPathErr(err)
		}
	}
	if it == nil || vocab.IsNil(it) {
		return nil, errors.NotFoundf("not found")
	}
	if it.IsCollection() {
		// we need to dereference them, so no further filtering/processing is needed here
		return it, nil
	}

	if len(fil) > 0 {
		if !applyAllFiltersOnItem(it, fil...) {
			return nil, errors.NotFoundf("not found")
		}
		it = dereferencePropertiesByType(r, it, fil...)
	}
	if cachedIt == nil {
		r.setToCache(it)
	}
	return it, nil
}

func (r *repo) setToCache(it vocab.Item) {
	if it == nil || r.cache == nil {
		return
	}
	r.cache.Store(it.GetLink(), it)
}

// loadCollectionFromPath
func (r *repo) loadCollectionFromPath(itPath string, iri vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	it, err := r.loadItemFromPath(itPath)
	if err != nil || vocab.IsNil(it) {
		return nil, errors.NewNotFound(err, "not found")
	}
	if vocab.IsIRI(it) {
		r.logger.Warnf("invalid collection to operate on %T: %s", it, it.GetLink())
		return nil, nil
	}

	_ = r.loadIndex()

	// NOTE(marius): let's make sure that if we have filters for authorization/recipients
	// we respect them for the collection itself.
	authCheck := filters.AuthorizedChecks(fil...)
	if it = authCheck.Filter(it); vocab.IsNil(it) {
		return nil, errors.NewForbidden(err, "forbidden")
	}

	_ = vocab.OnObject(it, func(ob *vocab.Object) error {
		ob.ID = iri
		return nil
	})
	var totalItems uint = 0
	_ = vocab.OnCollection(it, func(c *vocab.Collection) error {
		totalItems = c.TotalItems
		return nil
	})

	colDirPath := filepath.Dir(itPath)

	items, _ := r.searchIndex(it, fil...)
	if len(items) == 0 {
		// NOTE(marius): we load items the hard way if the index search resulted no hits, because we
		// can't make use of all the filters in the index. (Yet.)
		if items == nil {
			items = make(vocab.ItemCollection, 0, totalItems)
		}

		err = fs.WalkDir(r.root.FS(), colDirPath, func(p string, info os.DirEntry, err error) error {
			if err != nil && os.IsNotExist(err) {
				if isStorageCollectionKey(p) {
					return errors.NewNotFound(asPathErr(err), "not found")
				}
				return nil
			}

			dir := p
			diff := strings.TrimPrefix(dir, colDirPath)
			if strings.Count(diff, "/") != 1 {
				// NOTE(marius): when encountering the raw file that is deeper than the first level under the collection path, we skip
				return nil
			}
			if fn := filepath.Base(p); fn == objectKey || fn == metaDataKey || fn == _indexDirName {
				return nil
			}

			ob, err := r.loadItemFromPath(getObjectKey(p))
			if err != nil {
				r.logger.Warnf("unable to load %s: %+s", p, err)
				return nil
			}
			if !vocab.IsNil(ob) {
				items = append(items, ob)
			}
			return nil
		})
		if err != nil {
			r.logger.Errorf("unable to load collection items: %+s", err)
			return it, err
		}
	}

	if orderedCollectionTypes.Contains(it.GetType()) {
		err = vocab.OnOrderedCollection(it, buildOrderedCollection(items))
	} else {
		err = vocab.OnCollection(it, buildCollection(items))
	}

	return derefPropertiesForCurrentPage(r, it, fil...), err
}

func derefPropertiesForCurrentPage(r *repo, it vocab.Item, fil ...filters.Check) vocab.Item {
	if vocab.IsNil(it) || !it.IsCollection() || len(fil) == 0 {
		return it
	}

	_ = vocab.OnOrderedCollection(it, func(c *vocab.OrderedCollection) error {
		c.OrderedItems = dereferencePropertiesForCollection(r, c.OrderedItems, fil...)
		return nil
	})

	return filters.PaginateCollection(it, fil...)
}

func dereferencePropertiesByType(r *repo, it vocab.Item, fil ...filters.Check) vocab.Item {
	if vocab.IsNil(it) || vocab.IsIRI(it) {
		return it
	}

	intransitiveChecks := filters.IntransitiveActivityChecks(fil...)
	activityChecks := filters.ActivityChecks(fil...)
	actorChecks := filters.ActorChecks(fil...)
	objectChecks := filters.ObjectChecks(fil...)

	authorizedChecks := filters.AuthorizedChecks(fil...)

	typ := it.GetType()
	// NOTE(marius): this can probably expedite filtering if we early exit when we fail to load the
	// properties that need to be loaded for sub-filters.
	if vocab.IntransitiveActivityTypes.Contains(typ) /*&& len(intransitiveChecks) > 0*/ {
		checks := append(intransitiveChecks, authorizedChecks...)
		_ = vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r, checks...))
	}
	if vocab.ActivityTypes.Contains(typ) /*&& len(activityChecks) > 0*/ {
		checks := append(activityChecks, authorizedChecks...)
		_ = vocab.OnActivity(it, loadFilteredPropsForActivity(r, checks...))
	}
	if vocab.ActorTypes.Contains(typ) /*&& len(actorChecks) > 0*/ {
		checks := append(actorChecks, authorizedChecks...)
		_ = vocab.OnActor(it, loadFilteredPropsForActor(r, checks...))
	}
	if vocab.ObjectTypes.Contains(typ) /*&& len(objectChecks) > 0*/ {
		checks := append(objectChecks, authorizedChecks...)
		_ = vocab.OnObject(it, loadFilteredPropsForObject(r, checks...))
	}
	return it
}

func applyAllFiltersOnItem(it vocab.Item, fil ...filters.Check) bool {
	if !filters.All(filters.FilterChecks(fil...)...).Match(it) {
		return false
	}
	// NOTE(marius): the cursor checks don't get applied if it is not a collection,
	// extracting them from the filters and applying them manually is the solution.
	// We should probably fix this!!
	if !filters.All(filters.CursorChecks(fil...)...).Match(it) {
		return false
	}
	return true
}

func dereferencePropertiesForCollection(r *repo, items vocab.ItemCollection, fil ...filters.Check) vocab.ItemCollection {
	maxItems := filters.MaxCount(fil...)
	itemFilters := filters.ItemChecks(fil...)
	for i, it := range items {
		// NOTE(marius): we apply only the top level filters before we dereference the item's properties.
		// This makes it that if we have filters like actor.type=X, we don't filter them out because the activity
		// doesn't have the actor loaded, therefore having no type.
		// In the next step we dereference the properties, and we filter them with all filters.
		if !filters.All(filters.FilterChecks(itemFilters...)...).Match(it) {
			continue
		}
		if it = dereferencePropertiesByType(r, it, fil...); !vocab.IsNil(it) {
			items[i] = it
		}
		counted := filters.Counted(fil...)
		if maxItems > 0 && counted == maxItems {
			break
		}
	}

	return items
}

func buildCollection(items vocab.ItemCollection) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		if len(items) > 0 {
			col.Items = items
		}
		col.TotalItems = uint(len(items))
		return nil
	}
}

func buildOrderedCollection(items vocab.ItemCollection) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		if len(items) > 0 {
			col.OrderedItems = items
		}
		col.TotalItems = uint(len(items))
		return nil
	}
}

func (r *repo) loadFromIRI(iri vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	var err error
	var it vocab.Item

	itPath := iriPath(iri)
	if isStorageCollectionKey(itPath) {
		return r.loadCollectionFromPath(getObjectKey(itPath), iri, fil...)
	} else {
		if len(fil) == 0 {
			fil = filters.Checks{filters.SameID(iri)}
		}
		if it, err = r.loadItemFromPath(getObjectKey(itPath), fil...); err != nil {
			return nil, errors.NewNotFound(asPathErr(err), "not found")
		}
		if vocab.IsNil(it) {
			return nil, errors.NewNotFound(asPathErr(err), "not found")
		}
		if vocab.IsIRI(it) {
			return nil, errors.NewNotFound(asPathErr(err), "not found")
		}
	}
	return it, err
}

func Root(s *repo) *os.Root {
	return s.root
}

var encodeFn = func(v any) ([]byte, error) {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

var decodeFn = func(data []byte, m any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(m)
}
