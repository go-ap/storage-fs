package fs

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	xerrors "errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/processing"
	"golang.org/x/crypto/bcrypt"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

var errNotImplemented = errors.NotImplementedf("not implemented")

var emptyLogger = lw.Dev()

type Config struct {
	Path        string
	CacheEnable bool
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
	if err := mkDirIfNotExists(p); err != nil {
		return nil, err
	}
	cwd, _ := getwd()
	b := repo{
		path:   p,
		cwd:    cwd,
		logger: emptyLogger,
		cache:  cache.New(c.CacheEnable),
	}
	if c.Logger != nil {
		b.logger = c.Logger
	}
	return &b, nil
}

type repo struct {
	path   string
	cwd    string
	opened bool
	cache  cache.CanStore
	logger lw.Logger
}

// Open
func (r *repo) Open() error {
	if r.opened {
		return nil
	}
	return os.Chdir(r.path)
}

func (r *repo) close() error {
	if r.opened {
		return nil
	}
	return os.Chdir(r.cwd)
}

// Close
func (r *repo) Close() {
	r.close()
}

// Load
func (r *repo) Load(i vocab.IRI, f ...filters.Check) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	it, err := r.loadFromIRI(i, f...)
	if err != nil {
		return nil, err
	}
	return filters.Checks(f).Run(it), nil
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
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if it, err = save(r, it); err == nil {
		op := "Updated"
		id := it.GetID()
		if !id.IsValid() {
			op = "Added new"
		}
		r.logger.Debugf("%s %s: %s", op, it.GetType(), it.GetLink())
	}
	return it, err
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, it vocab.Item) error {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}

	ob, t := vocab.Split(col)
	var link vocab.IRI
	if filters.ValidCollection(t) {
		// Create the collection on the object, if it doesn't exist
		i, err := r.loadOneFromIRI(ob)
		if err != nil {
			return err
		}
		if p, ok := t.AddTo(i); ok {
			save(r, i)
			link = p
		} else {
			link = t.IRI(i)
		}
	}

	linkPath := r.itemStoragePath(link)
	name := path.Base(r.itemStoragePath(it.GetLink()))
	// we create a symlink to the persisted object in the current collection
	err = onCollection(r, col, it, func(p string) error {
		inCollection := false
		if dirInfo, err := os.ReadDir(p); err == nil {
			for _, di := range dirInfo {
				fi, err := di.Info()
				if err != nil {
					continue
				}
				if fi.Name() == name && (isSymLink(fi) || isHardLink(fi)) {
					inCollection = true
				}
			}
		}
		if inCollection {
			link := path.Join(linkPath, name)
			return os.RemoveAll(link)
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.removeFromCache(it.GetLink())
	return nil
}

func isSymLink(fi os.FileInfo) bool {
	if fi == nil {
		return false
	}
	return fi.Mode()&os.ModeSymlink == os.ModeSymlink
}

func isHardLink(fi os.FileInfo) bool {
	nlink := uint64(0)
	if sys := fi.Sys(); sys != nil {
		if stat, ok := sys.(*syscall.Stat_t); ok {
			nlink = uint64(stat.Nlink)
		}
	}
	return nlink > 1 && !fi.IsDir()
}

var allStorageCollections = append(vocab.ActivityPubCollections, filters.FedBOXCollections...)

func iriPath(iri vocab.IRI) string {
	u, err := iri.URL()
	if err != nil {
		return ""
	}

	pieces := make([]string, 0)
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

func createCollection(r *repo, colIRI vocab.IRI) (vocab.CollectionInterface, error) {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		Published: time.Now().UTC(),
	}
	return saveCollection(r, &col)
}

var orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
var collectionTypes = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}

// AddTo
func (r *repo) AddTo(colIRI vocab.IRI, it vocab.Item) error {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}

	var link vocab.IRI
	var col vocab.Item
	// NOTE(marius): We make sure the collection exists (unless it's a hidden collection)
	if !isHiddenCollectionKey(r.itemStoragePath(colIRI)) {
		if col, err = r.Load(colIRI); err != nil {
			return err
		}
		ob, t := allStorageCollections.Split(colIRI)
		if isStorageCollectionKey(string(t)) {
			// Create the collection on the object, if it doesn't exist
			i, err := r.loadOneFromIRI(ob)
			if err != nil {
				return err
			}
			if p, ok := t.AddTo(i); ok {
				save(r, i)
				link = p
			} else {
				link = t.IRI(i)
			}
		} else {
			return errors.Newf("Invalid collection %s", t)
		}
	} else {
		// NOTE(marius): for hidden collections we might not have the __raw file on disk, so we just wing it
		link = colIRI
		col = link
	}

	linkPath := r.itemStoragePath(link)
	itOriginalPath := r.itemStoragePath(it.GetLink())

	fullLink := path.Join(linkPath, url.PathEscape(iriPath(it.GetLink())))

	// we create a symlink to the persisted object in the current collection
	err = onCollection(r, col, it, func(p string) error {
		err := mkDirIfNotExists(p)
		if err != nil {
			return errors.Annotatef(err, "Unable to create collection folder %s", p)
		}
		// NOTE(marius): if 'it' IRI belongs to the 'col' collection we can skip symlinking it
		if it.GetLink().Contains(col.GetLink(), true) {
			return nil
		}
		inCollection := false
		if dirInfo, err := os.ReadDir(p); err == nil {
			for _, di := range dirInfo {
				fi, err := di.Info()
				if err != nil {
					continue
				}
				if fi.Name() == fullLink && (isSymLink(fi) || isHardLink(fi)) {
					inCollection = true
				}
			}
		}
		if inCollection {
			return nil
		}

		if itOriginalPath, err = filepath.Abs(itOriginalPath); err != nil {
			return err
		}
		if fullLink, err = filepath.Abs(fullLink); err != nil {
			return err
		}
		if itOriginalPath, err = filepath.Rel(fullLink, itOriginalPath); err != nil {
			return err
		}
		// TODO(marius): using filepath.Rel returns one extra parent for some reason, I need to look into why
		itOriginalPath = strings.Replace(itOriginalPath, "../", "", 1)
		if itOriginalPath == "." {
			// NOTE(marius): if the relative path resolves to the current folder, we don't try to symlink
			r.logger.Debugf("symlinking path resolved to the current directory: %s", itOriginalPath)
			return nil
		}
		// NOTE(marius): we can't use hard links as we're linking to folders :(
		// This would have been tremendously easier (as in, not having to compute paths) with hard-links.
		return os.Symlink(itOriginalPath, fullLink)
	})
	if err != nil {
		return errors.Annotatef(err, "unable to symlink object into collection")
	}

	if orderedCollectionTypes.Contains(col.GetType()) {
		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			c.TotalItems += 1
			c.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Contains(col.GetType()) {
		err = vocab.OnCollection(col, func(c *vocab.Collection) error {
			c.TotalItems += 1
			c.Items = nil
			return nil
		})
	}
	_, err = save(r, col)
	return err
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}
	return r.delete(it)
}

func (r *repo) delete(it vocab.Item) error {
	if it.IsCollection() {
		return vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			var err error
			for _, it := range c.Collection() {
				if err = deleteItem(r, it); err != nil {
					r.logger.Debugf("Unable to remove item %s", it.GetLink())
				}
			}
			return nil
		})
	}
	return deleteItem(r, it.GetLink())
}

// PasswordSet
func (r *repo) PasswordSet(it vocab.Item, pw []byte) error {
	pw, err := bcrypt.GenerateFromPassword(pw, -1)
	if err != nil {
		return errors.Annotatef(err, "could not generate pw hash")
	}
	m := processing.Metadata{
		Pw: pw,
	}
	return r.SaveMetadata(m, it.GetLink())
}

// PasswordCheck
func (r *repo) PasswordCheck(it vocab.Item, pw []byte) error {
	m, err := r.LoadMetadata(it.GetLink())
	if err != nil {
		return errors.Annotatef(err, "Could not find load metadata for %s", it)
	}

	if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
		return errors.NewUnauthorized(err, "Invalid pw")
	}
	return err
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI) (*processing.Metadata, error) {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return nil, err
	}

	p := r.itemStoragePath(iri)
	raw, err := loadRawFromPath(getMetadataKey(p))
	if err != nil {
		err = errors.NewNotFound(asPathErr(err, r.path), "Could not find metadata in path %s", sanitizePath(p, r.path))
		return nil, err
	}
	m := new(processing.Metadata)
	if err = decodeFn(raw, m); err != nil {
		return nil, errors.Annotatef(err, "Could not unmarshal metadata")
	}
	return m, nil
}

// SaveMetadata
func (r *repo) SaveMetadata(m processing.Metadata, iri vocab.IRI) error {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}

	p := getMetadataKey(r.itemStoragePath(iri))
	f, err := createOrOpenFile(p)
	if err != nil {
		return err
	}
	defer f.Close()

	entryBytes, err := encodeFn(m)
	if err != nil {
		return errors.Annotatef(err, "Could not marshal metadata")
	}
	wrote, err := f.Write(entryBytes)
	if err != nil {
		return errors.Annotatef(err, "could not store encoded object")
	}
	if wrote != len(entryBytes) {
		return errors.Annotatef(err, "failed writing full object")
	}
	return nil
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	m, err := r.LoadMetadata(iri)
	if err != nil {
		return nil, asPathErr(err, r.path)
	}

	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (vocab.Item, error) {
	ob, err := r.loadOneFromIRI(iri)
	if err != nil {
		return nil, err
	}

	typ := ob.GetType()
	if !vocab.ActorTypes.Contains(typ) {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}
	actor, err := vocab.ToActor(ob)
	if err != nil {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}

	m, err := r.LoadMetadata(iri)
	if err != nil && !errors.IsNotFound(err) {
		return ob, err
	}
	if m != nil && m.PrivateKey != nil {
		r.logger.Debugf("actor %s already has a private key", iri)
	}

	m = new(processing.Metadata)
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.logger.Errorf("unable to x509.MarshalPKCS8PrivateKey() the private key %T for %s", key, iri)
		return ob, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(*m, iri); err != nil {
		r.logger.Errorf("unable to save the private key %T for %s", key, iri)
		return ob, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case *ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.logger.Errorf("received key %T does not match any of the known private key types", key)
		return ob, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.logger.Errorf("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return ob, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	actor.PublicKey = vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}
	return r.Save(actor)
}

// GenKey creates and saves a private key for an actor found by its IRI
func (r *repo) GenKey(iri vocab.IRI) error {
	ob, err := r.loadOneFromIRI(iri)
	if err != nil {
		return err
	}
	if ob.GetType() != vocab.PersonType {
		return errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", ob.GetType())
	}
	m, err := r.LoadMetadata(iri)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if m.PrivateKey != nil {
		return nil
	}
	// TODO(marius): this needs a way to choose between ED25519 and RSA keys
	pubB, prvB := generateECKeyPair()
	m.PrivateKey = pem.EncodeToMemory(&prvB)

	if err = r.SaveMetadata(*m, iri); err != nil {
		return err
	}
	vocab.OnActor(ob, func(act *vocab.Actor) error {
		act.PublicKey = vocab.PublicKey{
			ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
			Owner:        iri,
			PublicKeyPem: string(pem.EncodeToMemory(&pubB)),
		}
		return nil
	})
	return nil
}

func generateECKeyPair() (pem.Block, pem.Block) {
	// TODO(marius): make this actually produce proper keys
	keyPub, keyPrv, _ := ed25519.GenerateKey(rand.New(rand.NewSource(6667)))

	var p, r pem.Block
	if pubEnc, err := x509.MarshalPKIXPublicKey(keyPub); err == nil {
		p = pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: pubEnc,
		}
	}
	if prvEnc, err := x509.MarshalPKCS8PrivateKey(keyPrv); err == nil {
		r = pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: prvEnc,
		}
	}
	return p, r
}

func createOrOpenFile(p string) (*os.File, error) {
	err := mkDirIfNotExists(path.Dir(p))
	if err != nil {
		return nil, err
	}
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
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

func (r *repo) itemStoragePath(iri vocab.IRI) string {
	return filepath.Join(r.path, iriPath(iri))
}

// createCollections
func createCollections(r *repo, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		vocab.OnActor(it, func(p *vocab.Actor) error {
			p.Inbox, _ = createCollectionInPath(r, p.Inbox)
			p.Outbox, _ = createCollectionInPath(r, p.Outbox)
			p.Followers, _ = createCollectionInPath(r, p.Followers)
			p.Following, _ = createCollectionInPath(r, p.Following)
			p.Liked, _ = createCollectionInPath(r, p.Liked)
			// NOTE(marius): shadow creating hidden collections for Blocked and Ignored items
			_, _ = createCollectionInPath(r, filters.BlockedType.Of(p))
			_, _ = createCollectionInPath(r, filters.IgnoredType.Of(p))
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		o.Replies, _ = createCollectionInPath(r, o.Replies)
		o.Likes, _ = createCollectionInPath(r, o.Likes)
		o.Shares, _ = createCollectionInPath(r, o.Shares)
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

func createCollectionInPath(r *repo, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}
	itPath := r.itemStoragePath(it.GetLink())

	colObject, err := r.loadItem(getObjectKey(itPath), it.GetLink())
	if colObject == nil {
		it, err = createCollection(r, it.GetLink())
	}
	if err != nil {
		return nil, errors.Annotatef(err, "saving collection object is not done")
	}

	return it.GetLink(), asPathErr(mkDirIfNotExists(itPath), r.path)
}

func deleteCollectionFromPath(r repo, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	itPath := r.itemStoragePath(it.GetLink())
	if fi, err := os.Stat(itPath); err != nil {
		if !os.IsNotExist(err) {
			return errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
	} else if fi.IsDir() {
		return os.Remove(itPath)
	}
	r.removeFromCache(it.GetLink())
	return nil
}

func (r *repo) removeFromCache(iri vocab.IRI) {
	if r.cache == nil {
		return
	}
	r.cache.Delete(iri.GetLink())
}

// deleteCollections
func deleteCollections(r repo, it vocab.Item) error {
	if vocab.ActorTypes.Contains(it.GetType()) {
		return vocab.OnActor(it, func(p *vocab.Actor) error {
			// NOTE(marius): deleting the hidden collections for Blocked and Ignored items
			_ = deleteCollectionFromPath(r, filters.BlockedType.Of(p))
			_ = deleteCollectionFromPath(r, filters.IgnoredType.Of(p))

			var err error
			err = deleteCollectionFromPath(r, vocab.Inbox.IRI(p))
			err = deleteCollectionFromPath(r, vocab.Outbox.IRI(p))
			err = deleteCollectionFromPath(r, vocab.Followers.IRI(p))
			err = deleteCollectionFromPath(r, vocab.Following.IRI(p))
			err = deleteCollectionFromPath(r, vocab.Liked.IRI(p))
			return err
		})
	}
	if vocab.ObjectTypes.Contains(it.GetType()) {
		return vocab.OnObject(it, func(o *vocab.Object) error {
			var err error
			err = deleteCollectionFromPath(r, vocab.Replies.IRI(o))
			err = deleteCollectionFromPath(r, vocab.Likes.IRI(o))
			err = deleteCollectionFromPath(r, vocab.Shares.IRI(o))
			return err
		})
	}
	return nil
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
	itemPath := r.itemStoragePath(it.GetLink())
	if err := os.RemoveAll(itemPath); err != nil {
		return err
	}
	r.removeFromCache(it.GetLink())
	return nil
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	if err := createCollections(r, it); err != nil {
		return it, errors.Annotatef(err, "could not create object's collections")
	}
	writeSingleObjFn := func(it vocab.Item) (vocab.Item, error) {
		itPath := r.itemStoragePath(it.GetLink())
		_ = mkDirIfNotExists(itPath)

		entryBytes, err := encodeItemFn(it)
		if err != nil {
			return it, errors.Annotatef(err, "could not marshal object")
		}

		if err := mkDirIfNotExists(itPath); err != nil {
			r.logger.Errorf("unable to create path: %s, %s", itPath, err)
			return it, errors.Annotatef(err, "could not create file")
		}
		objPath := getObjectKey(itPath)
		f, err := os.OpenFile(objPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			r.logger.Errorf("%s not found", objPath)
			return it, errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
		defer func() {
			if err := f.Close(); err != nil {
				r.logger.Errorf("Unable to close file: %s", err)
			}
		}()
		wrote, err := f.Write(entryBytes)
		if err != nil {
			return it, errors.Annotatef(err, "could not store encoded object")
		}
		if wrote != len(entryBytes) {
			return it, errors.Annotatef(err, "failed writing object")
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
	if len(col.GetLink()) == 0 {
		return errors.Newf("Unable to find collection")
	}
	if len(it.GetLink()) == 0 {
		return errors.Newf("Invalid collection, it does not have a valid IRI")
	}

	itPath := r.itemStoragePath(col.GetLink())
	err := fn(itPath)
	if err != nil {
		if os.IsExist(err) {
			return errors.NewConflict(err, "%s already exists in collection %s", it.GetID(), itPath)
		}
		return errors.Annotatef(err, "Unable to save entries to collection %s", itPath)
	}
	r.removeFromCache(col.GetLink())
	return nil
}

func loadRawFromPath(itPath string) ([]byte, error) {
	return os.ReadFile(itPath)
}

func loadFromRaw(raw []byte) (vocab.Item, error) {
	if raw == nil || len(raw) == 0 {
		// TODO(marius): log this instead of stopping the iteration and returning an error
		return nil, errors.Errorf("empty raw item")
	}
	return decodeItemFn(raw)
}

func (r *repo) loadOneFromIRI(f vocab.IRI) (vocab.Item, error) {
	col, err := r.loadFromIRI(f)
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
		if vocab.IsIRI(result) && result.GetLink().Equals(f.GetLink(), false) {
			// NOTE(marius): this covers the case where we ended up with the same IRI
			return nil, errors.NotFoundf("nothing found")
		}
		return result, nil
	}
	return col, nil
}

func loadFilteredPropsForActor(r *repo) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r))
	}
}

func loadFilteredPropsForObject(r *repo) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := r.loadOneFromIRI(t.GetLink()); err == nil {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}

func dereferenceItemAndFilter(r *repo, ob vocab.Item, fil ...filters.Check) (vocab.Item, error) {
	if vocab.IsNil(ob) {
		return ob, nil
	}

	if vocab.IsIRI(ob) {
		o, err := r.Load(ob.GetLink())
		if err != nil {
			return ob, nil
		}
		if o != nil {
			ob = o
		}
	}
	if filtered := filters.Checks(fil).Run(ob); filtered == nil {
		ob = ob.GetLink()
	}
	return ob, nil
}

func loadFilteredPropsForActivity(r *repo, fil ...filters.Check) func(a *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		var err error
		if !vocab.IsNil(a.Object) {
			if a.ID.Equals(a.Object.GetLink(), false) {
				//r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
				return errors.BadGatewayf("invalid activity with id %s, referencing itself as an object: %s", a.ID, a.Object.GetLink())
			}
			if a.Object, err = dereferenceItemAndFilter(r, a.Object, fil...); err != nil {
				return err
			}
		}
		fil = filters.IntransitiveActivityChecks(fil...)
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r, fil...))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo, fil ...filters.Check) func(a *vocab.IntransitiveActivity) error {
	return func(a *vocab.IntransitiveActivity) error {
		var err error
		if !vocab.IsNil(a.Actor) {
			if a.ID.Equals(a.Actor.GetLink(), false) {
				r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
				return errors.BadGatewayf("invalid activity with id %s, referencing itself as an actor: %s", a.ID, a.Actor.GetLink())
			}
			if a.Actor, err = dereferenceItemAndFilter(r, a.Actor); err != nil {
				return err
			}
		}
		if !vocab.IsNil(a.Target) {
			if a.ID.Equals(a.Target.GetLink(), false) {
				r.logger.Debugf("Invalid %s activity (probably from mastodon), that overwrote the original object. (%s)", a.Type, a.ID)
				return errors.BadGatewayf("invalid activity with id %s, referencing itself as a target: %s", a.ID, a.Target.GetLink())
			}
			if a.Target, err = dereferenceItemAndFilter(r, a.Target); err != nil {
				return err
			}
		}
		return vocab.OnObject(a, loadFilteredPropsForObject(r))
	}
}

func sanitizePath(p, prefix string) string {
	p = strings.TrimPrefix(p, prefix)
	p = strings.TrimSuffix(p, objectKey)
	p = strings.TrimSuffix(p, metaDataKey)
	return strings.Trim(p, "/")
}

func asPathErr(err error, prefix string) error {
	if err == nil {
		return nil
	}
	if perr, ok := err.(*fs.PathError); ok {
		perr.Path = sanitizePath(perr.Path, prefix)
		return perr
	}
	return err
}

func getOriginalIRI(p string) (vocab.Item, error) {
	// NOTE(marius): if the __raw file wasn't found, but the path corresponds to a valid symlink,
	// we can interpret that as an IRI (usually referencing an external object) and return that.
	dir := path.Dir(p)
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, nil
	}
	original, err := os.Readlink(dir)
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

func loadItemFromPath(p string) (vocab.Item, error) {
	raw, err := loadRawFromPath(p)
	if err != nil {
		if os.IsNotExist(err) && !isStorageCollectionKey(filepath.Dir(p)) {
			return getOriginalIRI(p)
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

func (r *repo) loadItem(p string, iri vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	var it vocab.Item
	if iri != "" {
		if cachedIt := r.loadFromCache(iri); cachedIt != nil {
			it = cachedIt
		}
	}
	var err error
	if vocab.IsNil(it) {
		it, err = loadItemFromPath(p)
		if err != nil {
			return nil, asPathErr(err, r.path)
		}
	}
	if vocab.IsIRI(it) {
		if it, err = loadItemFromPath(p); err != nil {
			return nil, asPathErr(err, r.path)
		}
	}
	if vocab.IsNil(it) {
		return nil, errors.NotFoundf("not found")
	}
	if it.IsCollection() {
		// we need to dereference them, so no further filtering/processing is needed here
		return it, nil
	}
	typ := it.GetType()
	// NOTE(marius): this can probably expedite filtering if we early exit if we fail to load the
	// properties that need to load for sub-filters.
	if vocab.IntransitiveActivityTypes.Contains(typ) {
		if validErr := vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ActivityTypes.Contains(typ) {
		if validErr := vocab.OnActivity(it, loadFilteredPropsForActivity(r)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ActorTypes.Contains(typ) {
		if validErr := vocab.OnActor(it, loadFilteredPropsForActor(r)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ObjectTypes.Contains(typ) {
		if validErr := vocab.OnObject(it, loadFilteredPropsForObject(r)); validErr != nil {
			return nil, nil
		}
	}

	r.setToCache(it)
	return it, nil
}

func (r *repo) setToCache(it vocab.Item) {
	if it == nil || r.cache == nil {
		return
	}
	r.cache.Store(it.GetLink(), it)
}

func (r *repo) loadCollectionFromPath(iri vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	itPath := r.itemStoragePath(iri)
	it, err := r.loadItem(getObjectKey(itPath), iri, fil...)
	_ = vocab.OnObject(it, func(ob *vocab.Object) error {
		ob.ID = iri
		return nil
	})
	if err != nil || vocab.IsNil(it) {
		if !isHiddenCollectionKey(itPath) {
			r.logger.Debugf("unable to load collection object for %s: %s", iri, err.Error())
			return nil, errors.NewNotFound(asPathErr(err, r.path), "unable to load collection")
		}
		// NOTE(marius): this creates blocked/ignored collections if they don't exist as dumb folders
		if err = mkDirIfNotExists(itPath); err != nil {
			r.logger.Warnf("unable to create collection %s: %s", iri, err.Error())
		}
	}
	items := make(vocab.ItemCollection, 0)
	err = filepath.WalkDir(itPath, func(p string, info os.DirEntry, err error) error {
		if err != nil && os.IsNotExist(err) {
			if isStorageCollectionKey(p) {
				return errors.NewNotFound(asPathErr(err, r.path), "not found")
			}
			return nil
		}
		dirPath, _ := filepath.Split(p)
		dir := strings.TrimRight(dirPath, "/")
		if dir != itPath || filepath.Base(p) == objectKey {
			return nil
		}

		iriFromPath := func(p string) vocab.IRI {
			return vocab.IRI(fmt.Sprintf("https://%s", strings.Replace(p, r.path, "", 1)))
		}
		iri = iriFromPath(p)
		ob, err := r.loadItem(getObjectKey(p), iri, fil...)
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
		r.logger.Errorf("unable to load from fs: %+s", err)
		return it, err
	}
	if vocab.IsNil(it) {
		return nil, nil
	}
	if vocab.IsIRI(it) {
		r.logger.Warnf("invalid collection to operate on %T: %s", it, it.GetLink())
		return nil, nil
	}

	if orderedCollectionTypes.Contains(it.GetType()) {
		err = vocab.OnOrderedCollection(it, postProcessOrderedItems(items))
	} else {
		err = vocab.OnCollection(it, postProcessItems(items))
	}
	return it, err
}

func postProcessItems(items vocab.ItemCollection) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		col.Items = items
		col.TotalItems = uint(len(items))
		return nil
	}
}

func postProcessOrderedItems(items vocab.ItemCollection) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		col.OrderedItems = items
		sort.Slice(col.OrderedItems, func(i, j int) bool {
			return vocab.ItemOrderTimestamp(col.OrderedItems[i], col.OrderedItems[j])
		})
		col.TotalItems = uint(len(items))
		return nil
	}
}

func (r *repo) loadFromIRI(iri vocab.IRI, fil ...filters.Check) (vocab.Item, error) {
	var err error
	var it vocab.Item

	itPath := r.itemStoragePath(iri)

	if isStorageCollectionKey(itPath) {
		return r.loadCollectionFromPath(iri, fil...)
	} else {
		if it, err = r.loadItem(getObjectKey(itPath), iri, fil...); err != nil {
			r.logger.Tracef("unable to load %s: %s", iri, err.Error())
			return nil, errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
		if vocab.IsNil(it) {
			return nil, errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
		if vocab.IsIRI(it) {
			return nil, errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
	}
	return it, err
}

var testCWD = ""

func getwd() (string, error) {
	if testCWD != "" {
		return testCWD, nil
	}
	return os.Getwd()
}
