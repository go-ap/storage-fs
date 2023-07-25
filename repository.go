package fs

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/fs"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/processing"
	"github.com/go-ap/storage-fs/internal/cache"
	"golang.org/x/crypto/bcrypt"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

var errNotImplemented = errors.NotImplementedf("not implemented")

type loggerFn func(string, ...interface{})

var defaultLogFn = func(string, ...interface{}) {}

type Config struct {
	Path        string
	CacheEnable bool
	LogFn       loggerFn
	ErrFn       loggerFn
}

type Filterable = processing.Filterable

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
		path:  p,
		cwd:   cwd,
		logFn: defaultLogFn,
		errFn: defaultLogFn,
		cache: cache.New(c.CacheEnable),
	}
	if c.LogFn != nil {
		b.logFn = c.LogFn
	}
	if c.ErrFn != nil {
		b.errFn = c.ErrFn
	}
	return &b, nil
}

type repo struct {
	baseURL string
	path    string
	cwd     string
	opened  bool
	cache   cache.CanStore
	logFn   loggerFn
	errFn   loggerFn
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

func (r *repo) CreateService(service *vocab.Service) error {
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}
	if it, err := save(r, service); err == nil {
		op := "Updated"
		id := it.GetID()
		if !id.IsValid() {
			op = "Added new"
		}
		r.logFn("%s %s: %s", op, it.GetType(), it.GetLink())
	}
	return err
}

// Load
func (r *repo) Load(i vocab.IRI) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	f, err := filters.FiltersFromIRI(i)
	if err != nil {
		return nil, err
	}

	return r.loadFromPath(f)
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
		r.logFn("%s %s: %s", op, it.GetType(), it.GetLink())
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
		i, err := r.loadOneFromPath(ob)
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
		pieces = append(pieces, url.PathEscape(u.Fragment))
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

	col, err := r.Load(colIRI)
	if err != nil {
		return err
	}
	if isHiddenCollectionKey(r.itemStoragePath(colIRI)) {
		err = errors.Newf("test")
		col = colIRI
	}

	ob, t := allStorageCollections.Split(colIRI)
	var link vocab.IRI
	if isStorageCollectionKey(string(t)) {
		// Create the collection on the object, if it doesn't exist
		i, err := r.loadOneFromPath(ob)
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
			r.logFn("symlinking path resolved to the current directory: %s", itOriginalPath)
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
	return delete(r, it)
}

func delete(r *repo, it vocab.Item) error {
	if it.IsCollection() {
		return vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			var err error
			for _, it := range c.Collection() {
				if err = deleteItem(r, it); err != nil {
					r.logFn("Unable to remove item %s", it.GetLink())
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
	ob, err := r.loadOneFromPath(iri)
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
		r.logFn("actor %s already has a private key", iri)
	}

	m = new(processing.Metadata)
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.errFn("unable to x509.MarshalPKCS8PrivateKey() the private key %T for %s", key, iri)
		return ob, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(*m, iri); err != nil {
		r.errFn("unable to save the private key %T for %s", key, iri)
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
		r.errFn("received key %T does not match any of the known private key types", key)
		return ob, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
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
	ob, err := r.loadOneFromPath(iri)
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

func (r *repo) removeFromCache(it Filterable) {
	if r.cache == nil || it == nil {
		return
	}
	r.cache.Remove(it.GetLink())
}

// deleteCollections
func deleteCollections(r repo, it vocab.Item) error {
	if vocab.ActorTypes.Contains(it.GetType()) {
		return vocab.OnActor(it, func(p *vocab.Actor) error {
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

type multiErr []error

func (e multiErr) Error() string {
	s := strings.Builder{}
	for i, err := range e {
		s.WriteString(err.Error())
		if i < len(e)-1 {
			s.WriteString(": ")
		}
	}
	return s.String()
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	if err := createCollections(r, it); err != nil {
		return it, errors.Annotatef(err, "could not create object's collections")
	}
	writeSingleObjFn := func(it vocab.Item) (vocab.Item, error) {
		itPath := r.itemStoragePath(it.GetLink())
		mkDirIfNotExists(itPath)

		// TODO(marius): it's possible to set the encoding/decoding functions on the package or storage object level
		//  instead of using jsonld.(Un)Marshal like this.
		entryBytes, err := encodeItemFn(it)
		if err != nil {
			return it, errors.Annotatef(err, "could not marshal object")
		}

		if err := mkDirIfNotExists(itPath); err != nil {
			r.errFn("unable to create path: %s, %s", itPath, err)
			return it, errors.Annotatef(err, "could not create file")
		}
		objPath := getObjectKey(itPath)
		f, err := os.OpenFile(objPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			r.errFn("%s not found", objPath)
			return it, errors.NewNotFound(asPathErr(err, r.path), "not found")
		}
		defer f.Close()
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
			m := make(multiErr, 0)
			for i, ob := range *col {
				saved, err := writeSingleObjFn(ob)
				if err == nil {
					(*col)[i] = saved
				} else {
					m = append(m, err)
				}
			}
			if len(m) > 0 {
				return m
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

func (r *repo) loadOneFromPath(f Filterable) (vocab.Item, error) {
	col, err := r.loadFromPath(f)
	if err != nil {
		return nil, err
	}
	if col == nil {
		return nil, errors.NotFoundf("nothing found")
	}
	if col.IsCollection() {
		var result vocab.Item
		vocab.OnCollectionIntf(col, func(col vocab.CollectionInterface) error {
			result = col.Collection().First()
			return nil
		})
		if vocab.IsIRI(result) && result.GetLink().Equals(f.GetLink(), false) {
			// NOTE(marius): basically we ended up with the same iri
			return nil, errors.NotFoundf("nothing found")
		}
		return result, nil
	}
	return col, nil
}

func loadFilteredPropsForActor(r *repo, f Filterable) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r, f))
	}
}

var subFilterValidationError = errors.NotValidf("Sub-filter failed validation")

func loadFilteredPropsForObject(r *repo, f Filterable) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := r.loadOneFromPath(t.GetLink()); err == nil {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}

func dereferenceItemAndFilter(r *repo, f Filterable, ob vocab.Item) (vocab.Item, error) {
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
	if f != nil {
		o, err := filters.FilterIt(ob, f)
		if err != nil {
			return ob, err
		}
		if o == nil {
			return ob, subFilterValidationError
		}
		ob = o
	}
	return ob, nil
}

func loadFilteredPropsForActivity(r *repo, f Filterable) func(a *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		var err error
		_, fo := filters.FiltersOnActivityObject(f)
		if !vocab.IsNil(a.Object) && a.ID.Equals(a.Object.GetLink(), true) {
			r.logFn("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
			return errors.BadGatewayf("invalid activity with id %s, referencing itself as an object: %s", a.ID, a.Object.GetLink())
		}
		if a.Object, err = dereferenceItemAndFilter(r, fo, a.Object); err != nil {
			return err
		}
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r, f))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo, f Filterable) func(a *vocab.IntransitiveActivity) error {
	return func(a *vocab.IntransitiveActivity) error {
		var err error
		_, fa := filters.FiltersOnActivityActor(f)
		if !vocab.IsNil(a.Actor) && a.ID.Equals(a.Actor.GetLink(), true) {
			r.logFn("Invalid %s activity (probably from mastodon), that overwrote the original actor. (%s)", a.Type, a.ID)
			return errors.BadGatewayf("invalid activity with id %s, referencing itself as an actor: %s", a.ID, a.Actor.GetLink())
		}
		if a.Actor, err = dereferenceItemAndFilter(r, fa, a.Actor); err != nil {
			return err
		}
		_, ft := filters.FiltersOnActivityTarget(f)
		if !vocab.IsNil(a.Target) && a.ID.Equals(a.Target.GetLink(), true) {
			r.logFn("Invalid %s activity (probably from mastodon), that overwrote the original object. (%s)", a.Type, a.ID)
			return errors.BadGatewayf("invalid activity with id %s, referencing itself as a target: %s", a.ID, a.Target.GetLink())
		}
		if a.Target, err = dereferenceItemAndFilter(r, ft, a.Target); err != nil {
			return err
		}
		return vocab.OnObject(a, loadFilteredPropsForObject(r, f))
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
	// is not very good and it might lead to problems down the line.
	// Currently it prevents returning invalid IRIs when an item in an inbox points to a valid folder in /activities,
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

func (r *repo) loadFromCache(f Filterable) vocab.Item {
	if f == nil || r.cache == nil {
		return nil
	}
	return r.cache.Get(f.GetLink())
}

func (r *repo) loadItem(p string, f Filterable) (vocab.Item, error) {
	var it vocab.Item
	if f != nil {
		if cachedIt := r.loadFromCache(f.GetLink()); cachedIt != nil {
			it = cachedIt
		}
	}
	if vocab.IsNil(it) {
		raw, err := loadRawFromPath(p)
		if err != nil {
			if os.IsNotExist(err) && !isStorageCollectionKey(filepath.Dir(p)) {
				return getOriginalIRI(p)
			}
			return nil, asPathErr(err, r.path)
		}
		if raw == nil {
			return nil, nil
		}
		it, err = loadFromRaw(raw)
		if err != nil {
			return nil, asPathErr(err, r.path)
		}
		if vocab.IsNil(it) {
			return nil, errors.NotFoundf("not found")
		}
	}
	if it.IsCollection() {
		// we need to dereference them, so no further filtering/processing is needed here
		return it, nil
	}
	if vocab.IsIRI(it) {
		if it, _ = r.loadOneFromPath(it.GetLink()); vocab.IsNil(it) {
			return nil, errors.NotFoundf("not found")
		}
	}
	typ := it.GetType()
	// NOTE(marius): this can probably expedite filtering if we early exit if we fail to load the
	// properties that need to load for sub-filters.
	if vocab.IntransitiveActivityTypes.Contains(typ) {
		if validErr := vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r, f)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ActivityTypes.Contains(typ) {
		if validErr := vocab.OnActivity(it, loadFilteredPropsForActivity(r, f)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ActorTypes.Contains(typ) {
		if validErr := vocab.OnActor(it, loadFilteredPropsForActor(r, f)); validErr != nil {
			return nil, nil
		}
	}
	if vocab.ObjectTypes.Contains(typ) {
		if validErr := vocab.OnObject(it, loadFilteredPropsForObject(r, f)); validErr != nil {
			return nil, nil
		}
	}

	r.setToCache(it)
	if f != nil {
		return filters.FilterIt(it, f)
	}
	return it, nil
}

func (r *repo) setToCache(it vocab.Item) {
	if it == nil || r.cache == nil {
		return
	}
	r.cache.Set(it.GetLink(), it)
}

func iriFromObjectPath(p string, r *repo) vocab.IRI {
	p = strings.Replace(p, r.path, "", -1)
	p = strings.Replace(p, filepath.Join("/", objectKey), "", -1)
	if !strings.Contains(p, "%2F") {
		return vocab.IRI("https:/" + p)
	}
	// the path contains encoded "/" characters, which means is a symlink to another folder
	_, p = filepath.Split(p)
	p, _ = url.PathUnescape(p)
	return vocab.IRI("https://" + p)
}

func (r *repo) loadCollectionFromPath(f Filterable) (vocab.Item, error) {
	itPath := r.itemStoragePath(f.GetLink())
	it, err := r.loadItem(getObjectKey(itPath), f)
	if err != nil || vocab.IsNil(it) {
		if !isHiddenCollectionKey(itPath) {
			r.logFn("unable to load collection object for %s: %s", f.GetLink(), err.Error())
			return nil, errors.NewNotFound(asPathErr(err, r.path), "unable to load collection")
		}
		// NOTE(marius): this creates blocked/ignored collections if they don't exist as dumb folders
		mkDirIfNotExists(itPath)
	}
	items := make(vocab.ItemCollection, 0)
	err = filepath.WalkDir(itPath, func(p string, info os.DirEntry, err error) error {
		if err != nil && os.IsNotExist(err) {
			if isStorageCollectionKey(p) {
				return errors.NewNotFound(asPathErr(err, r.path), "not found")
			}
			return nil
		}
		dirPath, _ := path.Split(p)
		dir := strings.TrimRight(dirPath, "/")
		if dir != itPath || filepath.Base(p) == objectKey {
			return nil
		}
		if isStorageCollectionKey(itPath) {
			if ff, ok := f.(*filters.Filters); ok {
				ff.IRI = iriFromObjectPath(p, r)
			}
		}
		ob, err := r.loadItem(getObjectKey(p), f)
		if err != nil {
			r.logFn("unable to load %s: %s", p, err.Error())
			return nil
		}
		if !vocab.IsNil(ob) {
			items = append(items, ob)
		}
		return nil
	})
	if err != nil {
		r.errFn("unable to load from fs: %s", err.Error())
		return it, err
	}

	err = vocab.OnOrderedCollection(it, func(col *vocab.OrderedCollection) error {
		col.OrderedItems = items
		if col.TotalItems == 0 {
			col.TotalItems = uint(len(items))
		}
		return nil
	})
	return it, err
}

func (r *repo) loadFromPath(f Filterable) (vocab.Item, error) {
	var err error
	var it vocab.Item

	itPath := r.itemStoragePath(f.GetLink())

	if isStorageCollectionKey(itPath) {
		return r.loadCollectionFromPath(f)
	} else {
		if it, err = r.loadItem(getObjectKey(itPath), f); err != nil {
			r.errFn("unable to load %s: %s", f.GetLink(), err.Error())
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
