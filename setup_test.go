package fs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	mrand "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	conformance "github.com/go-ap/storage-conformance-suite"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
	"golang.org/x/crypto/bcrypt"
)

func areErrors(a, b any) bool {
	_, ok1 := a.(error)
	_, ok2 := b.(error)
	return ok1 && ok2
}

func compareErrors(x, y interface{}) bool {
	xe := x.(error)
	ye := y.(error)
	if errors.Is(xe, ye) || errors.Is(ye, xe) {
		return true
	}
	return xe.Error() == ye.Error()
}

var EquateWeakErrors = cmp.FilterValues(areErrors, cmp.Comparer(compareErrors))

type fields struct {
	path  string
	root  *os.Root
	index *bitmaps
	cache cache.CanStore
}

func mockRepo(t *testing.T, f fields, initFns ...initFn) *repo {
	r := &repo{
		path:   f.path,
		root:   f.root,
		index:  f.index,
		cache:  f.cache,
		logger: lw.Dev(lw.SetOutput(t.Output()), lw.SetLevel(lw.InfoLevel)),
	}

	if r.root == nil {
		root := openRoot(t, t.TempDir())
		if r.index == nil {
			r.index = mockIndex(t, root)
		}
	}

	for _, fn := range initFns {
		_ = fn(r)
	}
	return r
}

func openRoot(t *testing.T, path string) *os.Root {
	rr, err := os.OpenRoot(path)
	if err != nil {
		t.Fatalf("Unable to open mock root: %s", err)
	}
	return rr
}

type initFn func(*repo) *repo

func withOpenRoot(r *repo) *repo {
	var err error
	r.root, err = os.OpenRoot(r.path)
	if err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("Unable to open mock root")
	}
	return r
}

func withMockItems(r *repo) *repo {
	for _, it := range mockItems {
		if _, err := save(r, it); err != nil {
			r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("unable to save item: %s", it.GetLink())
		}
	}
	return r
}

var (
	pk, _      = rsa.GenerateKey(rand.Reader, 4096)
	pkcs8Pk, _ = x509.MarshalPKCS8PrivateKey(pk)
	key        = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Pk,
	})

	pubEnc, _  = x509.MarshalPKIXPublicKey(pk.Public())
	pubEncoded = pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	apPublic = &vocab.PublicKey{
		ID:           "https://example.com/~jdoe#main",
		Owner:        "https://example.com/~jdoe",
		PublicKeyPem: string(pubEncoded),
	}

	defaultPw = []byte("dsa")

	encPw, _ = bcrypt.GenerateFromPassword(defaultPw, bcrypt.DefaultCost)
)

func withMetadataJDoe(r *repo) *repo {
	m := Metadata{
		Pw:         encPw,
		PrivateKey: key,
	}

	if err := r.SaveMetadata("https://example.com/~jdoe", m); err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("unable to save metadata for jdoe")
	}
	return r
}

var (
	defaultClient = &osin.DefaultClient{
		Id:          "test-client",
		Secret:      "asd",
		RedirectUri: "https://example.com",
		UserData:    nil,
	}
)

func mockAuth(code string, cl osin.Client) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:    cl,
		Code:      code,
		ExpiresIn: 10,
		CreatedAt: time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:  vocab.IRI("https://example.com/jdoe"),
	}
}

func mockAccess(code string, cl osin.Client) *osin.AccessData {
	return &osin.AccessData{
		Client:        cl,
		AuthorizeData: mockAuth("test-code", cl),
		AccessToken:   code,
		RefreshToken:  "refresh",
		ExpiresIn:     10,
		Scope:         "none",
		RedirectUri:   "http://localhost",
		CreatedAt:     time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:      vocab.IRI("https://example.com/jdoe"),
	}
}

func withClient(r *repo) *repo {
	if err := r.CreateClient(defaultClient); err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("failed to create client")
	}
	return r
}

func withAuthorization(r *repo) *repo {
	if err := r.SaveAuthorize(mockAuth("test-code", defaultClient)); err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("failed to create authorization data")
	}
	return r
}

func withAccess(r *repo) *repo {
	if err := r.SaveAccess(mockAccess("access-666", defaultClient)); err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("failed to create authorization data")
	}
	return r
}

var (
	rootIRI       = vocab.IRI("https://example.com")
	rootInboxIRI  = rootIRI.AddPath(string(vocab.Inbox))
	rootOutboxIRI = rootIRI.AddPath(string(vocab.Outbox))
	root          = &vocab.Actor{
		ID:        rootIRI,
		Type:      vocab.ServiceType,
		Published: publishedTime,
		Name:      vocab.DefaultNaturalLanguage("example.com"),
		Inbox:     rootInboxIRI,
		Outbox:    rootOutboxIRI,
	}

	publishedTime = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)

	createCnt     = atomic.Int32{}
	allActors     = atomic.Pointer[vocab.ItemCollection]{}
	allObjects    = atomic.Pointer[vocab.ItemCollection]{}
	allActivities = atomic.Pointer[vocab.ItemCollection]{}
)

func withGeneratedRoot(root vocab.Item) initFn {
	return func(r *repo) *repo {
		if _, err := r.Save(root); err != nil {
			r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("unable to save root service")
		}
		return r
	}
}

func withGeneratedItems(items vocab.ItemCollection) initFn {
	return func(r *repo) *repo {
		for _, it := range items {
			if _, err := save(r, it); err != nil {
				r.logger.WithContext(lw.Ctx{"err": err.Error(), "iri": it.GetLink()}).Errorf("unable to save %T", it)
			}
		}
		return r
	}
}

func withActivitiesToCollections(activities vocab.ItemCollection) initFn {
	return func(r *repo) *repo {
		collectionIRI := vocab.Outbox.IRI(root)
		_ = r.AddTo(collectionIRI, activities...)
		return r
	}
}

func createActivity(ob vocab.Item, attrTo vocab.Item) *vocab.Activity {
	act := new(vocab.Activity)
	act.Type = vocab.CreateType
	if ob != nil {
		act.Object = ob
	}
	act.AttributedTo = attrTo.GetLink()
	act.Actor = attrTo.GetLink()
	act.To = vocab.ItemCollection{rootIRI, vocab.PublicNS}
	createCnt.Add(1)

	return act
}

func withGeneratedMocks(r *repo) *repo {
	r.index = nil
	idSetter := setId(rootIRI)
	r = withGeneratedRoot(root)(r)

	actors := make(vocab.ItemCollection, 0, 20)
	for range cap(actors) - 1 {
		actor := conformance.RandomActor(root)
		_ = vocab.OnObject(actor, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = actors.Append(actor)
	}
	r = withGeneratedItems(actors)(r)
	allActors.Store(&actors)

	objects := make(vocab.ItemCollection, 0, 50)
	creates := make(vocab.ItemCollection, 0, 50)
	for range cap(objects) {
		//parent := actors[mrand.Intn(len(actors))]
		parent := root
		ob := conformance.RandomObject(parent)
		_ = vocab.OnObject(ob, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = objects.Append(ob)
		create := createActivity(ob, root)
		_ = vocab.OnObject(create, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = creates.Append(create)
	}
	r = withGeneratedItems(objects)(r)
	allObjects.Store(&objects)

	activities := make(vocab.ItemCollection, 0, cap(actors)*10)
	for range cap(activities) {
		object := objects[mrand.Intn(len(objects))]
		//author := actors[mrand.Intn(len(actors))]
		author := root

		activity := conformance.RandomActivity(object, author)
		_ = vocab.OnObject(activity, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = activities.Append(activity)
	}
	activities = append(creates, activities...)
	r = withGeneratedItems(activities)(r)
	r = withActivitiesToCollections(activities)(r)

	rebuildIndex(r)
	allActivities.Store(&activities)
	return r
}

func rebuildIndex(r *repo) {
	r.index = newBitmap()
	if err := saveIndex(r.root, r.index, _indexDirName); err != nil {
		r.logger.WithContext(lw.Ctx{"root": r.root.Name(), "err": err}).Errorf("unable to save mock root indexes")
	}
	if err := r.Reindex(); err != nil {
		r.logger.WithContext(lw.Ctx{"root": r.root.Name(), "err": err}).Errorf("unable to reindex repo")
	}
}

func setId(base vocab.IRI) func(ob *vocab.Object) error {
	idMap := sync.Map{}
	return func(ob *vocab.Object) error {
		typ := ob.Type
		id := 1
		if latestId, ok := idMap.Load(typ); ok {
			id = latestId.(int) + 1
		}
		ob.ID = base.AddPath(strings.ToLower(string(typ))).AddPath(strconv.Itoa(id))
		idMap.Store(typ, id)
		return nil
	}
}

func sortCollectionByIRI(col vocab.CollectionInterface) error {
	sort.Slice(col.Collection(), func(i, j int) bool {
		iti := col.Collection()[i]
		itj := col.Collection()[j]
		return iti.GetLink().String() <= itj.GetLink().String()
	})
	return nil
}

func filter(items vocab.ItemCollection, fil ...filters.Check) vocab.ItemCollection {
	result, _ := vocab.ToItemCollection(filters.Checks(fil).Run(items))
	return *result
}

func wantsRootOutboxPage(maxItems int, ff ...filters.Check) vocab.Item {
	return &vocab.OrderedCollectionPage{
		ID:           rootOutboxIRI,
		Type:         vocab.OrderedCollectionPageType,
		AttributedTo: rootIRI,
		Published:    publishedTime,
		CC:           vocab.ItemCollection{vocab.IRI("https://www.w3.org/ns/activitystreams#Public")},
		First:        vocab.IRI(string(rootOutboxIRI) + "?" + filters.ToValues(filters.WithMaxCount(maxItems)).Encode()),
		OrderedItems: filter(*allActivities.Load(), ff...),
		TotalItems:   allActivities.Load().Count(),
	}
}

func wantsRootOutbox(ff ...filters.Check) vocab.Item {
	return &vocab.OrderedCollection{
		ID:           rootOutboxIRI,
		Type:         vocab.OrderedCollectionType,
		AttributedTo: rootIRI,
		Published:    publishedTime,
		CC:           vocab.ItemCollection{vocab.IRI("https://www.w3.org/ns/activitystreams#Public")},
		First:        vocab.IRI(string(rootOutboxIRI) + "?" + filters.ToValues(filters.WithMaxCount(filters.MaxItems)).Encode()),
		OrderedItems: filter(*allActivities.Load(), ff...),
		TotalItems:   allActivities.Load().Count(),
	}
}
