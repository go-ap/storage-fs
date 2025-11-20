package fs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"
	"time"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
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
