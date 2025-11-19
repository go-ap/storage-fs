package fs

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"reflect"
	"testing"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/crypto/bcrypt"
)

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

	if err := r.Open(); err != nil {
		r.logger.WithContext(lw.Ctx{"err": err.Error()}).Errorf("unable to open storage")
	}
	for _, fn := range initFns {
		_ = fn(r)
	}
	return r
}

type initFn func(*repo) *repo

func withItems(r *repo) *repo {
	for _, it := range mockItems {
		if _, err := r.Save(it); err != nil {
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

func Test_repo_LoadKey(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		iri      vocab.IRI
		want     crypto.PrivateKey
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name: "empty IRI is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			iri:      "https://example.com/~jdoe",
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			iri:      "https://example.com/~jdoe",
			want:     pk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			got, err := r.LoadKey(tt.iri)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadKey() diff = %s", cmp.Diff(got, tt.want))
			}
		})
	}
}

func Test_repo_LoadMetadata(t *testing.T) {
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		want     any
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name: "empty args is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   Metadata{},
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   &Metadata{},
			},
			want: &Metadata{
				Pw:         encPw,
				PrivateKey: key,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.LoadMetadata(tt.args.iri, tt.args.m); !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			if !cmp.Equal(tt.want, tt.args.m) {
				t.Errorf("LoadMetadata() diff = %s", cmp.Diff(tt.want, tt.args.m))
			}
		})
	}
}

func Test_repo_PasswordCheck(t *testing.T) {
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name: "empty args is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			args: args{
				iri: "https://example.com/~jdoe",
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with correct pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  defaultPw,
			},
		},
		{
			name: "~jdoe with incorrect pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  []byte("asd"),
			},
			wantErr: errors.Unauthorizedf("Invalid pw"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.PasswordCheck(tt.args.iri, tt.args.pw); !errors.Is(err, tt.wantErr) {
				t.Errorf("PasswordCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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

func Test_repo_PasswordSet(t *testing.T) {
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withItems},
			wantErr:  errors.Newf("could not generate hash for nil pw"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			args: args{
				iri: "https://example.com/~jdoe",
			},
			wantErr: errors.Newf("could not generate hash for nil pw"),
		},
		{
			name: "~jdoe with pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  []byte("asd"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.PasswordSet(tt.args.iri, tt.args.pw); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("PasswordSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveKey(t *testing.T) {
	type args struct {
		iri vocab.IRI
		key crypto.PrivateKey
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		want     *vocab.PublicKey
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			want:    nil,
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withItems},
			wantErr:  fmt.Errorf("x509: unknown key type while marshaling PKCS#8: %T", nil),
		},
		{
			name: "~jdoe with private key",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				key: pk,
			},
			want: apPublic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			got, err := r.SaveKey(tt.args.iri, tt.args.key)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("SaveKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SaveKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_SaveMetadata(t *testing.T) {
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withItems},
			wantErr:  errors.Newf("Could not save nil metadata"),
		},
		{
			name: "~jdoe with simple pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   []byte("asd"),
			},
		},
		{
			name: "~jdoe with key/pw metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m: Metadata{
					Pw:         []byte("asd"),
					PrivateKey: pkcs8Pk,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.SaveMetadata(tt.args.iri, tt.args.m); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("SaveMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
