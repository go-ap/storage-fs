package fs

import (
	"os"
	"testing"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/storage-fs/internal/cache"
	"golang.org/x/sys/unix"
)

var wd, _ = os.Open(".")

type fields struct {
	baseURL string
	path    string
	cwd     string
	opened  bool
	cache   cache.CanStore
	logFn   loggerFn
	errFn   loggerFn
}

func Test_New(t *testing.T) {
	testFolder := t.TempDir()

	tests := []struct {
		name    string
		config  Config
		want    fields
		wantErr error
	}{
		{
			name:    "empty",
			config:  Config{},
			wantErr: errMissingPath,
		},
		{
			name:   "valid temp folder",
			config: Config{Path: testFolder},
			want: fields{
				path: testFolder,
				cwd:  os.Getenv("PWD"),
			},
		},
		{
			name:    "invalid permissions",
			config:  Config{Path: "/root/tmp"},
			wantErr: os.ErrPermission,
		},
		{
			name:    "invalid relative file",
			config:  Config{Path: "./not-sure-if-this-should-work-or-not"},
			wantErr: error(unix.ENOENT),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.config)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("New() returned different error than expected = %v, want %v", err, tt.wantErr)
			}
			if got == nil {
				return
			}
			if got.path != tt.want.path {
				t.Errorf("New().path = %v, want %v", got.path, tt.want.path)
			}
			/*
				if got.cwd != tt.want.cwd {
					t.Errorf("New().cwd = %v, want %v", got.cwd, tt.want.cwd)
				}
			*/
			if got.opened != tt.want.opened {
				t.Errorf("New().opened = %v, want %v", got.opened, tt.want.opened)
			}
		})
	}
}

func Test_repo_Open(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: error(unix.ENOENT),
		},
		{
			name:    "empty",
			fields:  fields{},
			wantErr: error(unix.ENOENT),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				baseURL: tt.fields.baseURL,
				path:    tt.fields.path,
				cwd:     tt.fields.cwd,
				opened:  tt.fields.opened,
				cache:   tt.fields.cache,
				logFn:   tt.fields.logFn,
				errFn:   tt.fields.errFn,
			}
			if err := r.Open(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
			}
			if r.path != tt.fields.path {
				t.Errorf("Open() path is not correct = %s, want %s", r.path, tt.fields.path)
			}
			if r.cwd != tt.fields.path {
				t.Errorf("Open() cwd path is not correct = %s, want %s", r.path, tt.fields.path)
			}
			defer r.Close()
		})
	}
}

func expectedCol(id vocab.IRI) *vocab.OrderedCollection {
	return &vocab.OrderedCollection{
		ID:   id,
		Type: vocab.OrderedCollectionType,
	}
}

func Test_repo_createCollection(t *testing.T) {
	type fields struct {
		baseURL string
		path    string
		cwd     string
		opened  bool
		cache   cache.CanStore
		logFn   loggerFn
		errFn   loggerFn
	}
	tests := []struct {
		name     string
		fields   fields
		iri      vocab.IRI
		expected vocab.CollectionInterface
		wantErr  bool
	}{
		{
			name: "example.com/replies",
			fields: fields{
				baseURL: "https://example.com",
				opened:  false,
				logFn:   t.Logf,
				errFn:   t.Errorf,
			},
			iri:      "https://example.com/replies",
			expected: expectedCol("https://example.com/replies"),
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				baseURL: tt.fields.baseURL,
				path:    t.TempDir(),
				cwd:     tt.fields.cwd,
				opened:  tt.fields.opened,
				cache:   cache.New(false),
				logFn:   tt.fields.logFn,
				errFn:   tt.fields.errFn,
			}
			col, err := r.createCollection(tt.iri)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !vocab.ItemsEqual(col, tt.expected) {
				t.Errorf("Returned collection is not equal to expected %v: %v", tt.expected, col)
			}
			saved, err := r.Load(tt.iri)
			if err != nil {
				t.Errorf("Unable to load collection at IRI %q: %s", tt.iri, err)
			}
			if !vocab.ItemsEqual(saved, tt.expected) {
				t.Errorf("Saved collection is not equal to expected %v: %v", tt.expected, saved)
			}
		})
	}
}
