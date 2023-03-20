package fs

import (
	"os"
	"path/filepath"
	"testing"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/storage-fs/internal/cache"
	"golang.org/x/sys/unix"
)

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
				cwd:  testCWD,
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
			if got.cwd != tt.want.cwd {
				t.Errorf("New().cwd = %v, want %v", got.cwd, tt.want.cwd)
			}
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
			name:   "skips opening",
			fields: fields{opened: true},
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
			err := r.Open()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
			}
			opened := err == nil
			if r.opened != opened {
				t.Errorf("Open() opened is not correct = %t, want %t", r.opened, opened)
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

func Test_repo_Load(t *testing.T) {
	mocksPath, err := filepath.Abs("./mocks")
	if err != nil {
		t.Errorf("%s", err)
	}
	type fields struct {
		baseURL string
		path    string
		cwd     string
		opened  bool
		cache   cache.CanStore
	}
	tests := []struct {
		name    string
		fields  fields
		args    vocab.IRI
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    "",
			want:    nil,
			wantErr: error(unix.ENOENT),
		},
		{
			name: "empty iri gives us the root",
			fields: fields{
				baseURL: "example.com",
				path:    mocksPath,
			},
			args: "",
			want: vocab.ItemCollection{vocab.Actor{Type: vocab.ApplicationType, ID: "https://example.com"}},
		},
		{
			name: "root iri gives us the root",
			fields: fields{
				baseURL: "example.com",
				path:    mocksPath,
			},
			args: "https://example.com",
			want: vocab.ItemCollection{vocab.Actor{Type: vocab.ApplicationType, ID: "https://example.com"}},
		},
		{
			name: "invalid iri gives 404",
			fields: fields{
				baseURL: "example.com",
				path:    mocksPath,
			},
			args:    "https://example.com/dsad",
			want:    vocab.ItemCollection{},
			wantErr: os.ErrNotExist,
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
				logFn:   t.Logf,
				errFn:   t.Logf,
			}
			got, err := r.Load(tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !vocab.ItemsEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}
