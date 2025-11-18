package fs

import (
	"os"
	"path/filepath"
	"testing"

	"git.sr.ht/~mariusor/lw"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
)

func createForbiddenDir(t *testing.T) string {
	forbiddenPath := filepath.Join(t.TempDir(), "forbidden")
	err := os.MkdirAll(forbiddenPath, 0o000)
	if err != nil {
		t.Fatalf("unable to create forbidden test path %s: %s", forbiddenPath, err)
	}
	return forbiddenPath
}

func TestBootstrap(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: os.ErrNotExist,
		},
		{
			name: "temp",
			arg:  Config{Path: filepath.Join(t.TempDir(), "test")},
		},
		{
			name: "forbidden",
			arg:  Config{Path: createForbiddenDir(t)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Bootstrap(tt.arg); !errors.Is(err, tt.wantErr) {
				t.Errorf("Bootstrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClean(t *testing.T) {
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: nil,
		},
		{
			name:    "temp - exists",
			arg:     Config{Path: t.TempDir()},
			wantErr: nil,
		},
		{
			name:    "temp - does not exists",
			arg:     Config{Path: filepath.Join(t.TempDir(), "test")},
			wantErr: nil,
		},
		{
			name: "forbidden",
			arg:  Config{Path: createForbiddenDir(t)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Clean(tt.arg); !errors.Is(err, tt.wantErr) {
				t.Errorf("Clean() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_Reset(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "empty",
			fields: fields{
				path:   "",
				root:   nil,
				index:  nil,
				cache:  nil,
				logger: nil,
			},
		},
		{
			name:   "not empty",
			fields: fields{index: newBitmap()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.fields.path,
				root:   tt.fields.root,
				index:  tt.fields.index,
				cache:  tt.fields.cache,
				logger: tt.fields.logger,
			}
			r.Reset()

			if r.cache == nil {
				t.Errorf("Reset() didn't reinitialize the cache map")
			}
			if tt.fields.index != nil {
				if r.index == nil || r.index == tt.fields.index {
					t.Errorf("Reset() didn't reinitialize the bitmap index")
				}
			}
		})
	}
}
