package fs

import (
	"crypto"
	"os"
	"reflect"
	"testing"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
)

func Test_repo_GenKey(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
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
			if err := r.GenKey(tt.args.iri); !errors.Is(err, tt.wantErr) {
				t.Errorf("GenKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_LoadKey(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    crypto.PrivateKey
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			want:    nil,
			wantErr: errNotOpen,
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
			got, err := r.LoadKey(tt.args.iri)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_LoadMetadata(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
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
			if err := r.LoadMetadata(tt.args.iri, tt.args.m); !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_PasswordCheck(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
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
			if err := r.PasswordCheck(tt.args.iri, tt.args.pw); !errors.Is(err, tt.wantErr) {
				t.Errorf("PasswordCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_PasswordSet(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
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
			if err := r.PasswordSet(tt.args.iri, tt.args.pw); !errors.Is(err, tt.wantErr) {
				t.Errorf("PasswordSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveKey(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
		key crypto.PrivateKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *vocab.PublicKey
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			want:    nil,
			wantErr: errNotOpen,
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
			got, err := r.SaveKey(tt.args.iri, tt.args.key)
			if !errors.Is(err, tt.wantErr) {
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
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
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
			if err := r.SaveMetadata(tt.args.iri, tt.args.m); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
