package fs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
)

func saveFsClients(base string, clients ...cl) error {
	for _, c := range clients {
		if err := saveFsClient(c, base); err != nil {
			return err
		}
	}
	return nil
}

func saveFsItem(it any, basePath string) error {
	if err := os.MkdirAll(basePath, defaultDirPerm); err != nil {
		return err
	}

	clientFile := filepath.Join(basePath, oauthObjectKey)
	f, err := os.Create(clientFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	var raw []byte
	raw, err = encodeFn(it)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err != nil {
		return err
	}
	return nil
}

func saveFsClient(client cl, basePath string) error {
	if len(client.Id) == 0 {
		return nil
	}
	testClientPath := path.Join(basePath, client.Id)
	return saveFsItem(client, testClientPath)
}

func initializeTemp(tempFolder string) *repo {
	_ = os.MkdirAll(path.Join(tempFolder, folder, clientsBucket), defaultDirPerm)
	_ = os.MkdirAll(path.Join(tempFolder, folder, accessBucket), defaultDirPerm)
	_ = os.MkdirAll(path.Join(tempFolder, folder, authorizeBucket), defaultDirPerm)
	_ = os.MkdirAll(path.Join(tempFolder, folder, refreshBucket), defaultDirPerm)

	s := repo{path: tempFolder, logger: lw.Dev()}
	return &s
}

func cleanup(tempFolder string) {
	_ = os.RemoveAll(tempFolder)
}

func Test_Close(t *testing.T) {
	s := repo{}
	s.root, _ = os.OpenRoot(os.TempDir())
	s.Close()
}

func Test_Open(t *testing.T) {
	s := repo{path: t.TempDir()}
	err := s.Open()
	if err != nil {
		t.Errorf("Expected nil when opening %T", s)
	}
}

var loadClientTests = map[string]struct {
	clients []cl
	want    []osin.Client
	err     error
}{
	"nil": {
		clients: []cl{},
		want:    []osin.Client{},
		err:     nil,
	},
	"test-client-id": {
		clients: []cl{
			{
				Id: "test-client-id",
			},
		},
		want: []osin.Client{
			&osin.DefaultClient{
				Id: "test-client-id",
			},
		},
		err: nil,
	},
}

func Test_ListClients(t *testing.T) {
	tempFolder := t.TempDir()
	for name, tt := range loadClientTests {
		s := initializeTemp(tempFolder)
		clientsPath := filepath.Join(tempFolder, folder, clientsBucket)
		if err := saveFsClients(clientsPath, tt.clients...); err != nil {
			t.Logf("Unable to save clients: %s", err)
			cleanup(tempFolder)
			continue
		}
		t.Run(name, func(t *testing.T) {
			_ = s.Open()
			defer s.Close()

			clients, err := s.ListClients()
			if tt.err != nil && !errors.Is(err, tt.err) {
				t.Errorf("Error when loading clients, expected %s, received %s", tt.err, err)
			}
			if tt.err == nil && err != nil {
				t.Errorf("Unexpected error when loading clients, received %s", err)
			}
			if len(clients) != len(tt.want) {
				t.Errorf("Error when loading clients, expected %d items, received %d", len(tt.want), len(clients))
			}
			if !reflect.DeepEqual(clients, tt.want) {
				t.Errorf("Error when loading clients, expected %#v, received %#v", tt.want, clients)
			}
		})
		cleanup(tempFolder)
	}
}

func Test_Clone(t *testing.T) {
	s := new(repo)
	ss := s.Clone()
	s1, ok := ss.(*repo)
	if !ok {
		t.Errorf("Error when cloning repoage, unable to convert interface back to %T: %T", s, ss)
	}
	if !reflect.DeepEqual(s, s1) {
		t.Errorf("Error when cloning repoage, invalid pointer returned %p: %p", s, s1)
	}
}

func Test_GetClient(t *testing.T) {
	tempFolder := t.TempDir()
	defer cleanup(tempFolder)
	s := initializeTemp(tempFolder)

	for name, tt := range loadClientTests {
		clientsPath := filepath.Join(tempFolder, folder, clientsBucket)
		if err := saveFsClients(clientsPath, tt.clients...); err != nil {
			t.Logf("Unable to save clients: %s", err)
			continue
		}
		for i, cl := range tt.clients {
			name = fmt.Sprintf("%s:%d", name, i)
			t.Run(name, func(t *testing.T) {
				_ = s.Open()
				defer s.Close()

				client, err := s.GetClient(cl.Id)
				if tt.err != nil && !errors.Is(err, tt.err) {
					t.Errorf("Error when loading clients, expected %s, received %s", tt.err, err)
					return
				}
				if tt.err == nil && err != nil {
					t.Errorf("Unexpected error when loading clients, received %s", err)
					return
				}
				expected := tt.want[i]
				if !reflect.DeepEqual(client, expected) {
					t.Errorf("Error when loading clients, expected %#v, received %#v", expected, client)
				}
			})
		}
	}
}

var createClientTests = map[string]struct {
	client *osin.DefaultClient
	err    error
}{
	"nil": {
		nil,
		nil,
	},
	"test-client": {
		&osin.DefaultClient{
			Id:          "test-client",
			Secret:      "asd",
			RedirectUri: "https://example.com",
			UserData:    nil,
		},
		nil,
	},
}

func Test_CreateClient(t *testing.T) {
	tempFolder := t.TempDir()
	defer cleanup(tempFolder)
	s := initializeTemp(tempFolder)

	for name, tt := range createClientTests {
		t.Run(name, func(t *testing.T) {
			_ = s.Open()
			defer s.Close()

			err := s.CreateClient(tt.client)
			if tt.err != nil && err == nil {
				t.Errorf("Unexpected error when calling CreateClient, received %s", err)
			}
			if tt.client == nil {
				return
			}
			filePath := getObjectKey(filepath.Join(tempFolder, folder, clientsBucket, tt.client.Id))
			f, err := os.Open(filePath)
			if err != nil {
				t.Errorf("Unable to read %s client file: %s", filePath, err)
				return
			}
			defer f.Close()

			fi, err := f.Stat()
			if err != nil {
				t.Errorf("error: %+s", err)
				return
			}
			raw := make([]byte, fi.Size())
			_, err = f.Read(raw)
			if err != nil {
				return
			}
			l := new(osin.DefaultClient)
			err = decodeFn(raw, l)
			if err != nil {
				t.Errorf("Unable to unmarshal %s client raw data: %s", filePath, err)
			}
			if !reflect.DeepEqual(l, tt.client) {
				t.Errorf("Error when saving client, expected %#v, received %#v", tt.client, l)
			}
		})
	}
}

func Test_UpdateClient(t *testing.T) {
	tempFolder := t.TempDir()
	defer cleanup(tempFolder)
	s := initializeTemp(tempFolder)

	for name, tt := range createClientTests {
		t.Run(name, func(t *testing.T) {
			_ = s.Open()
			defer s.Close()

			err := s.CreateClient(tt.client)
			if tt.err != nil && err == nil {
				t.Errorf("Unexpected error when calling CreateClient, received %s", err)
			}
			if tt.client == nil {
				return
			}
			filePath := getObjectKey(filepath.Join(tempFolder, folder, clientsBucket, tt.client.Id))
			f, err := os.Open(filePath)
			if err != nil {
				t.Errorf("Unable to read %s client file: %s", filePath, err)
				return
			}
			defer f.Close()

			fi, err := f.Stat()
			if err != nil {
				t.Errorf("Error: %+s", err)
				return
			}
			raw := make([]byte, fi.Size())
			_, err = f.Read(raw)
			if err != nil {
				t.Errorf("Unable to read %s client raw data: %s", filePath, err)
			}
			l := new(osin.DefaultClient)
			err = decodeFn(raw, l)
			if err != nil {
				t.Errorf("Unable to unmarshal %s client raw data: %s", filePath, err)
			}
			if !reflect.DeepEqual(l, tt.client) {
				t.Errorf("Error when saving client, expected %#v, received %#v", tt.client, l)
			}
		})
	}
}

func Test_LoadAccess(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		code string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *osin.AccessData
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errors.NotFoundf("Empty access code"),
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
			got, err := r.LoadAccess(tt.args.code)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.wantErr) {
				t.Errorf("LoadAccess() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_LoadRefresh(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		code string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *osin.AccessData
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errors.NotFoundf("Empty refresh code"),
		},
		{
			name:    "empty",
			fields:  fields{},
			args:    args{"test"},
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
			got, err := r.LoadRefresh(tt.args.code)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.wantErr) {
				t.Errorf("LoadRefresh() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_RemoveAccess(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		code string
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
			if err := r.RemoveAccess(tt.args.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_RemoveAuthorize(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		data *osin.AccessData
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
			if err := r.SaveAccess(tt.args.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_RemoveClient(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		code string
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
			if err := r.RemoveClient(tt.args.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_RemoveRefresh(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		code string
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
			if err := r.RemoveRefresh(tt.args.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_SaveAuthorize(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setup   func(*repo) error
		auth    *osin.AuthorizeData
		wantErr error
	}{
		{
			name:    "empty",
			path:    t.TempDir(),
			wantErr: errors.Errorf("unable to save nil authorization data"),
		},
		{
			name: "save mock auth",
			path: t.TempDir(),
			setup: func(r *repo) error {
				return r.CreateClient(defaultClient)
			},
			auth:    mockAuth("test-code123", defaultClient),
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.path,
				logger: logger,
				cache:  cache.New(true),
			}
			if err := r.Open(); err != nil {
				t.Errorf("Open before SaveAuthorize() error = %v", err)
				return
			}
			if tt.setup != nil {
				if err := tt.setup(r); err != nil {
					t.Errorf("Setup before SaveAuthorize() error = %v", err)
					return
				}
			}
			err := r.SaveAuthorize(tt.auth)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			got, err := r.LoadAuthorize(tt.auth.Code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			gotJson, _ := json.Marshal(got)
			wantJson, _ := json.Marshal(tt.auth)
			if !bytes.Equal(gotJson, wantJson) {
				t.Errorf("SaveAuthorize() got =\n%s\n====\n%s", gotJson, wantJson)
			}
		})
	}
}

var (
	defaultClient = &osin.DefaultClient{
		Id:          "test-client",
		Secret:      "asd",
		RedirectUri: "https://example.com",
		UserData:    nil,
	}
	mockAuth = func(code string, cl osin.Client) *osin.AuthorizeData {
		return &osin.AuthorizeData{
			Client:    cl,
			Code:      code,
			ExpiresIn: 10,
			CreatedAt: time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
			UserData:  vocab.IRI("https://example.com/jdoe"),
		}
	}
)

func Test_LoadAuthorize(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setup   func(*repo) error
		code    string
		want    *osin.AuthorizeData
		wantErr error
	}{
		{
			name: "empty",
			path: t.TempDir(),
		},
		{
			name: "authorized",
			path: t.TempDir(),
			code: "test-code",
			setup: func(r *repo) error {
				if err := r.CreateClient(defaultClient); err != nil {
					return err
				}
				if err := r.SaveAuthorize(mockAuth("test-code", defaultClient)); err != nil {
					return err
				}
				return nil
			},
			want: mockAuth("test-code", defaultClient),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.path,
				logger: logger,
				cache:  cache.New(true),
			}
			if err := r.Open(); err != nil {
				t.Errorf("Open before LoadAuthorize() error = %v", err)
				return
			}
			if tt.setup != nil {
				if err := tt.setup(r); err != nil {
					t.Errorf("Setup before LoadAuthorize() error = %v", err)
					return
				}
			}
			got, err := r.LoadAuthorize(tt.code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			gotJson, _ := json.Marshal(got)
			wantJson, _ := json.Marshal(tt.want)
			if !bytes.Equal(gotJson, wantJson) {
				t.Errorf("LoadAuthorize() got =\n%s\n====\n%s", gotJson, wantJson)
			}
		})
	}
}

func Test_SaveAccess(t *testing.T) {
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		data *osin.AccessData
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
			if err := r.SaveAccess(tt.args.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
