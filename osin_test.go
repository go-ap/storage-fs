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

	"git.sr.ht/~mariusor/lw"
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

func Test_repo_Close(t *testing.T) {
	s := repo{}
	s.root, _ = os.OpenRoot(os.TempDir())
	s.Close()
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

func Test_repo_ListClients(t *testing.T) {
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

func Test_repo_Clone(t *testing.T) {
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

func Test_repo_GetClient(t *testing.T) {
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

func Test_repo_CreateClient(t *testing.T) {
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

func Test_repo_UpdateClient(t *testing.T) {
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

func Test_repo_LoadAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errors.NotFoundf("Empty access code"),
		},
		{
			name:     "save access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization, withAccess},
			code:     "access-666",
			want:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			got, err := r.LoadAccess(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadAccess() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errors.NotFoundf("Empty refresh code"),
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			got, err := r.LoadRefresh(tt.code)
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

func Test_repo_RemoveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.RemoveAccess(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "remove auth",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			code:     "test-auth",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveAuthorize(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveClient(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove client",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient},
			code:     "test-client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveClient(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "not open",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty not open",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot},
			code:     "test",
			wantErr:  errNotOpen,
		},
		{
			name:     "mock access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withAccess},
			code:     "access-666",
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			defer r.Close()

			if err := r.RemoveRefresh(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		auth     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name:    "empty",
			path:    t.TempDir(),
			wantErr: errors.Errorf("unable to save nil authorization data"),
		},
		{
			name:     "save mock auth",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withClient},
			auth:     mockAuth("test-code123", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			defer r.Close()

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

func Test_repo_LoadAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		code     string
		want     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name: "empty",
			path: t.TempDir(),
		},
		{
			name:     "authorized",
			path:     t.TempDir(),
			code:     "test-code",
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			want:     mockAuth("test-code", defaultClient),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			defer r.Close()

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

func Test_repo_SaveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		data     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "save access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withClient, withAuthorization},
			data:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.SaveAccess(tt.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
