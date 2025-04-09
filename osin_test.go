package fs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"

	"git.sr.ht/~mariusor/lw"
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

func initialize(tempFolder string) *repo {
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
		s := initialize(tempFolder)
		clientsPath := filepath.Join(tempFolder, folder, clientsBucket)
		if err := saveFsClients(clientsPath, tt.clients...); err != nil {
			t.Logf("Unable to save clients: %s", err)
			cleanup(tempFolder)
			continue
		}
		t.Run(name, func(t *testing.T) {
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
	s := initialize(tempFolder)

	for name, tt := range loadClientTests {
		clientsPath := filepath.Join(tempFolder, folder, clientsBucket)
		if err := saveFsClients(clientsPath, tt.clients...); err != nil {
			t.Logf("Unable to save clients: %s", err)
			continue
		}
		for i, cl := range tt.clients {
			name = fmt.Sprintf("%s:%d", name, i)
			t.Run(name, func(t *testing.T) {
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
	s := initialize(tempFolder)

	for name, tt := range createClientTests {
		t.Run(name, func(t *testing.T) {
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
	s := initialize(tempFolder)

	for name, tt := range createClientTests {
		t.Run(name, func(t *testing.T) {
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

func Test_LoadAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func Test_LoadAccess(t *testing.T) {
	t.Skipf("TODO")
}

func Test_LoadRefresh(t *testing.T) {
	t.Skipf("TODO")
}

func Test_RemoveAccess(t *testing.T) {
	t.Skipf("TODO")
}

func Test_RemoveAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func Test_RemoveClient(t *testing.T) {
	t.Skipf("TODO")
}

func Test_RemoveRefresh(t *testing.T) {
	t.Skipf("TODO")
}

func Test_SaveAccess(t *testing.T) {
	t.Skipf("TODO")
}

func Test_SaveAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func TestNewFSDBStoreStore(t *testing.T) {
	t.Skipf("TODO")
}
