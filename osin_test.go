package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

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

func saveFsItem(it interface{}, basePath string) error {
	if err := os.MkdirAll(basePath, defaultPerm); err != nil {
		return err
	}

	clientFile := getOauthObjectKey(basePath)
	f, err := os.Create(clientFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var raw []byte
	raw, err = json.Marshal(it)
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
	os.MkdirAll(path.Join(tempFolder, folder, clientsBucket), defaultPerm)
	os.MkdirAll(path.Join(tempFolder, folder, accessBucket), defaultPerm)
	os.MkdirAll(path.Join(tempFolder, folder, authorizeBucket), defaultPerm)
	os.MkdirAll(path.Join(tempFolder, folder, refreshBucket), defaultPerm)
	s := repo{path: tempFolder, logFn: defaultLogFn, errFn: defaultLogFn}
	return &s
}

func cleanup(tempFolder string) {
	os.RemoveAll(tempFolder)
}

func TestStor_Close(t *testing.T) {
	s := repo{}
	s.Close()
}

func TestStor_Open(t *testing.T) {
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

func TestStor_ListClients(t *testing.T) {
	tempFolder := t.TempDir()
	for name, tt := range loadClientTests {
		s := initialize(tempFolder)
		if err := saveFsClients(s.oauthPath(clientsBucket), tt.clients...); err != nil {
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

func TestStor_Clone(t *testing.T) {
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

func TestStor_GetClient(t *testing.T) {
	tempFolder := t.TempDir()
	defer cleanup(tempFolder)
	s := initialize(tempFolder)

	for name, tt := range loadClientTests {
		if err := saveFsClients(s.oauthPath(clientsBucket), tt.clients...); err != nil {
			t.Logf("Unable to save clients: %s", err)
			continue
		}
		for i, cl := range tt.clients {
			name = fmt.Sprintf("%s:%d", name, i)
			t.Run(name, func(t *testing.T) {
				client, err := s.GetClient(cl.Id)
				if tt.err != nil && !errors.Is(err, tt.err) {
					t.Errorf("Error when loading clients, expected %s, received %s", tt.err, err)
				}
				if tt.err == nil && err != nil {
					t.Errorf("Unexpected error when loading clients, received %s", err)
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

func TestStor_CreateClient(t *testing.T) {
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
			filePath := getObjectKey(s.oauthPath(clientsBucket, tt.client.Id))
			f, err := os.Open(filePath)
			if err != nil {
				t.Errorf("Unable to read %s client file: %s", filePath, err)
			}
			defer f.Close()

			fi, err := f.Stat()
			if err != nil {
				t.Errorf("error: %+s", err)
			}
			raw := make([]byte, fi.Size())
			_, err = f.Read(raw)
			if err != nil {
				t.Errorf("Unable to read %s client raw data: %s", filePath, err)
			}
			l := new(osin.DefaultClient)
			err = json.Unmarshal(raw, l)
			if err != nil {
				t.Errorf("Unable to unmarshal %s client raw data: %s", filePath, err)
			}
			if !reflect.DeepEqual(l, tt.client) {
				t.Errorf("Error when saving client, expected %#v, received %#v", tt.client, l)
			}
		})
	}
}

func TestStor_UpdateClient(t *testing.T) {
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
			filePath := getObjectKey(s.oauthPath(clientsBucket, tt.client.Id))
			f, err := os.Open(filePath)
			if err != nil {
				t.Errorf("Unable to read %s client file: %s", filePath, err)
			}
			defer f.Close()

			fi, _ := f.Stat()
			raw := make([]byte, fi.Size())
			_, err = f.Read(raw)
			if err != nil {
				t.Errorf("Unable to read %s client raw data: %s", filePath, err)
			}
			l := new(osin.DefaultClient)
			err = json.Unmarshal(raw, l)
			if err != nil {
				t.Errorf("Unable to unmarshal %s client raw data: %s", filePath, err)
			}
			if !reflect.DeepEqual(l, tt.client) {
				t.Errorf("Error when saving client, expected %#v, received %#v", tt.client, l)
			}
		})
	}
}

func TestStor_LoadAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_LoadAccess(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_LoadRefresh(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_RemoveAccess(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_RemoveAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_RemoveClient(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_RemoveRefresh(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_SaveAccess(t *testing.T) {
	t.Skipf("TODO")
}

func TestStor_SaveAuthorize(t *testing.T) {
	t.Skipf("TODO")
}

func TestNewFSDBStoreStore(t *testing.T) {
	t.Skipf("TODO")
}
