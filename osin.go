package fs

import (
	"encoding/gob"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

const (
	defaultPerm     = os.ModeDir | os.ModePerm | 0700
	clientsBucket   = "clients"
	authorizeBucket = "authorize"
	accessBucket    = "access"
	refreshBucket   = "refresh"
	folder          = "oauth"
)

func init() {
	gob.Register(vocab.IRI(""))
}

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	Extra       interface{}
}

type auth struct {
	Client      string
	Code        string
	ExpiresIn   time.Duration
	Scope       string
	RedirectURI string
	State       string
	CreatedAt   time.Time
	Extra       interface{}
}

type acc struct {
	Client       string
	Authorize    string
	Previous     string
	AccessToken  string
	RefreshToken string
	ExpiresIn    time.Duration
	Scope        string
	RedirectURI  string
	CreatedAt    time.Time
	Extra        interface{}
}

type ref struct {
	Access string
}

func interfaceIsNil(c interface{}) bool {
	return reflect.ValueOf(c).Kind() == reflect.Ptr && reflect.ValueOf(c).IsNil()
}

func mkDirIfNotExists(p string) (err error) {
	p, err = filepath.Abs(p)
	if err != nil {
		return err
	}
	fi, err := os.Stat(p)
	if err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(p, defaultPerm); err != nil {
			return err
		}
		fi, err = os.Stat(p)
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.Errorf("path exists, and is not a folder %s", p)
	}
	return nil
}

func isOauthStorageCollectionKey(p string) bool {
	base := path.Base(p)
	return base == clientsBucket || base == authorizeBucket || base == accessBucket || base == refreshBucket
}

const (
	oauthObjectKey = "__raw"
)

func getOauthObjectKey(p string) string {
	return path.Join(p, oauthObjectKey)
}

func loadRawFromOauthPath(itPath string) ([]byte, error) {
	f, err := os.Open(itPath)
	if err != nil {
		return nil, errors.Annotatef(err, "Unable find path %s", itPath)
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Annotatef(err, "Unable stat file at path %s", itPath)
	}
	raw := make([]byte, fi.Size())
	cnt, err := f.Read(raw)
	if err != nil {
		return nil, errors.Annotatef(err, "Unable read file at path %s", itPath)
	}
	if cnt != len(raw) {
		return nil, errors.Annotatef(err, "Unable read the whole file at path %s", itPath)
	}
	return raw, nil
}

func (r *repo) loadFromOauthPath(itPath string, loaderFn func([]byte) error) (uint, error) {
	var err error
	var cnt uint = 0
	if isOauthStorageCollectionKey(itPath) {
		err = filepath.Walk(itPath, func(p string, info os.FileInfo, err error) error {
			if err != nil && os.IsNotExist(err) {
				return errors.NotFoundf("%s not found", p)
			}

			it, _ := loadRawFromOauthPath(getOauthObjectKey(p))
			if it != nil {
				if err := loaderFn(it); err == nil {
					cnt++
				}
			}
			return nil
		})
	} else {
		var raw []byte
		raw, err = loadRawFromOauthPath(getOauthObjectKey(itPath))
		if err != nil {
			return cnt, errors.NewNotFound(err, "not found")
		}
		if raw != nil {
			if err := loaderFn(raw); err == nil {
				cnt++
			}
		}
	}
	return cnt, err
}

// Clone
func (r *repo) Clone() osin.Storage {
	return r
}

// ListClients
func (r *repo) ListClients() ([]osin.Client, error) {
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	clients := make([]osin.Client, 0)

	_, err = r.loadFromOauthPath(r.oauthPath(clientsBucket), func(raw []byte) error {
		cl := cl{}
		if err := decodeFn(raw, &cl); err != nil {
			return err
		}
		d := osin.DefaultClient{
			Id:          cl.Id,
			Secret:      cl.Secret,
			RedirectUri: cl.RedirectUri,
			UserData:    cl.Extra,
		}
		clients = append(clients, &d)
		return nil
	})

	return clients, err
}

func (r *repo) loadClientFromPath(clientPath string) (osin.Client, error) {
	c := new(osin.DefaultClient)
	_, err := r.loadFromOauthPath(clientPath, func(raw []byte) error {
		cl := cl{}
		if err := decodeFn(raw, &cl); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		c.Id = cl.Id
		c.Secret = cl.Secret
		c.RedirectUri = cl.RedirectUri
		c.UserData = cl.Extra
		return nil
	})
	return c, err
}

func (r repo) oauthPath(pieces ...string) string {
	pieces = append([]string{r.path, folder}, pieces...)
	return filepath.Join(pieces...)
}

// GetClient
func (r *repo) GetClient(id string) (osin.Client, error) {
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.loadClientFromPath(r.oauthPath(clientsBucket, id))
}

func createFolderIfNotExists(p string) error {
	if _, err := os.Open(p); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err = os.MkdirAll(p, os.ModeDir|os.ModePerm|0770); err != nil {
			return err
		}
	}
	return nil
}

func putItem(basePath string, it interface{}) error {
	raw, err := encodeFn(it)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal %T", it)
	}
	return putRaw(basePath, raw)
}

func putRaw(basePath string, raw []byte) error {
	filePath := getOauthObjectKey(basePath)
	f, err := os.Open(filePath)
	if err != nil && os.IsNotExist(err) {
		f, err = os.Create(filePath)
	}
	if err != nil {
		return errors.Annotatef(err, "Unable to save data to path %s", filePath)
	}
	defer f.Close()
	n, err := f.Write(raw)
	if n != len(raw) {
		return errors.Newf("Unable to save all data to path %s, only saved %d bytes", filePath, n)
	}
	return err
}

// UpdateClient
func (r *repo) UpdateClient(c osin.Client) error {
	if interfaceIsNil(c) {
		return nil
	}
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs *repositoryage")
	}
	defer r.Close()
	if err != nil {
		r.errFn("Failed to update client id: %s: %+s", c.GetId(), err)
		return errors.Annotatef(err, "Invalid user-data")
	}
	cl := cl{
		Id:          c.GetId(),
		Secret:      c.GetSecret(),
		RedirectUri: c.GetRedirectUri(),
		Extra:       c.GetUserData(),
	}
	clientPath := r.oauthPath(clientsBucket, cl.Id)
	if err = createFolderIfNotExists(clientPath); err != nil {
		return errors.Annotatef(err, "Invalid path %s", clientPath)
	}
	return putItem(clientPath, cl)
}

// CreateClient
func (r *repo) CreateClient(c osin.Client) error {
	return r.UpdateClient(c)
}

// RemoveClient
func (r *repo) RemoveClient(id string) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs *repositoryage")
	}
	defer r.Close()
	return os.RemoveAll(r.oauthPath(clientsBucket, id))
}

// SaveAuthorize saves authorize data.
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs storage")
	}
	defer r.Close()

	auth := auth{
		Client:      data.Client.GetId(),
		Code:        data.Code,
		ExpiresIn:   time.Duration(data.ExpiresIn),
		Scope:       data.Scope,
		RedirectURI: data.RedirectUri,
		State:       data.State,
		CreatedAt:   data.CreatedAt.UTC(),
		Extra:       data.UserData,
	}
	authorizePath := r.oauthPath(authorizeBucket, auth.Code)
	if err = createFolderIfNotExists(authorizePath); err != nil {
		return errors.Annotatef(err, "Invalid path %s", authorizePath)
	}
	return putItem(authorizePath, auth)
}

func (r *repo) loadAuthorizeFromPath(authPath string) (*osin.AuthorizeData, error) {
	data := new(osin.AuthorizeData)
	_, err := r.loadFromOauthPath(authPath, func(raw []byte) error {
		auth := auth{}
		if err := decodeFn(raw, &auth); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		data.Code = auth.Code
		data.ExpiresIn = int32(auth.ExpiresIn)
		data.Scope = auth.Scope
		data.RedirectUri = auth.RedirectURI
		data.State = auth.State
		data.CreatedAt = auth.CreatedAt
		data.UserData = auth.Extra

		if data.ExpireAt().Before(time.Now().UTC()) {
			err := errors.Errorf("Token expired at %s.", data.ExpireAt().String())
			r.errFn("Code %s: %s", auth.Code, err)
			return err
		}
		cl, err := r.loadClientFromPath(r.oauthPath(clientsBucket, auth.Client))
		if err != nil {
			return err
		}
		data.Client = &osin.DefaultClient{
			Id:          cl.GetId(),
			Secret:      cl.GetSecret(),
			RedirectUri: cl.GetRedirectUri(),
			UserData:    cl.GetUserData(),
		}
		return nil
	})
	return data, err
}

// LoadAuthorize looks up AuthorizeData by a code.
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty authorize code")
	}
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.loadAuthorizeFromPath(r.oauthPath(authorizeBucket, code))
}

// RemoveAuthorize revokes or deletes the authorization code.
func (r *repo) RemoveAuthorize(code string) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs *repositoryage")
	}
	defer r.Close()
	return os.RemoveAll(r.oauthPath(authorizeBucket, code))
}

// SaveAccess writes AccessData.
func (r *repo) SaveAccess(data *osin.AccessData) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs storage")
	}
	defer r.Close()
	prev := ""
	authorizeData := &osin.AuthorizeData{}

	if data.AccessData != nil {
		prev = data.AccessData.AccessToken
	}

	if data.AuthorizeData != nil {
		authorizeData = data.AuthorizeData
	}

	if data.RefreshToken != "" {
		ref := ref{
			Access: data.AccessToken,
		}
		refreshPath := r.oauthPath(refreshBucket, data.RefreshToken)
		if err = createFolderIfNotExists(refreshPath); err != nil {
			return errors.Annotatef(err, "Invalid path %s", refreshPath)
		}
		if err := putItem(refreshPath, ref); err != nil {
			return err
		}
	}

	if data.Client == nil {
		return errors.Newf("data.Client must not be nil")
	}

	acc := acc{
		Client:       data.Client.GetId(),
		Authorize:    authorizeData.Code,
		Previous:     prev,
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresIn:    time.Duration(data.ExpiresIn),
		Scope:        data.Scope,
		RedirectURI:  data.RedirectUri,
		CreatedAt:    data.CreatedAt.UTC(),
		Extra:        data.UserData,
	}
	authorizePath := r.oauthPath(accessBucket, acc.AccessToken)
	if err = createFolderIfNotExists(authorizePath); err != nil {
		return errors.Annotatef(err, "Invalid path %s", authorizePath)
	}
	return putItem(authorizePath, acc)
}

func (r *repo) loadAccessFromPath(accessPath string) (*osin.AccessData, error) {
	result := new(osin.AccessData)
	_, err := r.loadFromOauthPath(accessPath, func(raw []byte) error {
		access := acc{}
		if err := decodeFn(raw, &access); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal access object")
		}
		result.AccessToken = access.AccessToken
		result.RefreshToken = access.RefreshToken
		result.ExpiresIn = int32(access.ExpiresIn)
		result.Scope = access.Scope
		result.RedirectUri = access.RedirectURI
		result.CreatedAt = access.CreatedAt.UTC()
		result.UserData = access.Extra

		if access.Authorize != "" {
			data, err := r.loadAuthorizeFromPath(r.oauthPath(authorizeBucket, access.Authorize))
			if err != nil {
				err := errors.Annotatef(err, "Unable to load authorize data for current access token %s.", access.AccessToken)
				r.errFn("Authorize code %s: %s", access.AccessToken, err)
				return nil
			}
			if data.ExpireAt().Before(time.Now().UTC()) {
				err := errors.Errorf("Token expired at %s.", data.ExpireAt().String())
				r.errFn("Access token: %s: %s", access.AccessToken, err)
				return nil
			}
			result.AuthorizeData = data
		}
		if access.Previous != "" {
			_, err := r.loadFromOauthPath(accessPath, func(raw []byte) error {
				access := acc{}
				if err := decodeFn(raw, &access); err != nil {
					return errors.Annotatef(err, "Unable to unmarshal access object")
				}
				prev := new(osin.AccessData)
				prev.AccessToken = access.AccessToken
				prev.RefreshToken = access.RefreshToken
				prev.ExpiresIn = int32(access.ExpiresIn)
				prev.Scope = access.Scope
				prev.RedirectUri = access.RedirectURI
				prev.CreatedAt = access.CreatedAt.UTC()
				prev.UserData = access.Extra
				result.AccessData = prev
				return nil
			})
			if err != nil {
				err := errors.Annotatef(err, "Unable to load previous access token for %s.", access.AccessToken)
				r.errFn("Access code %s: %s", access.AccessToken, err)
				return nil
			}
		}
		return nil
	})
	return result, err
}

// LoadAccess retrieves access data by token. Client information MUST be loaded together.
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty access code")
	}
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return r.loadAccessFromPath(r.oauthPath(accessBucket, code))
}

// RemoveAccess revokes or deletes an AccessData.
func (r *repo) RemoveAccess(code string) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs *repositoryage")
	}
	defer r.Close()
	return os.RemoveAll(r.oauthPath(accessBucket, code))
}

// LoadRefresh retrieves refresh AccessData. Client information MUST be loaded together.
func (r *repo) LoadRefresh(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty refresh code")
	}
	return nil, nil
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(code string) error {
	err := r.Open()
	if err != nil {
		return errors.Annotatef(err, "Unable to open fs *repositoryage")
	}
	defer r.Close()
	return os.RemoveAll(r.oauthPath(refreshBucket, code))
}
