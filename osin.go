package fs

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

const (
	defaultDirPerm      = 0o700
	defaultFilePerm     = 0o600
	defaultNewFileFlags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	clientsBucket   = "clients"
	authorizeBucket = "authorize"
	accessBucket    = "access"
	refreshBucket   = "refresh"
	folder          = "oauth"

	oauthObjectKey = "__raw"
)

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	UserData    any
}

type auth struct {
	Client      cl
	Code        string
	ExpiresIn   time.Duration
	Scope       string
	RedirectURI string
	State       string
	CreatedAt   time.Time
	UserData    vocab.IRI
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
	Extra        any
}

type ref struct {
	Access string
}

func interfaceIsNil(c any) bool {
	return reflect.ValueOf(c).Kind() == reflect.Ptr && reflect.ValueOf(c).IsNil()
}

func mkDirIfNotExists(root *os.Root, p string) (err error) {
	fi, err := root.Stat(p)
	if err == nil {
		return nil
	} else {
		if os.IsExist(err) {
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
	}
	pieces := strings.Split(p, string(os.PathSeparator))
	for i := range pieces {
		pp := filepath.Join(pieces[:i+1]...)
		if err = root.Mkdir(pp, defaultDirPerm); err != nil && !os.IsExist(err) {
			return err
		}
	}
	fi, err = root.Stat(p)
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

func (r *repo) openOauthRoot() (*os.Root, error) {
	if err := mkDirIfNotExists(r.root, folder); err != nil {
		return nil, errors.Annotatef(err, "Invalid path %s", folder)
	}

	return r.root.OpenRoot(folder)
}

func (r *repo) loadFromOauthPath(itPath string, loaderFn func([]byte) error) (uint, error) {
	root, err := r.openOauthRoot()
	if err != nil {
		return 0, err
	}
	defer root.Close()

	var cnt uint = 0
	if isOauthStorageCollectionKey(itPath) {
		err = fs.WalkDir(root.FS(), itPath, func(p string, info os.DirEntry, err error) error {
			if err != nil && os.IsNotExist(err) {
				return errors.NotFoundf("%s not found", sanitizePath(p, r.path))
			}

			it, _ := loadRaw(root, getObjectKey(p))
			if it != nil {
				if err := loaderFn(it); err == nil {
					cnt++
				}
			}
			return nil
		})
	} else {
		var raw []byte
		raw, err = loadRaw(root, getObjectKey(itPath))
		if err != nil {
			return cnt, errors.NewNotFound(asPathErr(err, r.path), "not found")
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
	clients := make([]osin.Client, 0)

	_, err := r.loadFromOauthPath(r.oauthClientPath(clientsBucket), func(raw []byte) error {
		cl := cl{}
		if err := decodeFn(raw, &cl); err != nil {
			return err
		}
		d := osin.DefaultClient{
			Id:          cl.Id,
			Secret:      cl.Secret,
			RedirectUri: cl.RedirectUri,
			UserData:    cl.UserData,
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
		c.UserData = cl.UserData
		return nil
	})
	return c, err
}

func (r *repo) oauthClientPath(pieces ...string) string {
	for i := range pieces {
		pieces[i] = strings.Replace(pieces[i], "https://", "", 1)
		pieces[i] = strings.Replace(pieces[i], "http://", "", 1)
	}
	return filepath.Join(pieces...)
}

// GetClient
func (r *repo) GetClient(id string) (osin.Client, error) {
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}
	return r.loadClientFromPath(r.oauthClientPath(clientsBucket, id))
}

func putItem(root *os.Root, basePath string, it any) error {
	raw, err := encodeFn(it)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal %T", it)
	}
	return putRaw(root, getObjectKey(basePath), raw)
}

func putRaw(root *os.Root, filePath string, raw []byte) error {
	if err := mkDirIfNotExists(root, filepath.Dir(filePath)); err != nil {
		return errors.Annotatef(err, "unable to create parent folder for %s", filePath)
	}

	f, err := root.OpenFile(filePath, defaultNewFileFlags, defaultFilePerm)
	if err != nil {
		return errors.Annotatef(err, "unable to save data to path %s", filePath)
	}

	defer func() {
		_ = f.Close()
	}()

	wrote, err := f.Write(raw)
	if err != nil {
		return errors.Annotatef(err, "could not store encoded object")
	}
	if wrote != len(raw) {
		return errors.Annotatef(err, "failed writing object")
	}
	return nil
}

// UpdateClient
func (r *repo) UpdateClient(c osin.Client) error {
	if interfaceIsNil(c) {
		return nil
	}
	cl := cl{
		Id:          c.GetId(),
		Secret:      c.GetSecret(),
		RedirectUri: c.GetRedirectUri(),
		UserData:    c.GetUserData(),
	}

	root, err := r.openOauthRoot()
	if err != nil {
		return err
	}

	clientPath := r.oauthClientPath(clientsBucket, cl.Id)
	return putItem(root, clientPath, cl)
}

// CreateClient
func (r *repo) CreateClient(c osin.Client) error {
	return r.UpdateClient(c)
}

// RemoveClient
func (r *repo) RemoveClient(id string) error {
	return r.root.RemoveAll(r.oauthClientPath(clientsBucket, id))
}

// SaveAuthorize saves authorize data.
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	root, err := r.openOauthRoot()
	if err != nil {
		return errors.Annotatef(err, "Invalid path %s", folder)
	}

	a := auth{
		Client: cl{
			Id:          data.Client.GetId(),
			Secret:      data.Client.GetSecret(),
			RedirectUri: data.Client.GetRedirectUri(),
			UserData:    data.Client.GetUserData(),
		},
		Code:        data.Code,
		ExpiresIn:   time.Duration(data.ExpiresIn),
		Scope:       data.Scope,
		RedirectURI: data.RedirectUri,
		State:       data.State,
		CreatedAt:   data.CreatedAt.UTC(),
	}
	if data.UserData != nil {
		a.UserData = data.UserData.(vocab.IRI)
	}

	authorizePath := filepath.Join(authorizeBucket, a.Code)
	return putItem(root, authorizePath, data)
}

func (r *repo) loadAuthorizeFromPath(authPath string) (*osin.AuthorizeData, error) {
	data := new(osin.AuthorizeData)
	_, err := r.loadFromOauthPath(authPath, func(raw []byte) error {
		a := auth{}
		if err := decodeFn(raw, &a); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		data.Code = a.Code
		data.ExpiresIn = int32(a.ExpiresIn)
		data.Scope = a.Scope
		data.RedirectUri = a.RedirectURI
		data.State = a.State
		data.CreatedAt = a.CreatedAt
		data.UserData = a.UserData

		if data.ExpireAt().Before(time.Now().UTC()) {
			err := errors.Errorf("Token expired at %s.", data.ExpireAt().String())
			r.logger.Errorf("Code %s: %s", a.Code, err)
			return err
		}
		data.Client = &osin.DefaultClient{
			Id:          a.Client.Id,
			Secret:      a.Client.Secret,
			RedirectUri: a.Client.RedirectUri,
			UserData:    a.Client.UserData,
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
	return r.loadAuthorizeFromPath(filepath.Join(authorizeBucket, code))
}

// RemoveAuthorize revokes or deletes the authorization code.
func (r *repo) RemoveAuthorize(code string) error {
	return r.root.RemoveAll(filepath.Join(authorizeBucket, code))
}

// SaveAccess writes AccessData.
func (r *repo) SaveAccess(data *osin.AccessData) error {
	root, err := r.openOauthRoot()
	if err != nil {
		return err
	}

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

		refreshPath := filepath.Join(refreshBucket, data.RefreshToken)
		if err := putItem(root, refreshPath, ref); err != nil {
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
	authorizePath := filepath.Join(accessBucket, acc.AccessToken)
	if err = mkDirIfNotExists(root, authorizePath); err != nil {
		return errors.Annotatef(err, "Invalid path %s", authorizePath)
	}
	return putItem(root, authorizePath, acc)
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
			if data, _ := r.loadAuthorizeFromPath(filepath.Join(authorizeBucket, access.Authorize)); data != nil {
				result.AuthorizeData = data
			}
		}
		if access.Previous != "" {
			if data, _ := r.loadAccessFromPath(filepath.Join(accessBucket, access.Previous)); data != nil {
				result.AccessData = data
			}
		}
		if access.Client != "" {
			if data, _ := r.loadClientFromPath(r.oauthClientPath(clientsBucket, access.Client)); data != nil {
				result.Client = data
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

	return r.loadAccessFromPath(filepath.Join(accessBucket, code))
}

// RemoveAccess revokes or deletes an AccessData.
func (r *repo) RemoveAccess(code string) error {
	return r.root.RemoveAll(filepath.Join(accessBucket, code))
}

// LoadRefresh retrieves refresh AccessData. Client information MUST be loaded together.
func (r *repo) LoadRefresh(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty refresh code")
	}

	refresh := ref{}
	_, err := r.loadFromOauthPath(filepath.Join(refreshBucket, code), func(raw []byte) error {
		if err := decodeFn(raw, &refresh); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal refresh object")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.loadAccessFromPath(filepath.Join(accessBucket, refresh.Access))
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(code string) error {
	return r.root.RemoveAll(filepath.Join(refreshBucket, code))
}
