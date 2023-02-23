package fs

import (
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/storage-fs/internal/cache"
	"net/url"
	"os"
)

func Clean(conf Config) error {
	return os.RemoveAll(conf.Path)
}

func defaultServiceIRI(baseURL string) vocab.IRI {
	u, _ := url.Parse(baseURL)
	// TODO(marius): I don't like adding the / folder to something like http://fedbox.git
	if u.Path == "" {
		u.Path = "/"
	}
	return vocab.IRI(u.String())
}

func Bootstrap(conf Config, self vocab.Item) error {
	r, err := New(conf)
	if err != nil {
		return err
	}
	err = r.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	return nil
}

func (r *repo) Reset() {
	r.cache = cache.New(true)
}
