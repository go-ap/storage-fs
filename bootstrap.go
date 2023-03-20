package fs

import (
	"net/url"
	"os"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/storage-fs/internal/cache"
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

	if err := vocab.OnActor(self, r.CreateService); err != nil {
		return err
	}

	mkCollection := func(iri vocab.IRI) vocab.CollectionInterface {
		return &vocab.OrderedCollection{
			ID:           iri,
			Type:         vocab.OrderedCollectionType,
			Published:    time.Now().UTC(),
			AttributedTo: self,
			CC:           vocab.ItemCollection{vocab.PublicNS},
		}
	}
	return vocab.OnActor(self, func(service *vocab.Actor) error {
		for _, stream := range service.Streams {
			if _, err := r.Create(mkCollection(stream.GetID())); err != nil {
				r.errFn("Unable to create %s collection for actor %s", stream.GetID(), service.GetLink())
			}
		}
		return nil
	})
}

func (r *repo) Reset() {
	r.cache = cache.New(true)
}
