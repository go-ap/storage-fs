package fs

import (
	"os"

	vocab "github.com/go-ap/activitypub"
	ap "github.com/go-ap/fedbox/activitypub"
	"github.com/go-ap/storage-fs/internal/cache"
)

func Clean(conf Config) error {
	return os.RemoveAll(conf.Path)
}

func Bootstrap(conf Config) error {
	r, err := New(conf)
	if err != nil {
		return err
	}
	err = r.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	self := ap.Self(ap.DefaultServiceIRI(conf.URL))
	actors := &vocab.OrderedCollection{ID: ap.ActorsType.IRI(self)}
	if _, err = r.Create(actors); err != nil {
		return err
	}
	activities := &vocab.OrderedCollection{ID: ap.ActivitiesType.IRI(self)}
	if _, err = r.Create(activities); err != nil {
		return err
	}
	objects := &vocab.OrderedCollection{ID: ap.ObjectsType.IRI(self)}
	if _, err = r.Create(objects); err != nil {
		return err
	}
	return nil
}

func (r *repo) Reset() {
	r.cache = cache.New(true)
}
