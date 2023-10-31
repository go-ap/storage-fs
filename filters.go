package fs

import (
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/filters"
)

func InCollection(iri vocab.IRI) filters.Fn {
	_, col := vocab.Split(iri)
	return func(item vocab.Item) bool {
		if col == vocab.Unknown {
			return false
		}
		return item.GetLink().Contains(iri, true)
	}
}

func FiltersFromIRI(i vocab.IRI) (vocab.IRI, filters.Fns, error) {
	f, err := filters.FromIRI(i)

	if u, err := i.URL(); err == nil && len(u.RawQuery) > 0 {
		u.RawQuery = ""
		i = vocab.IRI(u.String())
	}

	return i, f, err
}
