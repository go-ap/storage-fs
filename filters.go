package fs

import (
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/filters"
)

type CollectionFilter interface {
	Run(vocab.CollectionInterface) vocab.Item
}

type ItemFilter interface {
	Run(vocab.Item) vocab.Item
}

type inCollectionCheck vocab.IRI

func (i inCollectionCheck) Apply(item vocab.Item) bool {
	iri := vocab.IRI(i)
	_, col := vocab.Split(iri)
	if col == vocab.Unknown {
		return false
	}
	return item.GetLink().Contains(iri, true)
}

func InCollection(iri vocab.IRI) filters.Check {
	return inCollectionCheck(iri)
}

func FiltersFromIRI(i vocab.IRI) (vocab.IRI, filters.Checks, error) {
	f, err := filters.FromIRI(i)

	if u, err := i.URL(); err == nil && len(u.RawQuery) > 0 {
		u.RawQuery = ""
		i = vocab.IRI(u.String())
	}

	return i, f, err
}
