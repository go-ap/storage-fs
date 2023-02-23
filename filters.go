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

func FiltersFromIRI(i vocab.IRI) (filters.Fns, error) {
	return filters.FromIRI(i)
}
