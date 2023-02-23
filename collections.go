package fs

import (
	"strings"

	vocab "github.com/go-ap/activitypub"
)

const (
	// actorsType is a constant that represents the URL path for the local actors collection.
	// It is used as the parent for all To IDs
	actorsType = vocab.CollectionPath("actors")
	// activitiesType is a constant that represents the URL path for the local activities collection
	// It is used as the parent for all Activity IDs
	activitiesType = vocab.CollectionPath("activities")
	// objectsType is a constant that represents the URL path for the local objects collection
	// It is used as the parent for all non To, non Activity Object IDs
	objectsType = vocab.CollectionPath("objects")

	// blockedType is an internally used collection, to store a list of actors the actor has blocked
	blockedType = vocab.CollectionPath("blocked")

	// ignoredType is an internally used collection, to store a list of actors the actor has ignored
	ignoredType = vocab.CollectionPath("ignored")
)

var (
	fedBOXCollections = vocab.CollectionPaths{
		activitiesType,
		actorsType,
		objectsType,
		blockedType,
		ignoredType,
	}

	validActivityCollection = vocab.CollectionPaths{
		activitiesType,
	}

	validObjectCollection = vocab.CollectionPaths{
		actorsType,
		objectsType,
	}
)

func getValidActivityCollection(typ vocab.CollectionPath) vocab.CollectionPath {
	for _, t := range validActivityCollection {
		if strings.ToLower(string(typ)) == string(t) {
			return t
		}
	}
	return vocab.Unknown
}

func getValidObjectCollection(typ vocab.CollectionPath) vocab.CollectionPath {
	for _, t := range validObjectCollection {
		if strings.ToLower(string(typ)) == string(t) {
			return t
		}
	}
	return vocab.Unknown
}

// ValidCollection shows if the current ActivityPub end-point type is a valid collection
func ValidCollection(typ vocab.CollectionPath) bool {
	return ValidActivityCollection(typ) || ValidObjectCollection(typ)
}

// ValidActivityCollection shows if the current ActivityPub end-point type is a valid collection for handling Activities
func ValidActivityCollection(typ vocab.CollectionPath) bool {
	return getValidActivityCollection(typ) != vocab.Unknown || vocab.ValidActivityCollection(typ)
}

// ValidObjectCollection shows if the current ActivityPub end-point type is a valid collection for handling Objects
func ValidObjectCollection(typ vocab.CollectionPath) bool {
	return getValidObjectCollection(typ) != vocab.Unknown || vocab.ValidObjectCollection(typ)
}
