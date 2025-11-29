package fs

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"golang.org/x/crypto/bcrypt"
)

// PasswordSet
func (r *repo) PasswordSet(iri vocab.IRI, pw []byte) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if pw == nil {
		return errors.Newf("could not generate hash for nil pw")
	}
	if len(iri) == 0 {
		return errors.NotFoundf("not found")
	}
	m := Metadata{}
	_ = r.LoadMetadata(iri, &m)
	var err error
	if pw, err = bcrypt.GenerateFromPassword(pw, -1); err != nil {
		return errors.Annotatef(err, "could not generate pw hash")
	}
	m.Pw = pw
	return r.SaveMetadata(iri, m)
}

// PasswordCheck
func (r *repo) PasswordCheck(iri vocab.IRI, pw []byte) error {
	m := new(Metadata)

	if err := r.LoadMetadata(iri, m); err != nil {
		return errors.Annotatef(err, "Could not find load metadata for %s", iri)
	}

	if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
		return errors.NewUnauthorized(err, "Invalid pw")
	}
	return nil
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	p := iriPath(iri)
	raw, err := loadRaw(r.root, getMetadataKey(p))
	if err != nil {
		err = errors.NewNotFound(err, "Could not find metadata in path %s", p)
		return err
	}
	if err = decodeFn(raw, m); err != nil {
		return errors.Annotatef(err, "Could not unmarshal metadata")
	}
	return nil
}

// SaveMetadata
func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if m == nil {
		return errors.Newf("Could not save nil metadata")
	}
	entryBytes, err := encodeFn(m)
	if err != nil {
		return errors.Annotatef(err, "Could not marshal metadata")
	}

	basePath := iriPath(iri)
	if err := putRaw(r.root, getMetadataKey(basePath), entryBytes); err != nil {
		return err
	}
	return nil
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	m := new(Metadata)

	if err := r.LoadMetadata(iri, m); err != nil {
		return nil, err
	}

	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

// Metadata is the basic metadata for storing information about an actor.
// It holds the actor's password and private key, the former being necessary for cross server HTTP signatures.
type Metadata struct {
	Pw         []byte `jsonld:"pw,omitempty"`
	PrivateKey []byte `jsonld:"key,omitempty"`
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (*vocab.PublicKey, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.logger.Errorf("unable to marshal the private key %T for %s", key, iri)
		return nil, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(iri, m); err != nil {
		r.logger.Errorf("unable to save the private key %T for %s", key, iri)
		return nil, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.logger.Errorf("received key %T does not match any of the known private key types", key)
		return nil, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.logger.Errorf("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return nil, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	return &vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}, nil
}
