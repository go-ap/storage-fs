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
	"math/rand"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"golang.org/x/crypto/bcrypt"
)

// PasswordSet
func (r *repo) PasswordSet(iri vocab.IRI, pw []byte) error {
	pw, err := bcrypt.GenerateFromPassword(pw, -1)
	if err != nil {
		return errors.Annotatef(err, "could not generate pw hash")
	}
	m := Metadata{
		Pw: pw,
	}
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
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	if m.PrivateKey != nil {
		r.logger.Debugf("actor %s already has a private key", iri)
	}

	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.logger.Errorf("unable to x509.MarshalPKCS8PrivateKey() the private key %T for %s", key, iri)
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

// GenKey creates and saves a private key for an actor found by its IRI
func (r *repo) GenKey(iri vocab.IRI) error {
	ob, err := r.loadOneFromIRI(iri)
	if err != nil {
		return err
	}
	if ob.GetType() != vocab.PersonType {
		return errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", ob.GetType())
	}

	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return err
	}
	if m.PrivateKey != nil {
		return nil
	}
	// TODO(marius): this needs a way to choose between ED25519 and RSA keys
	pubB, prvB := generateECKeyPair()
	m.PrivateKey = pem.EncodeToMemory(&prvB)

	if err = r.SaveMetadata(iri, m); err != nil {
		return err
	}
	_ = vocab.OnActor(ob, func(act *vocab.Actor) error {
		act.PublicKey = vocab.PublicKey{
			ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
			Owner:        iri,
			PublicKeyPem: string(pem.EncodeToMemory(&pubB)),
		}
		return nil
	})
	return nil
}

func generateECKeyPair() (pem.Block, pem.Block) {
	// TODO(marius): make this actually produce proper keys, using a valid seed
	keyPub, keyPrv, _ := ed25519.GenerateKey(rand.New(rand.NewSource(6667)))

	var p, r pem.Block
	if pubEnc, err := x509.MarshalPKIXPublicKey(keyPub); err == nil {
		p = pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: pubEnc,
		}
	}
	if prvEnc, err := x509.MarshalPKCS8PrivateKey(keyPrv); err == nil {
		r = pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: prvEnc,
		}
	}
	return p, r
}
