// +build storage_badger

package app

import "github.com/go-ap/fedbox/internal/config"

func getBadgerStorage(c config.Options, l logrus.FieldLogger) (st.Repository, osin.Storage, error) {
	db := badger.New(badger.Config{
		Path:  c.Badger(),
		LogFn: InfoLogFn(l),
		ErrFn: ErrLogFn(l),
	}, c.BaseURL)
	oauth := auth.NewBoltDBStore(auth.BoltConfig{
		Path:       c.BoltDBOAuth2(),
		BucketName: c.Host,
		LogFn:      InfoLogFn(l),
		ErrFn:      ErrLogFn(l),
	})
	return db, oauth, nil
}

func Storage(c config.Options, l logrus.FieldLogger) (st.Repository, osin.Storage, error) {
	return getBadgerStorage(c, l)
}
