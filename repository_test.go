package fs

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"git.sr.ht/~mariusor/lw"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"golang.org/x/sys/unix"
)

type fields struct {
	path  string
	cache cache.CanStore
}

func Test_New(t *testing.T) {
	testFolder := t.TempDir()

	tests := []struct {
		name    string
		config  Config
		want    fields
		wantErr error
	}{
		{
			name:    "empty",
			config:  Config{},
			wantErr: errMissingPath,
		},
		{
			name:   "valid temp folder",
			config: Config{Path: testFolder},
			want: fields{
				path: testFolder,
			},
		},
		{
			name:    "invalid permissions",
			config:  Config{Path: "/root/tmp"},
			wantErr: os.ErrPermission,
		},
		{
			name:    "invalid relative file",
			config:  Config{Path: "./not-sure-if-this-should-work-or-not"},
			wantErr: error(unix.ENOENT),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.config)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("New() returned different error than expected = %v, want %v", err, tt.wantErr)
			}
			if got == nil {
				return
			}
			if got.path != tt.want.path {
				t.Errorf("New().path = %v, want %v", got.root, tt.want.path)
			}
		})
	}
}

var logger = lw.Dev()

func Test_repo_Open(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: error(unix.ENOENT),
		},
		{
			name:    "empty",
			fields:  fields{},
			wantErr: error(unix.ENOENT),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.fields.path,
				cache:  tt.fields.cache,
				logger: logger,
			}
			if err := r.Open(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
			}
			if r.path != tt.fields.path {
				t.Errorf("Open() path is not correct = %s, want %s", r.root.Name(), tt.fields.path)
			}
			defer r.Close()
		})
	}
}

func filter(items vocab.ItemCollection, fil ...filters.Check) vocab.ItemCollection {
	result, _ := vocab.ToItemCollection(filters.Checks(fil).Run(items))
	return *result
}

func Test_repo_Load(t *testing.T) {
	basePath, _ := getwd()

	mocksPath := filepath.Join(basePath, "mocks")
	mocks := make(map[vocab.IRI]vocab.Item)
	inbox := make(vocab.ItemCollection, 0, 80)
	_ = filepath.WalkDir(mocksPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(path) == objectKey {
			u := strings.TrimSuffix(strings.Replace(path, mocksPath, "https:/", 1), "/"+objectKey)
			j, _ := os.ReadFile(path)
			m, _ := vocab.UnmarshalJSON(j)
			mocks[vocab.IRI(u)] = m
			if strings.HasSuffix(filepath.Dir(u), "inbox") {
				inbox = append(inbox, m)
			}
		}
		return nil
	})

	sort.Slice(inbox, func(i, j int) bool {
		return vocab.ItemOrderTimestamp(inbox[i], inbox[j])
	})
	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name    string
		args    args
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name:    "empty iri gives us not found",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name: "root iri gives us the root",
			args: args{iri: "https://example.com"},
			want: vocab.Actor{Type: vocab.ApplicationType, ID: "https://example.com"},
		},
		{
			name:    "invalid iri gives 404",
			args:    args{iri: "https://example.com/dsad"},
			want:    nil,
			wantErr: os.ErrNotExist,
		},
		{
			name: "full inbox",
			args: args{iri: "https://example.com/inbox"},
			want: vocab.OrderedCollection{
				ID:           "https://example.com/inbox",
				Type:         vocab.OrderedCollectionType,
				OrderedItems: inbox,
				TotalItems:   inbox.Count(),
			},
		},
		{
			name: "inbox::0",
			args: args{iri: "https://example.com/inbox/0"},
			want: filter(inbox, filters.SameID("https://example.com/inbox/0"))[0],
		},
		{
			name: "inbox::99",
			args: args{iri: "https://example.com/inbox/99"},
			want: filter(inbox, filters.SameID("https://example.com/inbox/99"))[0],
		},
		{
			name: "inbox?type=Create",
			args: args{
				iri: "https://example.com/inbox",
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
				},
			},
			want: vocab.OrderedCollection{
				ID:    "https://example.com/inbox",
				Type:  vocab.OrderedCollectionType,
				First: vocab.IRI("https://example.com/inbox?maxItems=100"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Activity{
						ID:   "https://example.com/inbox/2",
						Type: vocab.CreateType,
						Actor: &vocab.Actor{
							ID:                "https://example.com/Ross",
							Type:              vocab.PersonType,
							PreferredUsername: vocab.DefaultNaturalLanguageValue("Ross"),
						},
						Object: &vocab.Object{
							ID:      "https://example.com/inbox/1",
							Type:    vocab.ArticleType,
							Name:    vocab.DefaultNaturalLanguageValue("Donec quis tempus eros, ut bibendum nibh."),
							Content: vocab.DefaultNaturalLanguageValue("Suspendisse blandit tempor faucibus.\nVestibulum eleifend eros metus, eget congue mauris molestie ut.\nNam ut odio id risus laoreet scelerisque.\n"),
						},
					},
					&vocab.Activity{
						ID:   "https://example.com/inbox/93",
						Type: vocab.CreateType,
						Actor: &vocab.Actor{
							ID:                "https://example.com/Hank",
							Type:              vocab.ServiceType,
							PreferredUsername: vocab.DefaultNaturalLanguageValue("Hank"),
						},
						Object: &vocab.Object{
							ID:      "https://example.com/inbox/93/object",
							Name:    vocab.DefaultNaturalLanguageValue("Cras pharetra libero."),
							Content: vocab.DefaultNaturalLanguageValue("Vivamus eget maximus quam, non dignissim sapien.\nDonec finibus sem vitae nisi ultricies dictum.\nCras pharetra libero.\nPhasellus sit amet aliquam quam.\nIn at vulputate est.\nIn porttitor augue ac dolor viverra, eget fringilla augue tincidunt.\nDonec accumsan pulvinar risus, eu ultrices est volutpat lobortis.\nCurabitur tincidunt mattis ornare.\nUt lacinia ligula a bibendum pulvinar.\nIn velit libero, ultrices nec quam at, lacinia congue purus.\nAliquam gravida gravida urna ac ornare.\nQuisque ac dolor tellus.\nSuspendisse blandit tempor faucibus.\nUt lacinia ligula a bibendum pulvinar.\nDonec accumsan pulvinar risus, eu ultrices est volutpat lobortis.\nFusce sit amet eros in lacus porta vehicula.\nNulla facilisi.\\nNulla facilisi.\nIn porttitor augue ac dolor viverra, eget fringilla augue tincidunt.\nNullam turpis turpis, malesuada non accumsan vitae, congue ac justo.\nQuisque id mi aliquet, pellentesque diam eu, euismod nisl.\nSuspendisse potenti.\nIn velit libero, ultrices nec quam at, lacinia congue purus.\n"),
							Type:    vocab.DocumentType,
						},
					},
				},
				TotalItems: inbox.Count(),
			},
		},
		{
			name: "inbox?type=Create&actor.name=Hank",
			args: args{
				iri: "https://example.com/inbox",
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Actor(filters.NameIs("Hank")),
				},
			},
			want: vocab.OrderedCollection{
				ID:    "https://example.com/inbox",
				Type:  vocab.OrderedCollectionType,
				First: vocab.IRI("https://example.com/inbox?maxItems=100"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Activity{
						ID:   "https://example.com/inbox/93",
						Type: vocab.CreateType,
						Actor: &vocab.Actor{
							ID:                "https://example.com/Hank",
							Type:              vocab.ServiceType,
							PreferredUsername: vocab.DefaultNaturalLanguageValue("Hank"),
						},
						Object: &vocab.Object{
							ID:      "https://example.com/inbox/93/object",
							Name:    vocab.DefaultNaturalLanguageValue("Cras pharetra libero."),
							Content: vocab.DefaultNaturalLanguageValue("Vivamus eget maximus quam, non dignissim sapien.\nDonec finibus sem vitae nisi ultricies dictum.\nCras pharetra libero.\nPhasellus sit amet aliquam quam.\nIn at vulputate est.\nIn porttitor augue ac dolor viverra, eget fringilla augue tincidunt.\nDonec accumsan pulvinar risus, eu ultrices est volutpat lobortis.\nCurabitur tincidunt mattis ornare.\nUt lacinia ligula a bibendum pulvinar.\nIn velit libero, ultrices nec quam at, lacinia congue purus.\nAliquam gravida gravida urna ac ornare.\nQuisque ac dolor tellus.\nSuspendisse blandit tempor faucibus.\nUt lacinia ligula a bibendum pulvinar.\nDonec accumsan pulvinar risus, eu ultrices est volutpat lobortis.\nFusce sit amet eros in lacus porta vehicula.\nNulla facilisi.\\nNulla facilisi.\nIn porttitor augue ac dolor viverra, eget fringilla augue tincidunt.\nNullam turpis turpis, malesuada non accumsan vitae, congue ac justo.\nQuisque id mi aliquet, pellentesque diam eu, euismod nisl.\nSuspendisse potenti.\nIn velit libero, ultrices nec quam at, lacinia congue purus.\n"),
							Type:    vocab.DocumentType,
						},
					},
				},
				TotalItems: inbox.Count(),
			},
		},
		{
			name: "inbox?type=Article",
			args: args{
				iri: "https://example.com/inbox",
				fil: filters.Checks{
					filters.HasType(vocab.ArticleType),
				},
			},
			want: vocab.OrderedCollection{
				ID:    "https://example.com/inbox",
				Type:  vocab.OrderedCollectionType,
				First: vocab.IRI("https://example.com/inbox?maxItems=100"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Object{
						ID:      "https://example.com/inbox/1",
						Type:    vocab.ArticleType,
						Name:    vocab.DefaultNaturalLanguageValue("Donec quis tempus eros, ut bibendum nibh."),
						Content: vocab.DefaultNaturalLanguageValue("Suspendisse blandit tempor faucibus.\nVestibulum eleifend eros metus, eget congue mauris molestie ut.\nNam ut odio id risus laoreet scelerisque.\n"),
					},
					&vocab.Object{
						ID:      "https://example.com/inbox/11",
						Type:    vocab.ArticleType,
						Name:    vocab.DefaultNaturalLanguageValue("In velit libero, ultrices nec quam at, lacinia congue purus."),
						Content: vocab.DefaultNaturalLanguageValue("Sed est elit, facilisis eu malesuada non, mattis nec risus.\nUt lacinia ligula a bibendum pulvinar.\nAliquam gravida gravida urna ac ornare.\nIn porttitor augue ac dolor viverra, eget fringilla augue tincidunt.\nCras pharetra libero.\nDonec quis tempus eros, ut bibendum nibh.\nMaecenas dapibus, mi quis elementum imperdiet, ipsum dolor molestie est, sit amet finibus nisi nunc et orci.\nVivamus eget maximus quam, non dignissim sapien.\nCurabitur tincidunt mattis ornare.\nSuspendisse blandit tempor faucibus.\nNulla semper aliquet tincidunt.\n"),
					},
					&vocab.Object{
						ID:      "https://example.com/inbox/74",
						Type:    vocab.ArticleType,
						Name:    vocab.DefaultNaturalLanguageValue("Cras pulvinar gravida purus, id tincidunt sem vestibulum vel."),
						Content: vocab.DefaultNaturalLanguageValue("Nam ut odio id risus laoreet scelerisque.\n"),
					},
				},
				TotalItems: inbox.Count(),
			},
		},
		{
			name: "inbox::2",
			args: args{iri: "https://example.com/inbox/2"},
			want: vocab.Activity{
				ID:   "https://example.com/inbox/2",
				Type: vocab.CreateType,
				Actor: &vocab.Actor{
					ID:                "https://example.com/Ross",
					Type:              vocab.PersonType,
					PreferredUsername: vocab.NaturalLanguageValuesNew(vocab.DefaultLangRef("Ross")),
				},
				Object: filter(inbox)[1],
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{path: mocksPath}
			_ = r.Open()
			defer r.Close()

			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !vocab.ItemsEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func expectedCol(id vocab.IRI) *vocab.OrderedCollection {
	return &vocab.OrderedCollection{
		ID:           id,
		Type:         vocab.OrderedCollectionType,
		OrderedItems: make(vocab.ItemCollection, 0),
	}
}

func Test_repo_createCollection(t *testing.T) {
	tests := []struct {
		name     string
		owner    vocab.Item
		iri      vocab.IRI
		expected vocab.CollectionInterface
		wantErr  bool
	}{
		{
			name:     "example.com/replies",
			owner:    &vocab.Actor{ID: "https://example.com"},
			iri:      "https://example.com/replies",
			expected: expectedCol("https://example.com/replies"),
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   t.TempDir(),
				cache:  cache.New(false),
				logger: logger,
			}
			_ = r.Open()
			defer r.Close()

			col, err := createCollectionInPath(r, tt.iri, tt.owner)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !vocab.ItemsEqual(col, tt.expected.GetLink()) {
				t.Errorf("Returned collection is not equal to expected %v: %v", tt.expected, col)
			}
			saved, err := r.Load(tt.iri)
			if err != nil {
				t.Errorf("Unable to load collection at IRI %q: %s", tt.iri, err)
			}
			if !vocab.ItemsEqual(saved, tt.expected) {
				t.Errorf("Saved collection is not equal to expected %v: %v", tt.expected, saved)
			}
		})
	}
}

func errPathNotFound(path string) error {
	return &fs.PathError{Op: "openat", Path: path, Err: unix.ENOENT}
}

func defaultCol(iri vocab.IRI) vocab.CollectionInterface {
	return &vocab.OrderedCollection{
		ID:        iri,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
}

func Test_repo_RemoveFrom(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name    string
		path    string
		setup   func(*repo) error
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errors.NotFoundf("not found"), // empty iri can't be found, unsure if that makes sense
		},
		{
			name: "collection doesn't exist",
			path: t.TempDir(),
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errPathNotFound("example.com/followers"),
		},
		{
			name: "item doesn't exist in collection",
			path: t.TempDir(),
			setup: func(r *repo) error {
				_, err := saveCollection(r, defaultCol("https://example.com/followers"))
				return err
			},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil, // if the item doesn't exist, we don't error out, unsure if that makes sense
		},
		{
			name: "item exists in collection",
			path: t.TempDir(),
			setup: func(r *repo) error {
				col := vocab.OrderedCollection{
					ID:        "https://example.com/followers",
					Type:      vocab.OrderedCollectionType,
					CC:        vocab.ItemCollection{vocab.PublicNS},
					Published: time.Now().UTC(),
				}
				if _, err := saveCollection(r, &col); err != nil {
					return err
				}
				ob, err := save(r, vocab.Object{ID: "https://example.com"})
				if err != nil {
					return err
				}
				return r.AddTo(col.ID, ob)
			},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.path,
				logger: logger,
				cache:  cache.New(true),
			}
			if err := r.Open(); err != nil {
				t.Errorf("Open before RemoveFrom() error = %v", err)
				return
			}
			if tt.setup != nil {
				if err := tt.setup(r); err != nil {
					t.Errorf("Setup before RemoveFrom() error = %v", err)
					return
				}
			}
			err := r.RemoveFrom(tt.args.colIRI, tt.args.it)
			if (err != nil) && tt.wantErr.Error() != err.Error() {
				t.Errorf("RemoveFrom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr != nil {
				// NOTE(marius): if we expected an error we don't need to following tests
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after RemoveFrom() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after RemoveFrom(), didn't return a CollectionInterface type")
				return
			}

			if col.Contains(tt.args.it) {
				t.Errorf("Load() after RemoveFrom(), the item is still in collection %#v", col.Collection())
			}

			// NOTE(marius): this is a bit of a hackish way to skip testing of the object when we didn't
			// save it to the disk
			if vocab.IsObject(tt.args.it) {
				ob, err := r.Load(tt.args.it.GetLink())
				if err != nil {
					t.Errorf("Load() of the object after RemoveFrom() error = %v", err)
					return
				}
				if !vocab.ItemsEqual(ob, tt.args.it) {
					t.Errorf("Loaded item after RemoveFrom(), is not equal %#v with the one provided %#v", ob, tt.args.it)
				}
			}
		})
	}
}

func Test_repo_AddTo(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name    string
		path    string
		setup   func(*repo) error
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errors.NotFoundf("not found"), // empty iri can't be found, unsure if that makes sense
		},
		{
			name: "collection doesn't exist",
			path: t.TempDir(),
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errPathNotFound("example.com/followers"),
		},
		{
			name: "item doesn't exist",
			path: t.TempDir(),
			setup: func(r *repo) error {
				_, err := saveCollection(r, defaultCol("https://example.com/followers"))
				return err
			},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "item exists in collection",
			path: t.TempDir(),
			setup: func(r *repo) error {
				col := defaultCol("https://example.com/followers")
				if _, err := saveCollection(r, col); err != nil {
					return err
				}
				ob, err := save(r, vocab.Object{ID: "https://example.com"})
				if err != nil {
					return err
				}
				return r.AddTo(col.GetLink(), ob)
			},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.path,
				logger: logger,
				cache:  cache.New(true),
			}
			if err := r.Open(); err != nil {
				t.Errorf("Open before AddTo() error = %v", err)
				return
			}
			if tt.setup != nil {
				if err := tt.setup(r); err != nil {
					t.Errorf("Setup before AddTo() error = %v", err)
					return
				}
			}
			err := r.AddTo(tt.args.colIRI, tt.args.it)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after AddTo() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after AddTo(), didn't return a CollectionInterface type")
				return
			}

			if !col.Contains(tt.args.it) {
				t.Errorf("Load() after AddTo(), the item is not in collection %#v", col.Collection())
			}

			ob, err := r.Load(tt.args.it.GetLink())
			if err != nil {
				t.Errorf("Load() of the object after AddTo() error = %v", err)
				return
			}
			if !vocab.ItemsEqual(ob, tt.args.it) {
				t.Errorf("Loaded item after AddTo(), is not equal %#v with the one provided %#v", ob, tt.args.it)
			}
		})
	}
}
