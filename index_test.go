package fs

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"git.sr.ht/~mariusor/lw"
	"github.com/RoaringBitmap/roaring/roaring64"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/filters/index"
)

func Test_getIndexKey(t *testing.T) {
	var unknown = index.Type(-1)
	tests := []struct {
		name string
		arg  index.Type
		want string
	}{
		{
			name: "unknown",
			arg:  unknown,
			want: "",
		},
		{
			name: "empty = all",
			want: ".all.gob",
		},
		{
			name: "byID",
			arg:  index.ByID,
			want: ".all.gob",
		},
		{
			name: "byType",
			arg:  index.ByType,
			want: ".type.gob",
		},
		{
			name: "byName",
			arg:  index.ByName,
			want: ".name.gob",
		},
		{
			name: "byPreferredUsername",
			arg:  index.ByPreferredUsername,
			want: ".preferredUsername.gob",
		},
		{
			name: "bySummary",
			arg:  index.BySummary,
			want: ".summary.gob",
		},
		{
			name: "byContent",
			arg:  index.ByContent,
			want: ".content.gob",
		},
		{
			name: "byActor",
			arg:  index.ByActor,
			want: ".actor.gob",
		},
		{
			name: "byObject",
			arg:  index.ByObject,
			want: ".object.gob",
		},
		{
			name: "byRecipients",
			arg:  index.ByRecipients,
			want: ".recipients.gob",
		},
		{
			name: "byAttributedTo",
			arg:  index.ByAttributedTo,
			want: ".attributedTo.gob",
		},
		{
			name: "byInReplyTo",
			arg:  index.ByInReplyTo,
			want: "",
		},
		{
			name: "byPublished",
			arg:  index.ByPublished,
			want: "",
		},
		{
			name: "byUpdated",
			arg:  index.ByUpdated,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getIndexKey(tt.arg); got != tt.want {
				t.Errorf("getIndexKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func saveIndexForRepo(r *repo) *repo {
	if err := r.saveIndex(); err != nil {
		r.logger.WithContext(lw.Ctx{"path": r.path, "err": err.Error()}).Errorf("unable to save indexes for mock repo")
	}
	return r
}

func Test_repo_loadIndex(t *testing.T) {
	mockRoot := openRoot(t, t.TempDir())
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "empty",
		},
		{
			name: "unopened indexes",
			fields: fields{
				path: t.TempDir(),
				root: mockFilesToIndex(t, openRoot(t, t.TempDir())),
			},
		},
		{
			name: "with indexes",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.fields.path,
				root:   tt.fields.root,
				index:  tt.fields.index,
				cache:  tt.fields.cache,
				logger: tt.fields.logger,
			}
			if err := r.loadIndex(); (err != nil) != tt.wantErr {
				t.Errorf("loadIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_newBitmap(t *testing.T) {
	tests := []struct {
		name string
		args []index.Type
	}{
		{
			name: "empty - all indexes",
			args: nil,
		},
		{
			name: "all indexes",
			args: allIndexTypes,
		},
		{
			name: "generic indexes",
			args: genericIndexTypes,
		},
		{
			name: "ID",
			args: []index.Type{index.ByID},
		},
		{
			name: "type",
			args: []index.Type{index.ByType},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newBitmap(tt.args...)
			if got.ref == nil {
				t.Errorf("newBitmap() index ref invalid %v", got.ref)
			}
			if got.all == nil {
				t.Errorf("newBitmap() index map invalid %v", got.all)
			}
			if len(tt.args) == 0 {
				tt.args = allIndexTypes
			}
			if len(got.all) != len(tt.args) {
				t.Errorf("newBitmap() index ref count is invalid, got %d expected %d", len(got.ref), len(tt.args))
			}
			for _, typ := range tt.args {
				if _, ok := got.all[typ]; !ok {
					t.Errorf("newBitmap() unable to fetch index of type %d, in ref %v", typ, got.ref)
				}
			}
		})
	}
}

func Test_onCollectionBitmap(t *testing.T) {
	var logHash = func(t *testing.T, ) func(bitmap *roaring64.Bitmap, u uint64) {
		return func(bitmap *roaring64.Bitmap, u uint64) {
			t.Logf("%d - %v", u, bitmap)
		}
	}
	type args struct {
		bmp *roaring64.Bitmap
		it  vocab.Item
		fn  func(*testing.T) func(*roaring64.Bitmap, uint64)
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			wantErr: indexDisabled,
		},
		{
			name: "nil item, nil func",
			args: args{bmp: roaring64.New()},
		},
		{
			name: "valid item, nil func",
			args: args{bmp: roaring64.New(), it: vocab.IRI("https://example.com")},
		},
		{
			name: "call logHash",
			args: args{
				bmp: roaring64.New(),
				it:  &vocab.Object{ID: "https://example.com"},
				fn:  logHash,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fn func(*roaring64.Bitmap, uint64)
			if tt.args.fn != nil {
				fn = tt.args.fn(t)
			}

			if err := onCollectionBitmap(tt.args.bmp, tt.args.it, fn); !errors.Is(err, tt.wantErr) {
				t.Errorf("onCollectionBitmap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func openRoot(t *testing.T, path string) *os.Root {
	rr, err := os.OpenRoot(path)
	if err != nil {
		t.Fatalf("Unable to open mock root: %s", err)
	}
	return rr
}

var mockItems = vocab.ItemCollection{
	vocab.IRI("https://example.com/plain-iri"),
	&vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
	&vocab.Link{ID: "https://example.com/1", Href: "https://example.com/1", Type: vocab.LinkType},
	&vocab.Actor{ID: "https://example.com/~jdoe", Type: vocab.PersonType},
	&vocab.Activity{ID: "https://example.com/~jdoe/1", Type: vocab.UpdateType},
	&vocab.Object{ID: "https://example.com/~jdoe/tag-none", Type: vocab.UpdateType},
	&vocab.Question{ID: "https://example.com/~jdoe/2", Type: vocab.QuestionType},
	&vocab.IntransitiveActivity{ID: "https://example.com/~jdoe/3", Type: vocab.ArriveType},
}

func mockFilesToIndex(t *testing.T, root *os.Root) *os.Root {
	for _, mockIt := range mockItems {
		raw, _ := encodeFn(mockIt)
		mockPath := iriPath(mockIt.GetLink())

		if err := putRaw(root, getObjectKey(mockPath), raw); err != nil {
			t.Fatalf("Unable to save mock item %s: %s", mockIt.GetLink(), err)
		}
	}
	return root
}

func mockIndex(t *testing.T, root *os.Root) *bitmaps {
	bmp := newBitmap()
	if err := saveIndex(root, bmp, _indexDirName); err != nil {
		t.Fatalf("Unable to save mock root %s indexes: %s", root.Name(), err)
	}
	return bmp
}

func Test_repo_Reindex(t *testing.T) {
	mockRoot := openRoot(t, t.TempDir())
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name: "with empty root folder",
			fields: fields{
				path: t.TempDir(),
				root: openRoot(t, t.TempDir()),
			},
		},
		{
			name: "with empty bitmap and files to index",
			fields: fields{
				path: t.TempDir(),
				root: mockFilesToIndex(t, openRoot(t, t.TempDir())),
			},
		},
		{
			name: "with bitmap and files to index",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.fields.path,
				root:   tt.fields.root,
				index:  tt.fields.index,
				cache:  tt.fields.cache,
				logger: tt.fields.logger,
			}
			if err := r.Reindex(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Reindex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_addToIndex(t *testing.T) {
	mockRoot := openRoot(t, t.TempDir())
	type fields struct {
		path   string
		root   *os.Root
		index  *bitmaps
		cache  cache.CanStore
		logger lw.Logger
	}
	type args struct {
		it   vocab.Item
		path string
	}
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			wantErr: indexDisabled,
		},
		{
			name: "with empty root folder",
			fields: fields{
				path: t.TempDir(),
				root: openRoot(t, t.TempDir()),
			},
			wantErr: indexDisabled,
		},
		{
			name: "with empty bitmap and files to index",
			fields: fields{
				path: t.TempDir(),
				root: mockFilesToIndex(t, openRoot(t, t.TempDir())),
			},
			wantErr: indexDisabled,
		},
		{
			name: "add IRI to index",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   vocab.IRI("https://example.com/1/2/3"),
				path: "1-2-3",
			},
		},
		{
			name: "add Object to index",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   &vocab.Object{ID: "https://example.com/1/2/3", Type: vocab.NoteType},
				path: "1-2-3",
			},
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name: fmt.Sprintf("add %d %T to index", i, mockIt),
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   mockIt,
				path: iriPath(mockIt.GetLink()),
			},
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:   tt.fields.path,
				root:   tt.fields.root,
				index:  tt.fields.index,
				cache:  tt.fields.cache,
				logger: tt.fields.logger,
			}
			if err := r.addToIndex(tt.args.it, tt.args.path); !errors.Is(err, tt.wantErr) {
				t.Errorf("addToIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_collectionIndexStoragePath(t *testing.T) {
	type args struct {
		col vocab.IRI
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "empty",
			fields: fields{},
			args:   args{},
			want:   ".index",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields)
			defer r.Close()

			if got := r.collectionIndexStoragePath(tt.args.col); got != tt.want {
				t.Errorf("collectionIndexStoragePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_removeFromIndex(t *testing.T) {
	mockRoot := openRoot(t, t.TempDir())
	type args struct {
		it   vocab.Item
		path string
	}
	type test struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}
	tests := []test{
		{
			name:    "empty",
			wantErr: indexDisabled,
		},
		{
			name: "with empty root folder",
			fields: fields{
				path: t.TempDir(),
				root: openRoot(t, t.TempDir()),
			},
			wantErr: indexDisabled,
		},
		{
			name: "with empty bitmap and files to index",
			fields: fields{
				path: t.TempDir(),
				root: mockFilesToIndex(t, openRoot(t, t.TempDir())),
			},
			wantErr: indexDisabled,
		},
		{
			name: "add IRI to index",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   vocab.IRI("https://example.com/1/2/3"),
				path: "1-2-3",
			},
		},
		{
			name: "add Object to index",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   &vocab.Object{ID: "https://example.com/1/2/3", Type: vocab.NoteType},
				path: "1-2-3",
			},
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name: fmt.Sprintf("add %d %T to index", i, mockIt),
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				it:   mockIt,
				path: iriPath(mockIt.GetLink()),
			},
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				path:  tt.fields.path,
				root:  tt.fields.root,
				index: tt.fields.index,
				cache: tt.fields.cache,
			}
			if err := r.removeFromIndex(tt.args.it, tt.args.path); !errors.Is(err, tt.wantErr) {
				t.Errorf("removeFromIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_searchIndex(t *testing.T) {
	mockRoot := openRoot(t, t.TempDir())
	type args struct {
		col vocab.Item
		ff  []filters.Check
	}
	type test struct {
		name    string
		fields  fields
		args    args
		want    vocab.ItemCollection
		wantErr bool
	}
	tests := []test{
		{
			name:    "empty",
			wantErr: true,
		},
		{
			name: "with empty root folder",
			fields: fields{
				path: t.TempDir(),
				root: openRoot(t, t.TempDir()),
			},
			wantErr: true,
		},
		{
			name: "with empty bitmap and files to index",
			fields: fields{
				path: t.TempDir(),
				root: mockFilesToIndex(t, openRoot(t, t.TempDir())),
			},
			wantErr: true,
		},
		{
			name: "search random iri",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				col: vocab.IRI("https://example.com/1/2/3"),
				ff:  filters.Checks{},
			},
			wantErr: true,
		},
		{
			name: "search / for iri",
			fields: fields{
				path:  t.TempDir(),
				index: mockIndex(t, mockRoot),
				root:  mockFilesToIndex(t, mockRoot),
			},
			args: args{
				col: vocab.IRI("./"),
				ff:  filters.Checks{filters.SameID("https://example.com")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields)
			for _, mockIt := range mockItems {
				_ = r.addToIndex(mockIt, iriPath(mockIt.GetLink()))
			}
			got, err := r.searchIndex(vocab.IRI(""), tt.args.ff...)
			if (err != nil) != tt.wantErr {
				t.Errorf("searchIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("searchIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}
