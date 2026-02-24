package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/filters"
	"github.com/go-ap/storage-conformance-suite/gen"
)

func setBenchId(it vocab.Item) {
	u, _ := gen.RootID.URL()
	u.Path = ""
	base := u.String()
	_ = vocab.OnObject(it, setId(vocab.IRI(base)))
}

var collectionIRI = vocab.Inbox.Of(gen.Root).GetLink()

var results = make(map[vocab.Typer]int)

func populate(st *repo, count int) error {
	oldSetter := gen.SetItemID
	defer func() {
		gen.SetItemID = oldSetter
	}()
	gen.SetItemID = setBenchId

	if _, err := st.Save(gen.Root); err != nil {
		return err
	}
	results[gen.Root.GetType()]++

	col := gen.RandomCollection(gen.Root)
	_ = vocab.OnObject(col, func(ob *vocab.Object) error {
		ob.ID = collectionIRI
		return nil
	})
	if _, err := st.Create(col); err != nil {
		return err
	}
	results[col.GetType()]++

	var success int
	var failure int

	items := gen.RandomItemCollection(count)
	for _, it := range items {
		if _, err := st.Save(it); err != nil {
			failure++
			continue
		}
		results[it.GetType()]++
		success++
	}
	if err := st.AddTo(col.GetLink(), items...); err != nil {
		return err
	}
	if failure > 0 {
		return fmt.Errorf("failed to save %d items", failure)
	}

	return nil
}

func setup(conf Config, count int) (*repo, error) {
	st, err := New(conf)
	if err != nil {
		return nil, err
	}
	if err = st.Open(); err != nil {
		return nil, err
	}
	if err := populate(st, count); err != nil {
		return nil, err
	}

	return st, nil
}

var tempDir = filepath.Join(os.TempDir(), "storage-fs-bench")

var _ = func() error {
	st := time.Now()
	_ = os.RemoveAll(tempDir)

	_ = os.MkdirAll(tempDir, defaultDirPerm)
	_, _ = setup(Config{Path: tempDir, EnableIndex: true, EnableCache: true}, count)

	fmt.Printf("created temp dir in %s %s\n", time.Now().Sub(st), tempDir)

	return nil
}()

const count = 2000

var checks = filters.Checks{}

func Benchmark_Load_All(b *testing.B) {
	st, err := New(Config{Path: tempDir, EnableOptimizedFiltering: true, EnableIndex: true, EnableCache: true})
	if err != nil {
		b.Fatalf("unable to initialize storage %s", err)
	}
	if err = st.Open(); err != nil {
		b.Fatalf("unable to open storage %s", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = st.Load(collectionIRI, checks...)
	}
}

func Benchmark_Load_None(b *testing.B) {
	st, err := New(Config{Path: tempDir, EnableOptimizedFiltering: false, EnableIndex: false, EnableCache: false})
	if err != nil {
		b.Fatalf("unable to initialize storage %s", err)
	}
	if err = st.Open(); err != nil {
		b.Fatalf("unable to open storage %s", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = st.Load(collectionIRI, checks...)
	}
}

func Benchmark_Load_wIndex(b *testing.B) {
	st, err := New(Config{Path: tempDir, EnableOptimizedFiltering: false, EnableIndex: true, EnableCache: false})
	if err != nil {
		b.Fatalf("unable to initialize storage %s", err)
	}
	if err = st.Open(); err != nil {
		b.Fatalf("unable to open storage %s", err)
	}
	b.ResetTimer()
	for b.Loop() {
		_, _ = st.Load(collectionIRI, checks...)
	}
}

func Benchmark_Load_wCache(b *testing.B) {
	st, err := New(Config{Path: tempDir, EnableOptimizedFiltering: false, EnableIndex: false, EnableCache: true})
	if err != nil {
		b.Fatalf("unable to initialize storage %s", err)
	}
	if err = st.Open(); err != nil {
		b.Fatalf("unable to open storage %s", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = st.Load(collectionIRI, checks...)
	}
}

func Benchmark_Load_wQuamina(b *testing.B) {
	st, err := New(Config{Path: tempDir, EnableOptimizedFiltering: false, EnableIndex: false, EnableCache: false})
	if err != nil {
		b.Fatalf("unable to initialize storage %s", err)
	}
	if err = st.Open(); err != nil {
		b.Fatalf("unable to open storage %s", err)
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = st.Load(collectionIRI, checks...)
	}
}
