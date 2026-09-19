package memory

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/realab/go-git-thread-safe/v5/plumbing"
	"github.com/realab/go-git-thread-safe/v5/plumbing/storer"
	"github.com/realab/go-git-thread-safe/v5/storage"
)

func TestReferenceStorageConcurrentAccess(t *testing.T) {
	for _, tc := range []struct {
		name string
		refs storer.ReferenceStorer
	}{
		{"storage", NewStorage()},
		{"references", &ReferenceStorage{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs := tc.refs
			start := make(chan struct{})
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					ref := plumbing.NewHashReference("refs/heads/main", plumbing.Hash{byte(worker + 1)})
					next := plumbing.NewHashReference(ref.Name(), plumbing.Hash{byte(worker + 9)})
					for i := 0; i < 100; i++ {
						if err := refs.SetReference(ref); err != nil {
							t.Error(err)
							return
						}
						if err := refs.CheckAndSetReference(next, ref); err != nil && err != storage.ErrReferenceHasChanged {
							t.Error(err)
							return
						}
						got, err := refs.Reference(ref.Name())
						if err != nil && err != plumbing.ErrReferenceNotFound {
							t.Error(err)
							return
						}
						if err == nil && (got == nil || got.Name() != ref.Name()) {
							t.Errorf("unexpected reference: %v", got)
							return
						}
						iter, err := refs.IterReferences()
						if err != nil {
							t.Error(err)
							return
						}
						if err := iter.ForEach(func(got *plumbing.Reference) error {
							if got == nil || got.Name() != ref.Name() {
								return fmt.Errorf("unexpected iterated reference: %v", got)
							}
							return nil
						}); err != nil {
							t.Error(err)
							return
						}
						if count, err := refs.CountLooseRefs(); err != nil || count < 0 || count > 1 {
							t.Errorf("unexpected reference count: %d, %v", count, err)
							return
						}
						if err := refs.RemoveReference(ref.Name()); err != nil {
							t.Error(err)
							return
						}
					}
				}(worker)
			}
			close(start)
			wg.Wait()
		})
	}
}

func TestReferenceStorageConcurrentCheckAndSet(t *testing.T) {
	for round := 0; round < 25; round++ {
		refs := NewStorage()
		old := plumbing.NewHashReference("refs/heads/main", plumbing.ZeroHash)
		// Exercise both replacement and creation of a missing reference.
		if round%2 == 0 {
			if err := refs.SetReference(old); err != nil {
				t.Fatal(err)
			}
		}
		start := make(chan struct{})
		winners := make(chan *plumbing.Reference, 32)
		var wg sync.WaitGroup
		for worker := 0; worker < cap(winners); worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				<-start
				ref := plumbing.NewHashReference(old.Name(), plumbing.Hash{byte(worker + 1)})
				// Compare by hash, even when old is a different reference instance.
				expected := plumbing.NewHashReference(old.Name(), old.Hash())
				switch err := refs.CheckAndSetReference(ref, expected); err {
				case nil:
					winners <- ref
				case storage.ErrReferenceHasChanged:
				default:
					t.Error(err)
				}
			}(worker)
		}
		close(start)
		wg.Wait()
		close(winners)
		if len(winners) != 1 {
			t.Fatalf("round %d: got %d successful updates, want 1", round, len(winners))
		}
		want := <-winners
		if got, err := refs.Reference(old.Name()); err != nil || got != want {
			t.Fatalf("stored reference = %v, %v; want %v", got, err, want)
		}
	}
}

func TestReferenceStorageZeroValue(t *testing.T) {
	var refs ReferenceStorage
	if got, err := refs.Reference("missing"); got != nil || err != plumbing.ErrReferenceNotFound {
		t.Fatalf("missing reference = %v, %v", got, err)
	}
	if err := refs.RemoveReference("missing"); err != nil {
		t.Fatal(err)
	}
	if err := refs.SetReference(nil); err != nil {
		t.Fatal(err)
	}
	old := plumbing.NewHashReference("refs/heads/main", plumbing.Hash{1})
	if err := refs.CheckAndSetReference(nil, old); err != nil {
		t.Fatal(err)
	}
	if count, err := refs.CountLooseRefs(); count != 0 || err != nil {
		t.Fatalf("empty reference count = %d, %v", count, err)
	}
	iter, err := refs.IterReferences()
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(); err != io.EOF {
		t.Fatalf("empty iterator error = %v, want EOF", err)
	}
	// A missing reference can be created even when an old value is supplied.
	if err := refs.CheckAndSetReference(old, old); err != nil {
		t.Fatal(err)
	}
	next := plumbing.NewHashReference(old.Name(), plumbing.Hash{2})
	if err := refs.CheckAndSetReference(next, nil); err != nil {
		t.Fatal(err)
	}
	if err := refs.CheckAndSetReference(old, old); err != storage.ErrReferenceHasChanged {
		t.Fatalf("stale update error = %v, want ErrReferenceHasChanged", err)
	}
	if got, err := refs.Reference(old.Name()); err != nil || got != next {
		t.Fatalf("stored reference = %v, %v; want %v", got, err, next)
	}
	if count, err := refs.CountLooseRefs(); count != 1 || err != nil {
		t.Fatalf("reference count = %d, %v; want 1", count, err)
	}
}

func TestReferenceStorageIteratorIsolation(t *testing.T) {
	refs := NewStorage()
	old := plumbing.NewHashReference("refs/heads/main", plumbing.Hash{1})
	if err := refs.SetReference(old); err != nil {
		t.Fatal(err)
	}
	iter, err := refs.IterReferences()
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if err := refs.RemoveReference(old.Name()); err != nil {
		t.Fatal(err)
	}
	if err := refs.SetReference(plumbing.NewHashReference(old.Name(), plumbing.Hash{2})); err != nil {
		t.Fatal(err)
	}
	if got, err := iter.Next(); err != nil || got != old {
		t.Fatalf("iterator reference = %v, %v; want %v", got, err, old)
	}
	if _, err := iter.Next(); err != io.EOF {
		t.Fatalf("iterator error = %v, want EOF", err)
	}
}
