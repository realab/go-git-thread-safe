// Package memory is a storage backend base on memory
package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/realab/go-git-thread-safe/v5/config"
	"github.com/realab/go-git-thread-safe/v5/plumbing"
	"github.com/realab/go-git-thread-safe/v5/plumbing/format/index"
	"github.com/realab/go-git-thread-safe/v5/plumbing/storer"
	"github.com/realab/go-git-thread-safe/v5/storage"
)

var ErrUnsupportedObjectType = fmt.Errorf("unsupported object type")

// Storage is an implementation of git.Storer that stores data on memory, being
// ephemeral. The use of this storage should be done in controlled environments,
// since the representation in memory of some repository can fill the machine
// memory. in the other hand this storage has the best performance.
type Storage struct {
	ConfigStorage
	ObjectStorage
	ShallowStorage
	IndexStorage
	ReferenceStorage
	ModuleStorage
}

// NewStorage returns a new Storage base on memory
func NewStorage() *Storage {
	return &Storage{
		ReferenceStorage: make(ReferenceStorage),
		ConfigStorage:    ConfigStorage{},
		ShallowStorage:   ShallowStorage{},
		ObjectStorage: ObjectStorage{
			Objects: &sync.Map{},
			Commits: &sync.Map{},
			Trees:   &sync.Map{},
			Blobs:   &sync.Map{},
			Tags:    &sync.Map{},
		},
		ModuleStorage: make(ModuleStorage),
	}
}

type ConfigStorage struct {
	config *config.Config
}

func (c *ConfigStorage) SetConfig(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg
	return nil
}

func (c *ConfigStorage) Config() (*config.Config, error) {
	if c.config == nil {
		c.config = config.NewConfig()
	}

	return c.config, nil
}

type IndexStorage struct {
	index *index.Index
}

func (c *IndexStorage) SetIndex(idx *index.Index) error {
	c.index = idx
	return nil
}

func (c *IndexStorage) Index() (*index.Index, error) {
	if c.index == nil {
		c.index = &index.Index{Version: 2}
	}

	return c.index, nil
}

type ObjectStorage struct {
	Objects *sync.Map // map[plumbing.Hash]plumbing.EncodedObject
	Commits *sync.Map // map[plumbing.Hash]plumbing.EncodedObject
	Trees   *sync.Map // map[plumbing.Hash]plumbing.EncodedObject
	Blobs   *sync.Map // map[plumbing.Hash]plumbing.EncodedObject
	Tags    *sync.Map // map[plumbing.Hash]plumbing.EncodedObject
}

func (o *ObjectStorage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

func (o *ObjectStorage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	h := obj.Hash()
	o.Objects.Store(h, obj)

	switch obj.Type() {
	case plumbing.CommitObject:
		val, _ := o.Objects.Load(h)
		o.Commits.Store(h, val)
	case plumbing.TreeObject:
		val, _ := o.Objects.Load(h)
		o.Trees.Store(h, val)
	case plumbing.BlobObject:
		val, _ := o.Objects.Load(h)
		o.Blobs.Store(h, val)
	case plumbing.TagObject:
		val, _ := o.Objects.Load(h)
		o.Tags.Store(h, val)
	default:
		return h, ErrUnsupportedObjectType
	}

	return h, nil
}

func (o *ObjectStorage) HasEncodedObject(h plumbing.Hash) (err error) {
	if _, ok := o.Objects.Load(h); !ok {
		return plumbing.ErrObjectNotFound
	}
	return nil
}

func (o *ObjectStorage) EncodedObjectSize(h plumbing.Hash) (
	size int64, err error) {
	val, ok := o.Objects.Load(h)
	if !ok {
		return 0, plumbing.ErrObjectNotFound
	}
	obj := val.(plumbing.EncodedObject)

	return obj.Size(), nil
}

func (o *ObjectStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	val, ok := o.Objects.Load(h)
	if !ok {
		return nil, plumbing.ErrObjectNotFound
	}
	obj := val.(plumbing.EncodedObject)
	if plumbing.AnyObject != t && obj.Type() != t {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

func (o *ObjectStorage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	var series []plumbing.EncodedObject
	switch t {
	case plumbing.AnyObject:
		series = flattenObjectSyncMap(o.Objects)
	case plumbing.CommitObject:
		series = flattenObjectSyncMap(o.Commits)
	case plumbing.TreeObject:
		series = flattenObjectSyncMap(o.Trees)
	case plumbing.BlobObject:
		series = flattenObjectSyncMap(o.Blobs)
	case plumbing.TagObject:
		series = flattenObjectSyncMap(o.Tags)
	}

	return storer.NewEncodedObjectSliceIter(series), nil
}

// func flattenObjectMap(m map[plumbing.Hash]plumbing.EncodedObject) []plumbing.EncodedObject {
// 	objects := make([]plumbing.EncodedObject, 0, len(m))
// 	for _, obj := range m {
// 		objects = append(objects, obj)
// 	}
// 	return objects
// }

func flattenObjectSyncMap(m *sync.Map) []plumbing.EncodedObject {
	objects := []plumbing.EncodedObject{}
	m.Range(func(_, v interface{}) bool {
		objects = append(objects, v.(plumbing.EncodedObject))
		return true
	})
	return objects
}

func (o *ObjectStorage) Begin() storer.Transaction {
	return &TxObjectStorage{
		Storage: o,
		Objects: make(map[plumbing.Hash]plumbing.EncodedObject),
	}
}

func (o *ObjectStorage) ForEachObjectHash(fun func(plumbing.Hash) error) error {
	var innerError error
	o.Objects.Range(func(k, _ interface{}) bool {
		h := k.(plumbing.Hash)
		err := fun(h)
		if err != nil {
			if err == storer.ErrStop {
				return false
			}
			innerError = err
			return false
		}
		return true
	})
	return innerError
}

func (o *ObjectStorage) ObjectPacks() ([]plumbing.Hash, error) {
	return nil, nil
}
func (o *ObjectStorage) DeleteOldObjectPackAndIndex(plumbing.Hash, time.Time) error {
	return nil
}

var errNotSupported = fmt.Errorf("not supported")

func (o *ObjectStorage) LooseObjectTime(hash plumbing.Hash) (time.Time, error) {
	return time.Time{}, errNotSupported
}
func (o *ObjectStorage) DeleteLooseObject(plumbing.Hash) error {
	return errNotSupported
}

type TxObjectStorage struct {
	Storage *ObjectStorage
	Objects map[plumbing.Hash]plumbing.EncodedObject
}

func (tx *TxObjectStorage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	h := obj.Hash()
	tx.Objects[h] = obj

	return h, nil
}

func (tx *TxObjectStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	obj, ok := tx.Objects[h]
	if !ok || (plumbing.AnyObject != t && obj.Type() != t) {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

func (tx *TxObjectStorage) Commit() error {
	for h, obj := range tx.Objects {
		delete(tx.Objects, h)
		if _, err := tx.Storage.SetEncodedObject(obj); err != nil {
			return err
		}
	}

	return nil
}

func (tx *TxObjectStorage) Rollback() error {
	tx.Objects = make(map[plumbing.Hash]plumbing.EncodedObject)
	return nil
}

type ReferenceStorage map[plumbing.ReferenceName]*plumbing.Reference

func (r ReferenceStorage) SetReference(ref *plumbing.Reference) error {
	if ref != nil {
		r[ref.Name()] = ref
	}

	return nil
}

func (r ReferenceStorage) CheckAndSetReference(ref, old *plumbing.Reference) error {
	if ref == nil {
		return nil
	}

	if old != nil {
		tmp := r[ref.Name()]
		if tmp != nil && tmp.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
	}
	r[ref.Name()] = ref
	return nil
}

func (r ReferenceStorage) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	ref, ok := r[n]
	if !ok {
		return nil, plumbing.ErrReferenceNotFound
	}

	return ref, nil
}

func (r ReferenceStorage) IterReferences() (storer.ReferenceIter, error) {
	var refs []*plumbing.Reference
	for _, ref := range r {
		refs = append(refs, ref)
	}

	return storer.NewReferenceSliceIter(refs), nil
}

func (r ReferenceStorage) CountLooseRefs() (int, error) {
	return len(r), nil
}

func (r ReferenceStorage) PackRefs() error {
	return nil
}

func (r ReferenceStorage) RemoveReference(n plumbing.ReferenceName) error {
	delete(r, n)
	return nil
}

type ShallowStorage []plumbing.Hash

func (s *ShallowStorage) SetShallow(commits []plumbing.Hash) error {
	*s = commits
	return nil
}

func (s ShallowStorage) Shallow() ([]plumbing.Hash, error) {
	return s, nil
}

type ModuleStorage map[string]*Storage

func (s ModuleStorage) Module(name string) (storage.Storer, error) {
	if m, ok := s[name]; ok {
		return m, nil
	}

	m := NewStorage()
	s[name] = m

	return m, nil
}
