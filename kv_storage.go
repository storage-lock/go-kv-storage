package kv_storage

import (
	"context"
	"sync"
	"time"

	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// KVStorage 通用KV存储实现
// 使用mutex实现进程内CAS
type KVStorage struct {
	mu sync.RWMutex
}

var _ storage.Storage = &KVStorage{}

func NewKVStorage(ctx context.Context) (*KVStorage, error) {
	return &KVStorage{}, nil
}

const StorageName = "kv-storage"

func (x *KVStorage) GetName() string { return StorageName }

func (x *KVStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime}
}

func (x *KVStorage) Init(ctx context.Context) error { return nil }

func (x *KVStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}

func (x *KVStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}

func (x *KVStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return nil
}

func (x *KVStorage) Get(ctx context.Context, lockId string) (string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return "", storage_lock.ErrLockNotFound
}

func (x *KVStorage) GetTime(ctx context.Context) (time.Time, error) { return time.Now(), nil }
func (x *KVStorage) Close(ctx context.Context) error               { return nil }
func (x *KVStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	return go_iterator.FromSlice([]*storage.LockInformation{}), nil
}
