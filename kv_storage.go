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
// 进程内 map + mutex 实现 CAS 语义（单机多 goroutine 安全，跨进程需服务端配合）
type KVStorage struct {
	mu         sync.RWMutex
	storageMap map[string]*kvStorageValue
}

type kvStorageValue struct {
	Version                   storage.Version
	LockInformationJsonString string
}

var _ storage.Storage = &KVStorage{}

func NewKVStorage(ctx context.Context) (*KVStorage, error) {
	return &KVStorage{
		storageMap: make(map[string]*kvStorageValue),
	}, nil
}

const StorageName = "kv-storage"

func (x *KVStorage) GetName() string { return StorageName }

func (x *KVStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete, storage.CapabilityWatch}
}

func (x *KVStorage) Init(ctx context.Context) error { return nil }

func (x *KVStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, exists := x.storageMap[lockId]; exists {
		return storage_lock.ErrLockAlreadyExists
	}
	x.storageMap[lockId] = &kvStorageValue{
		Version:                   version,
		LockInformationJsonString: lockInformation.ToJsonString(),
	}
	return nil
}

func (x *KVStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	oldValue, exists := x.storageMap[lockId]
	if !exists {
		// 记录不存在返回 ErrVersionMiss（可重试），不是 ErrLockNotFound（硬错误）。
		// 对齐 memory/etcd/zookeeper 已修口径。
		return storage_lock.ErrVersionMiss
	}
	if oldValue.Version != expectedVersion {
		return storage_lock.ErrVersionMiss
	}
	oldValue.Version = newVersion
	oldValue.LockInformationJsonString = lockInformation.ToJsonString()
	return nil
}

func (x *KVStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	oldValue, exists := x.storageMap[lockId]
	if !exists {
		return storage_lock.ErrLockNotFound
	}
	if oldValue.Version != expectedVersion {
		return storage_lock.ErrVersionMiss
	}
	delete(x.storageMap, lockId)
	return nil
}

func (x *KVStorage) Get(ctx context.Context, lockId string) (string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	value, exists := x.storageMap[lockId]
	if !exists {
		return "", storage_lock.ErrLockNotFound
	}
	return value.LockInformationJsonString, nil
}

func (x *KVStorage) GetTime(ctx context.Context) (time.Time, error) { return time.Now(), nil }
func (x *KVStorage) Close(ctx context.Context) error               { return nil }
func (x *KVStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	x.mu.RLock()
	slice := make([]*storage.LockInformation, 0, len(x.storageMap))
	for _, v := range x.storageMap {
		info, err := storage.LockInformationFromJsonString(v.LockInformationJsonString)
		if err != nil {
			x.mu.RUnlock()
			return nil, err
		}
		slice = append(slice, info)
	}
	x.mu.RUnlock()
	return go_iterator.FromSlice(slice), nil
}

func (x *KVStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}
