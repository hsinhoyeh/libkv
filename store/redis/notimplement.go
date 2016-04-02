package redis

import (
	"errors"

	"github.com/docker/libkv/store"
)

var (
	// ErrNotImplemented is thrown when this function does not support yet
	ErrNotImplemented = errors.New("redis driver does not support this function yet")
)

// Watch for changes on a key
func (r *Redis) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	// TODO
	return nil, ErrNotImplemented
}

// WatchTree watches for changes on child nodes under
// a given directory
func (r *Redis) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	// TODO
	return nil, ErrNotImplemented
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (r *Redis) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	// TODO
	return nil, ErrNotImplemented
}
