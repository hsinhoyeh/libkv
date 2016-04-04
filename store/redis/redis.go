package redis

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/hsinhoyeh/libkv"
	"github.com/hsinhoyeh/libkv/store"

	"gopkg.in/redis.v3"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Redis
	ErrMultipleEndpointsUnsupported = errors.New("redis does not support multiple endpoints")

	// ErrTLSUnsupported is thrown when tls config is given
	ErrTLSUnsupported = errors.New("redis does not support tls")
)

// Register registers consul to libkv
func Register() {
	libkv.AddStore(store.REDIS, New)
}

// New creates a new Redis client given a list
// of endpoints and optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}
	if options != nil {
		return nil, ErrTLSUnsupported
	}

	// TODO: use *redis.ClusterClient if we support miltiple endpoints
	client := redis.NewClient(&redis.Options{
		Addr:         endpoints[0],
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	return &Redis{
		client: client,
	}, nil
}

type Redis struct {
	client *redis.Client
}

const (
	noExpiration = time.Duration(0)
)

type versionedValue struct {
	Value   []byte
	Version uint64
}

func (v *versionedValue) marshal() []byte {
	b, err := json.Marshal(*v)
	if err != nil {
		panic(err) // shouldn't happen
	}
	return b
}

func (v *versionedValue) unmarshal(in []byte) {
	if err := json.Unmarshal(in, v); err != nil {
		panic(err) // shouldn't happen
	}
}

func timeBasedVersion() uint64 {
	return uint64(time.Now().Nanosecond())
}

// Put a value at the specified key
func (r *Redis) Put(key string, value []byte, options *store.WriteOptions) error {
	expirationAfter := noExpiration
	if options != nil {
		expirationAfter = options.TTL
	}

	vv := &versionedValue{
		Value:   value,
		Version: timeBasedVersion(),
	}
	if err := r.client.Set(r.normalize(key), string(vv.marshal()), expirationAfter).Err(); err != nil {
		return err
	}
	return nil
}

// Get a value given its key
func (r *Redis) Get(key string) (*store.KVPair, error) {
	reply, err := r.client.Get(r.normalize(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	var vv versionedValue
	vv.unmarshal(reply)

	return &store.KVPair{
		Key:       key,
		Value:     vv.Value,
		LastIndex: vv.Version,
	}, nil
}

// Delete the value at the specified key
func (r *Redis) Delete(key string) error {
	if err := r.client.Del(r.normalize(key)).Err(); err != nil {
		return err
	}
	return nil
}

// Verify if a Key exists in the store
func (r *Redis) Exists(key string) (bool, error) {
	return r.client.Exists(r.normalize(key)).Result()
}

// List the content of a given prefix
func (r *Redis) List(directory string) ([]*store.KVPair, error) {
	const (
		startCursor  = 0
		endCursor    = 0
		defaultCount = 10
	)

	var allKeys []string
	regex := r.normalize(directory) + "*" // for all keyed with $directory
	allKeys, err := r.keys(regex)
	if err != nil {
		return nil, err
	}
	// TODO: need to handle when #key is too large
	return r.mget(allKeys...)
}

func (r *Redis) keys(regex string) ([]string, error) {
	const (
		startCursor  = 0
		endCursor    = 0
		defaultCount = 10
	)

	var allKeys []string

	nextCursor, keys, err := r.client.Scan(startCursor, regex, defaultCount).Result()
	if err != nil {
		return nil, err
	}
	allKeys = append(allKeys, keys...)
	for nextCursor != endCursor {
		nextCursor, keys, err = r.client.Scan(startCursor, regex, defaultCount).Result()
		if err != nil {
			return nil, err
		}
		allKeys = append(allKeys, keys...)
	}
	if len(allKeys) == 0 {
		return nil, store.ErrKeyNotFound
	}
	return allKeys, nil
}

// mget values given their keys
func (r *Redis) mget(keys ...string) ([]*store.KVPair, error) {
	replies, err := r.client.MGet(keys...).Result()
	if err != nil {
		return nil, err
	}

	var pairs []*store.KVPair
	for index, reply := range replies {
		var sreply string
		key := keys[index]
		if _, ok := reply.(string); ok {
			sreply = reply.(string)
		}
		if sreply == "" {
			// empty reply
			continue
		}

		vv := &versionedValue{}
		vv.unmarshal([]byte(sreply))
		pairs = append(pairs, &store.KVPair{
			Key:       key,
			Value:     vv.Value,
			LastIndex: vv.Version,
		})
	}
	return pairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (r *Redis) DeleteTree(directory string) error {
	var allKeys []string
	regex := r.normalize(directory) + "*" // for all keyed with $directory
	allKeys, err := r.keys(regex)
	if err != nil {
		return err
	}
	return r.client.Del(allKeys...).Err()
}

// Atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
// NOTE: we haven't introduce script on this page yet.
// We perform read-modify-write on two separated operations which is not atomic guaranteed
func (r *Redis) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	existedOne, err := r.Get(key)
	if err != nil && err != store.ErrKeyNotFound {
		return false, nil, err
	}
	if previous == nil && existedOne != nil {
		return false, nil, store.ErrKeyModified
	}
	if previous != nil {
		if existedOne == nil {
			return false, nil, store.ErrKeyModified
		}
		// check existed == previous
		if existedOne.LastIndex != previous.LastIndex {
			return false, nil, store.ErrKeyModified
		}
		// ok for now, delete first, so setnx can work
		if err := r.Delete(key); err != nil {
			return false, nil, err
		}
	}

	// write
	expirationAfter := noExpiration
	if options != nil {
		expirationAfter = options.TTL
	}
	nkey := r.normalize(key)
	vv := versionedValue{
		Value:   value,
		Version: timeBasedVersion(),
	}
	if err := r.client.SetNX(nkey, string(vv.marshal()), expirationAfter).Err(); err != nil {
		return false, nil, err
	}
	return true, &store.KVPair{
		Key:       nkey,
		Value:     vv.Value,
		LastIndex: vv.Version,
	}, nil
}

// Atomic delete of a single value
// NOTE: we haven't introduce script on this page yet.
// We perform read-modify-write on two separated operations which is not atomic guaranteed
func (r *Redis) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	existedOne, err := r.Get(key)
	if err != nil && err != store.ErrKeyNotFound {
		return false, err
	}
	if previous == nil && existedOne != nil {
		return false, store.ErrKeyModified
	}
	if previous != nil {
		if existedOne == nil {
			return false, store.ErrKeyModified
		}
		// check existed == previous
		if existedOne.LastIndex != previous.LastIndex {
			return false, store.ErrKeyModified
		}
	}
	return true, r.Delete(key)
}

// Close the store connection
func (r *Redis) Close() {
	r.client.Close()
}

func (r *Redis) normalize(key string) string {
	return store.Normalize(key)
}
