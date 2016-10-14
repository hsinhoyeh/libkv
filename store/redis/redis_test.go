package redis

import (
	"testing"

	"github.com/docker/libkv/testutils"
	"github.com/maichain/libkv"
	"github.com/maichain/libkv/store"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:6379"
)

func makeRedisClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			Password: "", // password is empty by default
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := libkv.NewStore(store.REDIS, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Redis); !ok {
		t.Fatal("Error registering and initializing redis")
	}
}

func TestRedisStore(t *testing.T) {
	kv := makeRedisClient(t)
	kvTTL := makeRedisClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestTTL(t, kv, kvTTL)
	testutils.RunCleanup(t, kv)
}
