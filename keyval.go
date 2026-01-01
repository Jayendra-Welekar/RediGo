package main

import (
	"fmt"
	"sync"
	"time"
)

type KV struct {
	mu    sync.RWMutex
	data  map[string][]byte
	cache Cacher
}

func NewKV(c Cacher) *KV {
	return &KV{
		cache: c,
		data:  map[string][]byte{},
	}
}

func (kv *KV) Set(key, val []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[string(key)] = []byte(val)
	if err := kv.cache.Set(key, val, 5*time.Minute); err != nil {
		fmt.Printf("cache set error: %v\n", err)
	}
	return nil
}

func (kv *KV) GetFromCache(key []byte) ([]byte, bool) {
	val, ok := kv.cache.Get(key)
	if ok {
		fmt.Println("Returning key from cache")
		return val, ok
	}
	return []byte{}, false
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if val, ok := kv.GetFromCache(key); ok {
		return val, ok
	}

	if val, ok := kv.data[string(key)]; ok{
		return val, ok
	}
	return nil, false
}

func (kv *KV) Delete(key []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.data, string(key))
	if err := kv.cache.Remove(key); err != nil {
		fmt.Printf("cache removal error: %v\n", err)
	}
}
func (kv *KV) Flush() {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    
    kv.data = make(map[string][]byte)  // Clear in-memory storage
    kv.cache.Clear() // Clear LRU cache
}

