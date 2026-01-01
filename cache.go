package main

import "time"

type Cacher interface {
	Get([]byte) ([]byte, bool)
	Set([]byte, []byte, time.Duration) error
	Remove([]byte) error
	Clear()
}

type NopCache struct{}

func (nc NopCache) Get([]byte) ([]byte, bool) {
	return []byte{}, false
}

func (nc NopCache) Set([]byte, []byte, time.Duration) error {
	return nil
}

func (nc NopCache) Remove([]byte) error {
	return nil
}

func (nc NopCache) Clear() {}
