package gstore

import (
	"errors"
	"github.com/patrickmn/go-cache"
	"sync"
)

var g *GlobalStore

var once = sync.Once{}

var lock = sync.RWMutex{}

type GlobalStore struct {
	store *cache.Cache
}

func Init() {
	// make sure to init only once
	once.Do(func() {
		c := cache.New(cache.NoExpiration, cache.NoExpiration)
		g = &GlobalStore{
			store: c,
		}
	},
	)
}

func Add(key string, data any) error {
	lock.Lock()
	defer lock.Unlock()

	return g.store.Add(key, data, cache.NoExpiration)
}

func Get(key string) (any, error) {
	lock.RLock()
	defer lock.RUnlock()

	val, exist := g.store.Get(key)

	if !exist {
		return nil, errors.New("Gstore No such key: " + key)
	}

	return val, nil
}

func GetString(key string) (string, error) {
	val, exist := g.store.Get(key)

	if !exist {
		return "", errors.New("Gstore No such key of string" + key)
	}

	return val.(string), nil
}

func Put(key string, value any) {
	lock.Lock()
	defer lock.Unlock()

	g.store.Set(key, value, cache.NoExpiration)
}
