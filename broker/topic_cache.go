package broker

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/lyonbrown4d/ech0/store"
	"golang.org/x/sync/singleflight"
)

const topicCacheBufferItems = 64

var (
	errTopicCacheDisabled = errors.New("topic cache disabled")
	topicConfigLoadGroup  singleflight.Group
)

type topicConfigLoadResult struct {
	topic store.TopicConfig
	found bool
}

func newTopicConfigCache(maxEntries int64) (*ristretto.Cache[string, store.TopicConfig], error) {
	if maxEntries <= 0 {
		return nil, errTopicCacheDisabled
	}
	cache, err := ristretto.NewCache(&ristretto.Config[string, store.TopicConfig]{
		NumCounters: maxEntries * 10,
		MaxCost:     maxEntries,
		BufferItems: topicCacheBufferItems,
	})
	if err != nil {
		return nil, wrapBroker("topic_cache_create_failed", err, "create topic metadata cache")
	}
	return cache, nil
}

func (b *Broker) topicConfig(name string) (*store.TopicConfig, error) {
	cache := b.topicCache
	if cache == nil {
		return b.loadTopicConfigDirect(name)
	}
	if cached, ok := cache.Get(name); ok {
		topic := cached
		return &topic, nil
	}

	value, err, _ := topicConfigLoadGroup.Do(topicConfigLoadKey(b, name), func() (any, error) {
		if cached, ok := cache.Get(name); ok {
			return topicConfigLoadResult{topic: cached, found: true}, nil
		}
		topic, err := b.loadTopicConfigDirect(name)
		if err != nil || topic == nil {
			return topicConfigLoadResult{}, err
		}
		b.cacheTopicConfig(*topic)
		return topicConfigLoadResult{topic: *topic, found: true}, nil
	})
	if err != nil {
		return nil, wrapBroker("topic_cache_load_failed", err, "load topic config")
	}
	loaded, ok := value.(topicConfigLoadResult)
	if !ok {
		return nil, brokerStoreError(store.CodeCodec, "invalid topic config load result %T", value)
	}
	if !loaded.found {
		var absent *store.TopicConfig
		return absent, nil
	}
	topic := loaded.topic
	return &topic, nil
}

func topicConfigLoadKey(b *Broker, name string) string {
	return fmt.Sprintf("%p\x00%s", b, name)
}

func (b *Broker) loadTopicConfigDirect(name string) (*store.TopicConfig, error) {
	topic, err := b.meta.LoadTopicConfig(name)
	if err != nil {
		return nil, wrapBrokerStore(err, "load topic config")
	}
	return topic, nil
}

func (b *Broker) cacheTopicConfig(topic store.TopicConfig) {
	if b.topicCache == nil {
		return
	}
	if b.topicCache.Set(topic.Name, topic, 1) {
		b.topicCache.Wait()
	}
}

func (b *Broker) closeTopicCache() {
	cache := b.topicCache
	if cache != nil {
		b.topicCache = nil
		cache.Close()
	}
}
