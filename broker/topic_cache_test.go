package broker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestTopicConfigCacheMissSuppressesConcurrentLoad(t *testing.T) {
	b, meta := newBlockingTopicConfigBroker(t)
	release := releaseBlockingTopicConfigStore(meta)
	defer release()

	firstResult := publishBlockedTopicRecord(t, b, "first")
	waitForTopicCacheSignal(t, meta.firstLoadStarted, "first topic config load")

	duplicateResults := publishDuplicateTopicRecords(b, "duplicate", 16)
	requireNoDuplicateTopicConfigLoad(t, meta)
	release()

	requirePublishResult(t, <-firstResult)
	for range cap(duplicateResults) {
		requirePublishResult(t, <-duplicateResults)
	}
	if got := meta.loadCount(); got != 1 {
		t.Fatalf("LoadTopicConfig calls = %d, want 1", got)
	}
}

func newBlockingTopicConfigBroker(t *testing.T) (*broker.Broker, *blockingTopicConfigStore) {
	t.Helper()
	backing := store.NewMemoryStore()
	if err := backing.SaveTopicConfig(store.NewTopicConfig("orders")); err != nil {
		t.Fatal(err)
	}
	meta := &blockingTopicConfigStore{
		MemoryStore:          backing,
		firstLoadStarted:     make(chan struct{}),
		duplicateLoadStarted: make(chan struct{}),
		release:              make(chan struct{}),
	}
	cfg := broker.DefaultConfig()
	cfg.Broker.TopicCacheMaxEntries = 16
	b, err := broker.NewWithStores(cfg, meta, meta)
	if err != nil {
		t.Fatal(err)
	}
	return b, meta
}

func releaseBlockingTopicConfigStore(meta *blockingTopicConfigStore) func() {
	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			close(meta.release)
		})
	}
}

func publishBlockedTopicRecord(t *testing.T, b *broker.Broker, payload string) <-chan publishCallResult {
	t.Helper()
	result := make(chan publishCallResult, 1)
	go func() {
		result <- publishTopicRecord(b, payload)
	}()
	return result
}

func publishDuplicateTopicRecords(b *broker.Broker, prefix string, callers int) <-chan publishCallResult {
	results := make(chan publishCallResult, callers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for index := range callers {
		go func() {
			ready.Done()
			<-start
			results <- publishTopicRecord(b, fmt.Sprintf("%s-%d", prefix, index))
		}()
	}
	ready.Wait()
	close(start)
	return results
}

func requireNoDuplicateTopicConfigLoad(t *testing.T, meta *blockingTopicConfigStore) {
	t.Helper()
	select {
	case <-meta.duplicateLoadStarted:
		t.Fatal("topic cache miss loaded the same topic more than once while the first load was in flight")
	case <-time.After(200 * time.Millisecond):
	}
}

type blockingTopicConfigStore struct {
	*store.MemoryStore

	mu                   sync.Mutex
	loads                int
	firstOnce            sync.Once
	duplicateOnce        sync.Once
	firstLoadStarted     chan struct{}
	duplicateLoadStarted chan struct{}
	release              chan struct{}
}

func (s *blockingTopicConfigStore) LoadTopicConfig(topic string) (*store.TopicConfig, error) {
	loads := s.recordLoad()
	if loads == 1 {
		s.firstOnce.Do(func() {
			close(s.firstLoadStarted)
		})
	} else {
		s.duplicateOnce.Do(func() {
			close(s.duplicateLoadStarted)
		})
	}
	<-s.release
	cfg, err := s.MemoryStore.LoadTopicConfig(topic)
	if err != nil {
		return nil, fmt.Errorf("load topic config: %w", err)
	}
	return cfg, nil
}

func (s *blockingTopicConfigStore) recordLoad() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	return s.loads
}

func (s *blockingTopicConfigStore) loadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads
}

type publishCallResult struct {
	result broker.ProduceResult
	err    error
}

func publishTopicRecord(b *broker.Broker, payload string) publishCallResult {
	result, err := b.PublishRecord(
		context.Background(),
		"orders",
		broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
		store.NewRecordAppend([]byte(payload)),
	)
	return publishCallResult{result: result, err: err}
}

func requirePublishResult(t *testing.T, result publishCallResult) {
	t.Helper()
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.result.Record.Payload) == 0 {
		t.Fatalf("unexpected publish result: %#v", result.result)
	}
}

func waitForTopicCacheSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}
