package store

import (
	"cmp"
	"slices"
)

func (s *MemoryStore) SaveConsumerOffsetState(state ConsumerOffsetState) error {
	state = NormalizeConsumerOffsetState(state)
	if err := validateConsumerOffsetState(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := offsetKey(state.Consumer, state.TopicPartition())
	s.offsets.Set(key, state.NextOffset)
	s.offsetStates.Set(key, state)
	return nil
}

func (s *MemoryStore) SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error {
	return s.SaveConsumerOffsetState(ConsumerOffsetState{
		Consumer:    consumer,
		Topic:       topicPartition.Topic,
		Partition:   topicPartition.Partition,
		NextOffset:  nextOffset,
		UpdatedAtMS: NowMS(),
	})
}

func (s *MemoryStore) LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.offsetStates.Get(offsetKey(consumer, topicPartition))
	if ok {
		return &state.NextOffset, nil
	}
	value, ok := s.offsets.Get(offsetKey(consumer, topicPartition))
	if !ok {
		var absent *uint64
		return absent, nil
	}
	return &value, nil
}

func (s *MemoryStore) LoadConsumerOffsetState(consumer string, topicPartition TopicPartition) (*ConsumerOffsetState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := offsetKey(consumer, topicPartition)
	state, ok := s.offsetStates.Get(key)
	if ok {
		return &state, nil
	}
	offset, ok := s.offsets.Get(key)
	if !ok {
		var absent *ConsumerOffsetState
		return absent, nil
	}
	state = ConsumerOffsetState{Consumer: consumer, Topic: topicPartition.Topic, Partition: topicPartition.Partition, NextOffset: offset}
	return &state, nil
}

func (s *MemoryStore) ListConsumerOffsetStates() ([]ConsumerOffsetState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortConsumerOffsetStates(s.offsetStates.Values()), nil
}

func (s *MemoryStore) DeleteConsumerOffsetState(consumer string, topicPartition TopicPartition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := offsetKey(consumer, topicPartition)
	s.offsets.Delete(key)
	s.offsetStates.Delete(key)
	return nil
}

func validateConsumerOffsetState(state ConsumerOffsetState) error {
	if state.Consumer == "" {
		return E(CodeInvalidArgument, "consumer is required")
	}
	if state.Topic == "" {
		return E(CodeInvalidArgument, "topic is required")
	}
	return nil
}

func sortConsumerOffsetStates(states []ConsumerOffsetState) []ConsumerOffsetState {
	sorted := slices.Clone(states)
	slices.SortFunc(sorted, compareConsumerOffsetState)
	return sorted
}

func compareConsumerOffsetState(left, right ConsumerOffsetState) int {
	if value := cmp.Compare(left.Consumer, right.Consumer); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Topic, right.Topic); value != 0 {
		return value
	}
	return cmp.Compare(left.Partition, right.Partition)
}
