package store

import (
	"cmp"
	"slices"
)

func (s *MemoryStore) SaveConsumerPause(state ConsumerPauseState) error {
	if err := validateConsumerPauseState(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumerPauses.Set(consumerPauseKey(state.Consumer, state.TopicPartition()), state)
	return nil
}

func (s *MemoryStore) LoadConsumerPause(consumer string, topicPartition TopicPartition) (*ConsumerPauseState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.consumerPauses.Get(consumerPauseKey(consumer, topicPartition))
	if !ok {
		var absent *ConsumerPauseState
		return absent, nil
	}
	return &state, nil
}

func (s *MemoryStore) ListConsumerPauses() ([]ConsumerPauseState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortConsumerPauses(s.consumerPauses.Values()), nil
}

func (s *MemoryStore) DeleteConsumerPause(consumer string, topicPartition TopicPartition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumerPauses.Delete(consumerPauseKey(consumer, topicPartition))
	return nil
}

func validateConsumerPauseState(state ConsumerPauseState) error {
	if state.Consumer == "" {
		return E(CodeInvalidArgument, "consumer is required")
	}
	if state.Topic == "" {
		return E(CodeInvalidArgument, "topic is required")
	}
	return nil
}

func (s ConsumerPauseState) TopicPartition() TopicPartition {
	return NewTopicPartition(s.Topic, s.Partition)
}

func sortConsumerPauses(states []ConsumerPauseState) []ConsumerPauseState {
	sorted := slices.Clone(states)
	slices.SortFunc(sorted, compareConsumerPauseState)
	return sorted
}

func compareConsumerPauseState(left, right ConsumerPauseState) int {
	if value := cmp.Compare(left.Consumer, right.Consumer); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Topic, right.Topic); value != 0 {
		return value
	}
	return cmp.Compare(left.Partition, right.Partition)
}
