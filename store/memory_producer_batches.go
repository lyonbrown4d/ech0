package store

import (
	"cmp"
	"slices"
)

func (s *MemoryStore) SaveProducerBatch(batch ProducerPublishedBatch) error {
	if err := validateProducerBatch(batch); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.producerBatches.Set(producerBatchKey(batch), cloneProducerPublishedBatch(batch))
	return nil
}

func (s *MemoryStore) ListProducerBatches(filter ProducerBatchFilter) ([]ProducerPublishedBatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ProducerPublishedBatch, 0, s.producerBatches.Len())
	s.producerBatches.Range(func(_ string, batch ProducerPublishedBatch) bool {
		if producerBatchMatchesFilter(batch, filter) {
			out = append(out, cloneProducerPublishedBatch(batch))
		}
		return true
	})
	return sortProducerPublishedBatches(out), nil
}

func (s *MemoryStore) DeleteProducerBatch(batch ProducerPublishedBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.producerBatches.Delete(producerBatchKey(batch))
	return nil
}

func validateProducerBatch(batch ProducerPublishedBatch) error {
	if batch.ProducerID == 0 {
		return E(CodeInvalidArgument, "producer_id is required")
	}
	if batch.Topic == "" {
		return E(CodeInvalidArgument, "producer batch topic is required")
	}
	if batch.RecordCount == 0 {
		return E(CodeInvalidArgument, "producer batch record_count is required")
	}
	if batch.LastOffset < batch.BaseOffset {
		return E(CodeInvalidArgument, "producer batch offsets are invalid")
	}
	if batch.LastOffset == ^uint64(0) {
		return E(CodeInvalidArgument, "producer batch last_offset overflows next_offset")
	}
	if batch.NextOffset != batch.LastOffset+1 {
		return E(CodeInvalidArgument, "producer batch next_offset is invalid")
	}
	return nil
}

func producerBatchMatchesFilter(batch ProducerPublishedBatch, filter ProducerBatchFilter) bool {
	if filter.ProducerID != 0 && batch.ProducerID != filter.ProducerID {
		return false
	}
	if filter.Topic != "" && batch.Topic != filter.Topic {
		return false
	}
	if filter.EpochSet && batch.ProducerEpoch != filter.ProducerEpoch {
		return false
	}
	if filter.PartitionSet && batch.Partition != filter.Partition {
		return false
	}
	return true
}

func sortProducerPublishedBatches(batches []ProducerPublishedBatch) []ProducerPublishedBatch {
	sorted := slices.Clone(batches)
	slices.SortFunc(sorted, func(left, right ProducerPublishedBatch) int {
		if left.ProducerID != right.ProducerID {
			return cmp.Compare(left.ProducerID, right.ProducerID)
		}
		if left.Topic != right.Topic {
			return cmp.Compare(left.Topic, right.Topic)
		}
		if left.Partition != right.Partition {
			return cmp.Compare(left.Partition, right.Partition)
		}
		if left.ProducerEpoch != right.ProducerEpoch {
			return cmp.Compare(left.ProducerEpoch, right.ProducerEpoch)
		}
		return cmp.Compare(left.BaseSequence, right.BaseSequence)
	})
	return sorted
}
