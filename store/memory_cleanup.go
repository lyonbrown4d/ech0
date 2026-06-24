package store

import (
	"context"

	collectionset "github.com/arcgolabs/collectionx/set"
)

func (s *MemoryStore) EnforceRetention(ctx context.Context, nowMS uint64) (RetentionCleanupResult, error) {
	if err := ctx.Err(); err != nil {
		return RetentionCleanupResult{}, wrapExternal(err, "enforce memory retention")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RetentionCleanupResult{}
	topics := s.topics.Values()
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			tp := NewTopicPartition(topic.Name, partition)
			records := s.records.GetOrDefault(tp, nil)
			remove := retentionRemovableOffsets(topic, records, nowMS)
			if remove.IsEmpty() {
				continue
			}
			kept, removed := filterRemovedRecords(records, remove)
			s.records.Set(tp, kept)
			result.RemovedRecords += removed
		}
	}
	return result, nil
}

func (s *MemoryStore) Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error) {
	_ = sealedSegmentBatch
	if err := ctx.Err(); err != nil {
		return CompactionCleanupResult{}, wrapExternal(err, "compact memory records")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := CompactionCleanupResult{}
	topics := s.topics.Values()
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			tp := NewTopicPartition(topic.Name, partition)
			records := s.records.GetOrDefault(tp, nil)
			remove := compactionRemovableOffsets(topic, records, nowMS)
			if remove.IsEmpty() {
				continue
			}
			kept, removed := filterRemovedRecords(records, remove)
			s.records.Set(tp, kept)
			result.CompactedPartitions++
			result.RemovedRecords += removed
		}
	}
	return result, nil
}

func filterRemovedRecords(records []Record, remove *collectionset.Set[uint64]) ([]Record, int) {
	kept := make([]Record, 0, len(records))
	removed := 0
	for _, record := range records {
		if remove.Contains(record.Offset) {
			removed++
			continue
		}
		kept = append(kept, cloneRecord(record))
	}
	return kept, removed
}
