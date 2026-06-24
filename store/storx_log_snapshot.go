package store

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *StorxLogStore) Snapshot() (Snapshot, error) {
	topics := s.snapshotTopics()
	nextOffsets := s.snapshotLogOffsets()
	records, err := s.snapshotRecords()
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Topics: topics, Records: *records, LogOffsets: *nextOffsets}, nil
}

func (s *StorxLogStore) snapshotTopics() collectionlist.List[TopicConfig] {
	s.indexMu.RLock()
	topics := collectionlist.NewListWithCapacity[TopicConfig](s.topics.Len())
	s.topics.Range(func(_ string, topic TopicConfig) bool {
		topics.Add(cloneTopic(topic))
		return true
	})
	s.indexMu.RUnlock()
	return *topics.
		Sort(func(left, right TopicConfig) int {
			return cmp.Compare(left.Name, right.Name)
		})
}

func (s *StorxLogStore) snapshotLogOffsets() *collectionmapping.Map[string, uint64] {
	s.indexMu.RLock()
	nextOffsets := collectionmapping.NewMap[string, uint64]()
	s.nextOffsets.Range(func(tp TopicPartition, value uint64) bool {
		nextOffsets.Set(partitionKey(tp), value)
		return true
	})
	s.indexMu.RUnlock()
	return nextOffsets
}

func (s *StorxLogStore) snapshotRecords() (*collectionmapping.Map[string, []Record], error) {
	records := collectionmapping.NewMap[string, []Record]()
	pointers := s.snapshotPointers()
	if pointers == nil {
		return records, nil
	}
	var snapshotErr error
	pointers.Range(func(tp TopicPartition, topicPointers []segmentRecordPointer) bool {
		if err := s.snapshotPartitionRecords(tp, topicPointers, records); err != nil {
			snapshotErr = err
			return false
		}
		return true
	})
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	records.Range(func(key string, topicRecords []Record) bool {
		records.Set(key, sortedRecords(topicRecords))
		return true
	})
	return records, nil
}

func (s *StorxLogStore) snapshotPointers() *collectionmapping.Map[TopicPartition, []segmentRecordPointer] {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	out := collectionmapping.NewMapWithCapacity[TopicPartition, []segmentRecordPointer](s.records.Len())
	s.records.Range(func(tp TopicPartition, pointers []segmentRecordPointer) bool {
		out.Set(tp, cloneSegmentPointers(pointers))
		return true
	})
	return out
}

func (s *StorxLogStore) snapshotPartitionRecords(
	tp TopicPartition,
	pointers []segmentRecordPointer,
	records *collectionmapping.Map[string, []Record],
) error {
	if len(pointers) == 0 {
		return nil
	}
	partitionRecords, err := s.readPointers(pointers)
	if err != nil {
		return err
	}
	if len(partitionRecords) == 0 {
		return nil
	}
	key := partitionKey(tp)
	records.Set(key, append(records.GetOrDefault(key, nil), cloneRecords(partitionRecords)...))
	return nil
}
