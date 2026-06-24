package store

import (
	"slices"
	"sort"
)

func (s *StorxLogStore) recordPointersFrom(tp TopicPartition, offset uint64, maxRecords int) []segmentRecordPointer {
	if maxRecords <= 0 {
		return nil
	}
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	pointers := s.records.GetOrDefault(tp, nil)
	start := segmentPointerSearch(pointers, offset)
	end := min(start+maxRecords, len(pointers))
	return cloneSegmentPointers(pointers[start:end])
}

func (s *StorxLogStore) recordPointersPage(
	tp TopicPartition,
	offset uint64,
	maxRecords int,
) ([]segmentRecordPointer, bool) {
	if maxRecords <= 0 {
		return nil, false
	}
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	pointers := s.records.GetOrDefault(tp, nil)
	start := segmentPointerSearch(pointers, offset)
	end := start + maxRecords
	hasMore := end < len(pointers)
	end = min(end, len(pointers))
	return cloneSegmentPointers(pointers[start:end]), hasMore
}

func (s *StorxLogStore) recordAppendedPointer(tp TopicPartition, pointer segmentRecordPointer, record Record) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	pointers := s.records.GetOrDefault(tp, nil)
	s.records.Set(tp, appendSegmentPointerSorted(pointers, pointer))
	timestampPointers := s.timestampRecords.GetOrDefault(tp, nil)
	s.timestampRecords.Set(tp, insertSegmentTimestampPointerSorted(timestampPointers, pointer))
	s.recordAppendedKeyPointersLocked(tp, []Record{record}, []segmentRecordPointer{pointer})
	if pointer.Offset >= s.nextOffsets.GetOrDefault(tp, 0) {
		s.nextOffsets.Set(tp, pointer.Offset+1)
	}
}

func (s *StorxLogStore) recordAppendedPointers(tp TopicPartition, pointers []segmentRecordPointer, records []Record) {
	if len(pointers) == 0 {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	existing := s.records.GetOrDefault(tp, nil)
	existing = append(existing, pointers...)
	sortSegmentPointers(existing)
	s.records.Set(tp, existing)
	timestamped := s.timestampRecords.GetOrDefault(tp, nil)
	for _, pointer := range pointers {
		timestamped = insertSegmentTimestampPointerSorted(timestamped, pointer)
	}
	s.timestampRecords.Set(tp, timestamped)
	s.recordAppendedKeyPointersLocked(tp, records, pointers)
	s.nextOffsets.Set(tp, nextOffsetFromPointers(existing))
}

func (s *StorxLogStore) removeRecordPointers(tp TopicPartition, remove interface{ Contains(uint64) bool }) []segmentRecordPointer {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	existing := s.records.GetOrDefault(tp, nil)
	nextOffset := s.nextOffsets.GetOrDefault(tp, nextOffsetFromPointers(existing))
	kept := make([]segmentRecordPointer, 0, len(existing))
	removed := make([]segmentRecordPointer, 0)
	for _, pointer := range existing {
		if remove.Contains(pointer.Offset) {
			removed = append(removed, pointer)
			continue
		}
		kept = append(kept, pointer)
	}
	s.records.Set(tp, kept)
	timestampExisting := s.timestampRecords.GetOrDefault(tp, nil)
	timestampKept := make([]segmentRecordPointer, 0, len(timestampExisting))
	for _, pointer := range timestampExisting {
		if remove.Contains(pointer.Offset) {
			continue
		}
		timestampKept = append(timestampKept, pointer)
	}
	s.timestampRecords.Set(tp, timestampKept)
	s.removeKeyPointersLocked(tp, remove)
	s.nextOffsets.Set(tp, max(nextOffset, nextOffsetFromPointers(kept)))
	return removed
}

func (s *StorxLogStore) removeKeyPointersLocked(tp TopicPartition, remove interface{ Contains(uint64) bool }) {
	if !s.keyIndexReady.Contains(tp) {
		return
	}
	keyPointers, ok := s.keyRecords.Get(tp)
	if !ok || keyPointers == nil {
		return
	}
	keys := make([]string, 0, keyPointers.Len())
	keyPointers.Range(func(key string, pointer segmentRecordPointer) bool {
		if remove.Contains(pointer.Offset) {
			keys = append(keys, key)
		}
		return true
	})
	for _, key := range keys {
		keyPointers.Delete(key)
	}
}

func segmentPointerSearch(pointers []segmentRecordPointer, offset uint64) int {
	return sort.Search(len(pointers), func(index int) bool {
		return pointers[index].Offset >= offset
	})
}

func appendSegmentPointerSorted(pointers []segmentRecordPointer, pointer segmentRecordPointer) []segmentRecordPointer {
	index := segmentPointerSearch(pointers, pointer.Offset)
	if index == len(pointers) {
		return append(pointers, pointer)
	}
	if pointers[index].Offset == pointer.Offset {
		pointers[index] = pointer
		return pointers
	}
	pointers = append(pointers, segmentRecordPointer{})
	copy(pointers[index+1:], pointers[index:])
	pointers[index] = pointer
	return pointers
}

func cloneSegmentPointers(pointers []segmentRecordPointer) []segmentRecordPointer {
	if len(pointers) == 0 {
		return nil
	}
	return slices.Clone(pointers)
}

func segmentPointerOffsets(pointers []segmentRecordPointer) []uint64 {
	if len(pointers) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(pointers))
	for _, pointer := range pointers {
		out = append(out, pointer.Offset)
	}
	return out
}
