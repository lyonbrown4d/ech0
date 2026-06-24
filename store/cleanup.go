package store

import (
	"cmp"
	"slices"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type RetentionCleanupResult struct {
	RemovedRecords      int `json:"removed_records"`
	DeadLetteredRecords int `json:"dead_lettered_records,omitempty"`
}

type CompactionCleanupResult struct {
	CompactedPartitions int `json:"compacted_partitions"`
	RemovedRecords      int `json:"removed_records"`
}

func retentionRemovableOffsets(topic TopicConfig, records []Record, nowMS uint64) *collectionset.Set[uint64] {
	remove := collectionset.NewSet[uint64]()
	markExpiredMessages(records, nowMS, remove)
	if !retentionApplies(topic) {
		return remove
	}
	markExpiredRecords(topic, records, nowMS, remove)
	markOversizedRecords(topic, records, remove)
	return remove
}

func markExpiredMessages(records []Record, nowMS uint64, remove *collectionset.Set[uint64]) {
	for _, record := range records {
		if messageExpired(record, nowMS) {
			remove.Add(record.Offset)
		}
	}
}

func markExpiredRecords(topic TopicConfig, records []Record, nowMS uint64, remove *collectionset.Set[uint64]) {
	if topic.RetentionMS == nil {
		return
	}
	for _, record := range records {
		if recordExpired(record, *topic.RetentionMS, nowMS) {
			remove.Add(record.Offset)
		}
	}
}

func markOversizedRecords(topic TopicConfig, records []Record, remove *collectionset.Set[uint64]) {
	if topic.RetentionMaxBytes == 0 || len(records) == 0 {
		return
	}
	totalBytes, sizes := retainedRecordBytes(records, remove)
	if totalBytes <= topic.RetentionMaxBytes {
		return
	}
	removeOldestUntilWithinLimit(records, topic.RetentionMaxBytes, totalBytes, sizes, remove)
}

func retainedRecordBytes(records []Record, remove *collectionset.Set[uint64]) (uint64, *collectionmapping.Map[uint64, uint64]) {
	totalBytes := uint64(0)
	sizes := collectionmapping.NewMap[uint64, uint64]()
	for _, record := range records {
		size := recordStorageBytes(record)
		sizes.Set(record.Offset, size)
		if !remove.Contains(record.Offset) {
			totalBytes += size
		}
	}
	return totalBytes, sizes
}

func removeOldestUntilWithinLimit(
	records []Record,
	retentionMaxBytes uint64,
	totalBytes uint64,
	sizes *collectionmapping.Map[uint64, uint64],
	remove *collectionset.Set[uint64],
) {
	for _, record := range sortedRecords(records) {
		if remove.Contains(record.Offset) {
			continue
		}
		remove.Add(record.Offset)
		totalBytes = saturatingSub(totalBytes, sizes.GetOrDefault(record.Offset, 0))
		if totalBytes <= retentionMaxBytes {
			break
		}
	}
}

func compactionRemovableOffsets(topic TopicConfig, records []Record, nowMS uint64) *collectionset.Set[uint64] {
	remove := collectionset.NewSet[uint64]()
	if !compactionApplies(topic) {
		return remove
	}
	latestOffsetByKey := latestOffsetByRecordKey(records)
	markCompactedRecords(topic, records, nowMS, latestOffsetByKey, remove)
	return remove
}

func compactionRemovableOffsetsWithLatest(
	topic TopicConfig,
	records []Record,
	nowMS uint64,
	latestOffsetByKey *collectionmapping.Map[string, uint64],
) *collectionset.Set[uint64] {
	remove := collectionset.NewSet[uint64]()
	if !compactionApplies(topic) {
		return remove
	}
	markCompactedRecords(topic, records, nowMS, latestOffsetByKey, remove)
	return remove
}

func latestOffsetByRecordKey(records []Record) *collectionmapping.Map[string, uint64] {
	latestOffsetByKey := collectionmapping.NewMap[string, uint64]()
	for _, record := range records {
		if len(record.Key) > 0 {
			latestOffsetByKey.Set(string(record.Key), record.Offset)
		}
	}
	return latestOffsetByKey
}

func markCompactedRecords(
	topic TopicConfig,
	records []Record,
	nowMS uint64,
	latestOffsetByKey *collectionmapping.Map[string, uint64],
	remove *collectionset.Set[uint64],
) {
	for _, record := range records {
		if compactedRecordShouldRemove(topic, record, nowMS, latestOffsetByKey) {
			remove.Add(record.Offset)
		}
	}
}

func compactedRecordShouldRemove(
	topic TopicConfig,
	record Record,
	nowMS uint64,
	latestOffsetByKey *collectionmapping.Map[string, uint64],
) bool {
	if len(record.Key) == 0 {
		return false
	}
	latestOffset := latestOffsetByKey.GetOrDefault(string(record.Key), record.Offset)
	return latestOffset != record.Offset || compactedTombstoneExpired(topic, record, nowMS)
}

func retentionApplies(topic TopicConfig) bool {
	return topic.CleanupPolicy == TopicCleanupDelete || topic.CleanupPolicy == TopicCleanupCompactAndDelete
}

func compactionApplies(topic TopicConfig) bool {
	if !topic.CompactionEnabled {
		return false
	}
	return topic.CleanupPolicy == TopicCleanupCompact || topic.CleanupPolicy == TopicCleanupCompactAndDelete
}

func recordExpired(record Record, retentionMS, nowMS uint64) bool {
	if record.TimestampMS > nowMS {
		return false
	}
	return nowMS-record.TimestampMS >= retentionMS
}

func messageExpired(record Record, nowMS uint64) bool {
	return record.ExpiresAtMS != nil && *record.ExpiresAtMS <= nowMS
}

func compactedTombstoneExpired(topic TopicConfig, record Record, nowMS uint64) bool {
	if !record.IsTombstone() {
		return false
	}
	if topic.CompactionTombstoneRetentionMS == nil {
		return true
	}
	return recordExpired(record, *topic.CompactionTombstoneRetentionMS, nowMS)
}

func recordStorageBytes(record Record) uint64 {
	return RecordStorageBytes(record)
}

func RecordStorageBytes(record Record) uint64 {
	total := uint64(len(record.Payload) + len(record.Key) + 24)
	if record.ExpiresAtMS != nil {
		total += 9
	}
	for _, header := range record.Headers {
		total += uint64(len(header.Key) + len(header.Value) + 8)
	}
	return total
}

func RecordAppendStorageBytes(record RecordAppend) uint64 {
	total := uint64(len(record.Payload) + len(record.Key) + 24)
	if record.ExpiresAtMS != nil {
		total += 9
	}
	for _, header := range record.Headers {
		total += uint64(len(header.Key) + len(header.Value) + 8)
	}
	return total
}

func sortedRecords(records []Record) []Record {
	if len(records) == 0 {
		return nil
	}
	sorted := slices.Clone(records)
	slices.SortFunc(sorted, func(left, right Record) int {
		return cmp.Compare(left.Offset, right.Offset)
	})
	return sorted
}

func saturatingSub(v, other uint64) uint64 {
	if other >= v {
		return 0
	}
	return v - other
}
