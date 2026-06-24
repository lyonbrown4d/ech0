package store

import (
	"cmp"
	"fmt"
	"slices"
)

type DLQIndexEntry struct {
	DLQTopic          string `json:"dlq_topic"`
	DLQPartition      uint32 `json:"dlq_partition"`
	DLQOffset         uint64 `json:"dlq_offset"`
	TimestampMS       uint64 `json:"timestamp_ms"`
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition uint32 `json:"original_partition"`
	OriginalOffset    uint64 `json:"original_offset"`
	ErrorCode         string `json:"error_code"`
	RetryCount        uint32 `json:"retry_count"`
}

type DLQIndexFilter struct {
	DLQTopic          string
	DLQPartition      *uint32
	MinDLQOffset      *uint64
	MaxDLQOffset      *uint64
	FromTimestampMS   *uint64
	ToTimestampMS     *uint64
	OriginalTopic     *string
	OriginalPartition *uint32
	OriginalOffset    *uint64
	ErrorCode         string
	RetryCount        *uint32
}

type DLQIndexStore interface {
	SaveDLQIndex(entry DLQIndexEntry) error
	LoadDLQIndex(dlqTopic string, dlqPartition uint32, dlqOffset uint64) (*DLQIndexEntry, error)
	ListDLQIndexes(filter DLQIndexFilter) ([]DLQIndexEntry, error)
}

func dlqIndexKey(dlqTopic string, dlqPartition uint32, dlqOffset uint64) string {
	return fmt.Sprintf("%s\x00%d\x00%d", dlqTopic, dlqPartition, dlqOffset)
}

func cloneDLQIndexEntry(entry DLQIndexEntry) DLQIndexEntry {
	return entry
}

func dlqIndexMatchesFilter(entry DLQIndexEntry, filter DLQIndexFilter) bool {
	return dlqIndexMatchesDLQ(entry, filter) &&
		dlqIndexMatchesTime(entry, filter) &&
		dlqIndexMatchesOrigin(entry, filter) &&
		dlqIndexMatchesError(entry, filter)
}

func dlqIndexMatchesDLQ(entry DLQIndexEntry, filter DLQIndexFilter) bool {
	if filter.DLQTopic != "" && entry.DLQTopic != filter.DLQTopic {
		return false
	}
	if filter.DLQPartition != nil && entry.DLQPartition != *filter.DLQPartition {
		return false
	}
	if filter.MinDLQOffset != nil && entry.DLQOffset < *filter.MinDLQOffset {
		return false
	}
	return filter.MaxDLQOffset == nil || entry.DLQOffset <= *filter.MaxDLQOffset
}

func dlqIndexMatchesTime(entry DLQIndexEntry, filter DLQIndexFilter) bool {
	if filter.FromTimestampMS != nil && entry.TimestampMS < *filter.FromTimestampMS {
		return false
	}
	return filter.ToTimestampMS == nil || entry.TimestampMS <= *filter.ToTimestampMS
}

func dlqIndexMatchesOrigin(entry DLQIndexEntry, filter DLQIndexFilter) bool {
	if filter.OriginalTopic != nil && entry.OriginalTopic != *filter.OriginalTopic {
		return false
	}
	if filter.OriginalPartition != nil && entry.OriginalPartition != *filter.OriginalPartition {
		return false
	}
	return filter.OriginalOffset == nil || entry.OriginalOffset == *filter.OriginalOffset
}

func dlqIndexMatchesError(entry DLQIndexEntry, filter DLQIndexFilter) bool {
	if filter.ErrorCode != "" && entry.ErrorCode != filter.ErrorCode {
		return false
	}
	return filter.RetryCount == nil || entry.RetryCount == *filter.RetryCount
}

func sortDLQIndexes(entries []DLQIndexEntry) []DLQIndexEntry {
	if len(entries) == 0 {
		return nil
	}
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(left, right DLQIndexEntry) int {
		if left.DLQTopic != right.DLQTopic {
			return cmp.Compare(left.DLQTopic, right.DLQTopic)
		}
		if left.DLQPartition != right.DLQPartition {
			return cmp.Compare(left.DLQPartition, right.DLQPartition)
		}
		return cmp.Compare(left.DLQOffset, right.DLQOffset)
	})
	return sorted
}
