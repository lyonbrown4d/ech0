package store

import "slices"

func cloneRecords(records []Record) []Record {
	if len(records) == 0 {
		return nil
	}
	copied := make([]Record, len(records))
	for i := range records {
		copied[i] = cloneRecord(records[i])
	}
	return copied
}

func cloneGroupPartitionAssignments(assignments []GroupPartitionAssignment) []GroupPartitionAssignment {
	if len(assignments) == 0 {
		return nil
	}
	return slices.Clone(assignments)
}
