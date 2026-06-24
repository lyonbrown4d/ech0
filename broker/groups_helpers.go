package broker

import (
	"cmp"
	"context"
	"slices"
	"strconv"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func groupAssignmentsEqual(left, right []store.GroupPartitionAssignment) bool {
	if len(left) != len(right) {
		return false
	}
	left = sortGroupAssignments(left)
	right = sortGroupAssignments(right)
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func groupAssignmentStable(previous, next []store.GroupPartitionAssignment, active []store.ConsumerGroupMember) bool {
	if !groupAssignmentsEqual(previous, next) {
		return false
	}
	return assignmentMembersMatchActive(previous, active) && assignmentMembersMatchActive(next, active)
}

func assignmentMembersMatchActive(assignments []store.GroupPartitionAssignment, active []store.ConsumerGroupMember) bool {
	members := collectionset.NewSetWithCapacity[string](len(assignments))
	for _, assignment := range assignments {
		members.Add(assignment.MemberID)
	}
	if members.Len() != len(active) {
		return false
	}
	for _, member := range active {
		if !members.Contains(member.MemberID) {
			return false
		}
	}
	return true
}

func memberLoads(assignments []store.GroupPartitionAssignment) []GroupMemberLoadSummary {
	loads := assignmentsByMember(assignments)
	members := collectionlist.NewList(loads.Keys()...).Sort(cmp.Compare).Values()
	if len(members) == 0 {
		return nil
	}
	return lo.Map(members, func(memberID string, _ int) GroupMemberLoadSummary {
		return GroupMemberLoadSummary{MemberID: memberID, Partitions: len(loads.Get(memberID))}
	})
}

func assignmentOwnerTable(assignments []store.GroupPartitionAssignment) *collectionmapping.Table[string, uint32, string] {
	owners := collectionmapping.NewTable[string, uint32, string]()
	for _, assignment := range assignments {
		owners.Put(assignment.Topic, assignment.Partition, assignment.MemberID)
	}
	return owners
}

func assignmentsByMember(assignments []store.GroupPartitionAssignment) *collectionmapping.MultiMap[string, store.GroupPartitionAssignment] {
	byMember := collectionmapping.NewMultiMapWithCapacity[string, store.GroupPartitionAssignment](len(assignments))
	for _, assignment := range assignments {
		byMember.Put(assignment.MemberID, assignment)
	}
	return byMember
}

func sortGroupAssignments(assignments []store.GroupPartitionAssignment) []store.GroupPartitionAssignment {
	return collectionlist.NewList(assignments...).
		Sort(compareGroupPartitionAssignment).Values()
}

func memberWantsTopic(member store.ConsumerGroupMember, topic string) bool {
	return slices.Contains(member.Topics, topic)
}

func normalizeGroupAssignmentStrategy(raw string) GroupAssignmentStrategy {
	switch GroupAssignmentStrategy(raw) {
	case GroupAssignmentRoundRobin:
		return GroupAssignmentRoundRobin
	case GroupAssignmentRange:
		return GroupAssignmentRange
	default:
		return GroupAssignmentRoundRobin
	}
}

func strconvUint32(value uint32) string {
	return strconv.FormatUint(uint64(value), 10)
}

func recordRebalanceMetrics(ctx context.Context, metrics *MetricsRuntime, plan groupRebalancePlan) {
	if metrics != nil {
		metrics.RecordRebalance(ctx, string(plan.Strategy), uint64(len(plan.Assignment.Assignments)), plan.MovedPartitions)
	}
}
