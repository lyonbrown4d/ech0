package broker

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func buildCooperativeStickyAssignments(
	previous []store.GroupPartitionAssignment,
	active []store.ConsumerGroupMember,
	partitions []store.TopicPartition,
) (*collectionlist.List[store.GroupPartitionAssignment], uint64, uint64) {
	activeMembers := activeMemberMap(active)
	partitionSet := topicPartitionSet(partitions)
	assignments := retainedStickyAssignments(previous, activeMembers, partitionSet)
	assignMissingPartitions(assignments, active, partitions)
	balanceStickyAssignments(assignments, active)
	stickyCandidates := stickyCandidateCount(previous, activeMembers, partitionSet)
	stickyApplied := stickyAppliedCount(assignments.Values(), previous)
	return assignments.Sort(compareGroupPartitionAssignment), stickyCandidates, stickyApplied
}

func activeMemberMap(active []store.ConsumerGroupMember) *collectionmapping.Map[string, store.ConsumerGroupMember] {
	out := collectionmapping.NewMapWithCapacity[string, store.ConsumerGroupMember](len(active))
	for _, member := range active {
		out.Set(member.MemberID, member)
	}
	return out
}

func topicPartitionSet(partitions []store.TopicPartition) *collectionset.Set[store.TopicPartition] {
	return collectionset.NewSetWithCapacity[store.TopicPartition](len(partitions), partitions...)
}

func retainedStickyAssignments(
	previous []store.GroupPartitionAssignment,
	active *collectionmapping.Map[string, store.ConsumerGroupMember],
	partitions *collectionset.Set[store.TopicPartition],
) *collectionlist.List[store.GroupPartitionAssignment] {
	out := collectionlist.NewList[store.GroupPartitionAssignment]()
	seen := collectionset.NewSetWithCapacity[store.TopicPartition](len(previous))
	for _, assignment := range sortGroupAssignments(previous) {
		tp := store.NewTopicPartition(assignment.Topic, assignment.Partition)
		if seen.Contains(tp) || !partitions.Contains(tp) {
			continue
		}
		member, ok := active.Get(assignment.MemberID)
		if !ok || !memberWantsTopic(member, assignment.Topic) {
			continue
		}
		out.Add(assignment)
		seen.Add(tp)
	}
	return out
}

func assignMissingPartitions(assignments *collectionlist.List[store.GroupPartitionAssignment], active []store.ConsumerGroupMember, partitions []store.TopicPartition) {
	assigned := assignedTopicPartitions(assignments.Values())
	for _, partition := range partitions {
		if assigned.Contains(partition) {
			continue
		}
		owner, ok := leastLoadedEligibleMember(assignments.Values(), active, partition.Topic)
		if !ok {
			continue
		}
		assignments.Add(store.GroupPartitionAssignment{MemberID: owner.MemberID, Topic: partition.Topic, Partition: partition.Partition})
		assigned.Add(partition)
	}
}

func assignedTopicPartitions(assignments []store.GroupPartitionAssignment) *collectionset.Set[store.TopicPartition] {
	out := collectionset.NewSetWithCapacity[store.TopicPartition](len(assignments))
	for _, assignment := range assignments {
		out.Add(store.NewTopicPartition(assignment.Topic, assignment.Partition))
	}
	return out
}

func leastLoadedEligibleMember(assignments []store.GroupPartitionAssignment, active []store.ConsumerGroupMember, topic string) (store.ConsumerGroupMember, bool) {
	loads := groupAssignmentLoads(assignments)
	candidates := lo.Filter(active, func(member store.ConsumerGroupMember, _ int) bool {
		return memberWantsTopic(member, topic)
	})
	sorted := collectionlist.NewList(candidates...).Sort(func(left, right store.ConsumerGroupMember) int {
		leftLoad := len(loads.Get(left.MemberID))
		rightLoad := len(loads.Get(right.MemberID))
		if leftLoad == rightLoad {
			return cmp.Compare(left.MemberID, right.MemberID)
		}
		return cmp.Compare(leftLoad, rightLoad)
	}).Values()
	if len(sorted) == 0 {
		return store.ConsumerGroupMember{}, false
	}
	return sorted[0], true
}

func balanceStickyAssignments(assignments *collectionlist.List[store.GroupPartitionAssignment], active []store.ConsumerGroupMember) {
	if assignments.Len() == 0 || len(active) == 0 {
		return
	}
	minLoad := assignments.Len() / len(active)
	maxLoad := minLoad
	if assignments.Len()%len(active) != 0 {
		maxLoad++
	}
	for {
		moved := moveOneStickyAssignment(assignments, active, minLoad, maxLoad)
		if !moved {
			return
		}
	}
}

func moveOneStickyAssignment(assignments *collectionlist.List[store.GroupPartitionAssignment], active []store.ConsumerGroupMember, minLoad, maxLoad int) bool {
	loads := groupAssignmentLoads(assignments.Values())
	targets := underloadedMembers(loads, active, minLoad)
	if len(targets) == 0 {
		return false
	}
	for _, target := range targets {
		index, ok := movableAssignmentIndex(assignments.Values(), loads, target, maxLoad)
		if !ok {
			continue
		}
		assignment, ok := assignments.Get(index)
		if !ok {
			continue
		}
		assignments.Set(index, store.GroupPartitionAssignment{
			MemberID:  target.MemberID,
			Topic:     assignment.Topic,
			Partition: assignment.Partition,
		})
		return true
	}
	return false
}

func groupAssignmentLoads(assignments []store.GroupPartitionAssignment) *collectionmapping.MultiMap[string, store.GroupPartitionAssignment] {
	loads := collectionmapping.NewMultiMap[string, store.GroupPartitionAssignment]()
	for _, assignment := range assignments {
		loads.Put(assignment.MemberID, assignment)
	}
	return loads
}

func underloadedMembers(
	loads *collectionmapping.MultiMap[string, store.GroupPartitionAssignment],
	active []store.ConsumerGroupMember,
	minLoad int,
) []store.ConsumerGroupMember {
	underloaded := lo.Filter(active, func(member store.ConsumerGroupMember, _ int) bool {
		return len(loads.Get(member.MemberID)) < minLoad
	})
	return collectionlist.NewList(underloaded...).Sort(func(left, right store.ConsumerGroupMember) int {
		leftLoad := len(loads.Get(left.MemberID))
		rightLoad := len(loads.Get(right.MemberID))
		if leftLoad == rightLoad {
			return cmp.Compare(left.MemberID, right.MemberID)
		}
		return cmp.Compare(leftLoad, rightLoad)
	}).Values()
}

func movableAssignmentIndex(
	assignments []store.GroupPartitionAssignment,
	loads *collectionmapping.MultiMap[string, store.GroupPartitionAssignment],
	target store.ConsumerGroupMember,
	maxLoad int,
) (int, bool) {
	indexes := collectionlist.NewList[int]()
	for index, assignment := range assignments {
		if assignment.MemberID == target.MemberID || len(loads.Get(assignment.MemberID)) <= maxLoad || !memberWantsTopic(target, assignment.Topic) {
			continue
		}
		indexes.Add(index)
	}
	sorted := indexes.Sort(func(left, right int) int {
		leftItem := assignments[left]
		rightItem := assignments[right]
		leftLoad := len(loads.Get(leftItem.MemberID))
		rightLoad := len(loads.Get(rightItem.MemberID))
		if leftLoad != rightLoad {
			return cmp.Compare(rightLoad, leftLoad)
		}
		return compareGroupPartitionAssignment(rightItem, leftItem)
	}).Values()
	if len(sorted) == 0 {
		return 0, false
	}
	return sorted[0], true
}

func stickyCandidateCount(
	previous []store.GroupPartitionAssignment,
	active *collectionmapping.Map[string, store.ConsumerGroupMember],
	partitions *collectionset.Set[store.TopicPartition],
) uint64 {
	var count uint64
	for _, assignment := range previous {
		member, ok := active.Get(assignment.MemberID)
		tp := store.NewTopicPartition(assignment.Topic, assignment.Partition)
		if ok && partitions.Contains(tp) && memberWantsTopic(member, assignment.Topic) {
			count++
		}
	}
	return count
}

func stickyAppliedCount(assignments, previous []store.GroupPartitionAssignment) uint64 {
	previousOwners := assignmentOwnerTable(previous)
	var count uint64
	for _, assignment := range assignments {
		owner, ok := previousOwners.Get(assignment.Topic, assignment.Partition)
		if ok && owner == assignment.MemberID {
			count++
		}
	}
	return count
}

func compareGroupPartitionAssignment(left, right store.GroupPartitionAssignment) int {
	if left.Topic == right.Topic {
		if left.Partition == right.Partition {
			return cmp.Compare(left.MemberID, right.MemberID)
		}
		return cmp.Compare(left.Partition, right.Partition)
	}
	return cmp.Compare(left.Topic, right.Topic)
}
