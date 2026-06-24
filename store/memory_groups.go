package store

import (
	"cmp"
	"slices"
)

func (s *MemoryStore) SaveGroupMember(member ConsumerGroupMember) error {
	if member.Group == "" || member.MemberID == "" {
		return E(CodeInvalidArgument, "group and member_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	member.Topics = sortedStrings(member.Topics)
	s.members.Set(groupMemberKey(member.Group, member.MemberID), member)
	return nil
}

func (s *MemoryStore) LoadGroupMember(group, memberID string) (*ConsumerGroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	member, ok := s.members.Get(groupMemberKey(group, memberID))
	if !ok {
		var absent *ConsumerGroupMember
		return absent, nil
	}
	member.Topics = slices.Clone(member.Topics)
	return &member, nil
}

func (s *MemoryStore) ListGroupMembers(group string) ([]ConsumerGroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsumerGroupMember, 0, s.members.Len())
	s.members.Range(func(_ string, member ConsumerGroupMember) bool {
		if member.Group != group {
			return true
		}
		member.Topics = sortedStrings(member.Topics)
		out = append(out, member)
		return true
	})
	slices.SortFunc(out, func(left, right ConsumerGroupMember) int {
		return cmp.Compare(left.MemberID, right.MemberID)
	})
	return out, nil
}

func (s *MemoryStore) DeleteGroupMember(group, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members.Delete(groupMemberKey(group, memberID))
	return nil
}

func (s *MemoryStore) DeleteExpiredGroupMembers(nowMS uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	s.members.Range(func(key string, member ConsumerGroupMember) bool {
		if member.ExpiredAt(nowMS) {
			s.members.Delete(key)
			deleted++
		}
		return true
	})
	return deleted, nil
}

func (s *MemoryStore) SaveGroupAssignment(assignment ConsumerGroupAssignment) error {
	if assignment.Group == "" {
		return E(CodeInvalidArgument, "group is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
	s.assignments.Set(assignment.Group, assignment)
	return nil
}

func (s *MemoryStore) LoadGroupAssignment(group string) (*ConsumerGroupAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	assignment, ok := s.assignments.Get(group)
	if !ok {
		var absent *ConsumerGroupAssignment
		return absent, nil
	}
	assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
	return &assignment, nil
}

func (s *MemoryStore) ListGroupAssignments() ([]ConsumerGroupAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsumerGroupAssignment, 0, s.assignments.Len())
	s.assignments.Range(func(_ string, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		out = append(out, assignment)
		return true
	})
	slices.SortFunc(out, func(left, right ConsumerGroupAssignment) int {
		return cmp.Compare(left.Group, right.Group)
	})
	return out, nil
}

func (s *MemoryStore) SaveBrokerState(state BrokerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := state
	s.brokerState = &cp
	return nil
}

func (s *MemoryStore) LoadBrokerState() (*BrokerState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.brokerState == nil {
		var absent *BrokerState
		return absent, nil
	}
	cp := *s.brokerState
	return &cp, nil
}

func sortedStrings(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}
