package store

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type Snapshot struct {
	Topics            collectionlist.List[TopicConfig]             `json:"topics"`
	Records           collectionmapping.Map[string, []Record]      `json:"records"`
	LogOffsets        collectionmapping.Map[string, uint64]        `json:"log_offsets"`
	Offsets           collectionmapping.Map[string, uint64]        `json:"offsets"`
	OffsetStates      collectionlist.List[ConsumerOffsetState]     `json:"offset_states"`
	ConsumerPauses    collectionlist.List[ConsumerPauseState]      `json:"consumer_pauses"`
	Placements        collectionlist.List[ShardPlacement]          `json:"shard_placements"`
	Members           collectionlist.List[ConsumerGroupMember]     `json:"members"`
	Assignments       collectionlist.List[ConsumerGroupAssignment] `json:"assignments"`
	Transactions      collectionlist.List[TransactionState]        `json:"transactions"`
	ProducerBatches   collectionlist.List[ProducerPublishedBatch]  `json:"producer_batches"`
	ACLPolicies       collectionlist.List[ACLPolicy]               `json:"acl_policies"`
	DLQIndexes        collectionlist.List[DLQIndexEntry]           `json:"dlq_indexes"`
	NextTransactionID uint64                                       `json:"next_transaction_id,omitempty"`
	BrokerState       *BrokerState                                 `json:"broker_state,omitempty"`
}

func (s *MemoryStore) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		Topics:            *collectionlist.NewListWithCapacity[TopicConfig](s.topics.Len()),
		Records:           *collectionmapping.NewMapWithCapacity[string, []Record](s.records.Len()),
		LogOffsets:        *collectionmapping.NewMapWithCapacity[string, uint64](s.nextOffsets.Len()),
		Offsets:           *collectionmapping.NewMapWithCapacity[string, uint64](s.offsets.Len()),
		OffsetStates:      *collectionlist.NewListWithCapacity[ConsumerOffsetState](s.offsetStates.Len()),
		ConsumerPauses:    *collectionlist.NewListWithCapacity[ConsumerPauseState](s.consumerPauses.Len()),
		Placements:        *collectionlist.NewListWithCapacity[ShardPlacement](s.placements.Len()),
		Members:           *collectionlist.NewListWithCapacity[ConsumerGroupMember](s.members.Len()),
		Assignments:       *collectionlist.NewListWithCapacity[ConsumerGroupAssignment](s.assignments.Len()),
		Transactions:      *collectionlist.NewListWithCapacity[TransactionState](s.transactions.Len()),
		ProducerBatches:   *collectionlist.NewListWithCapacity[ProducerPublishedBatch](s.producerBatches.Len()),
		ACLPolicies:       *collectionlist.NewListWithCapacity[ACLPolicy](s.aclPolicies.Len()),
		DLQIndexes:        *collectionlist.NewListWithCapacity[DLQIndexEntry](s.dlqIndexes.Len()),
		NextTransactionID: s.nextTxID,
	}
	topics := s.topics.Values()
	for i := range topics {
		topic := topics[i]
		snap.Topics.Add(cloneTopic(topic))
	}
	s.records.Range(func(tp TopicPartition, records []Record) bool {
		snap.Records.Set(partitionKey(tp), cloneRecords(records))
		return true
	})
	s.nextOffsets.Range(func(tp TopicPartition, value uint64) bool {
		snap.LogOffsets.Set(partitionKey(tp), value)
		return true
	})
	s.offsets.Range(func(key string, value uint64) bool {
		snap.Offsets.Set(key, value)
		return true
	})
	for _, state := range sortConsumerOffsetStates(s.offsetStates.Values()) {
		snap.OffsetStates.Add(state)
	}
	for _, state := range sortConsumerPauses(s.consumerPauses.Values()) {
		snap.ConsumerPauses.Add(state)
	}
	placements := make([]ShardPlacement, 0, s.placements.Len())
	s.placements.Range(func(_ TopicPartition, placement ShardPlacement) bool {
		placements = append(placements, cloneShardPlacement(placement))
		return true
	})
	for _, placement := range sortShardPlacements(placements) {
		snap.Placements.Add(placement)
	}
	s.members.Range(func(_ string, member ConsumerGroupMember) bool {
		member.Topics = sortedStrings(member.Topics)
		snap.Members.Add(member)
		return true
	})
	s.assignments.Range(func(_ string, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		snap.Assignments.Add(assignment)
		return true
	})
	s.transactions.Range(func(_ uint64, state TransactionState) bool {
		snap.Transactions.Add(cloneTransactionState(state))
		return true
	})
	for _, batch := range sortProducerPublishedBatches(s.producerBatches.Values()) {
		snap.ProducerBatches.Add(cloneProducerPublishedBatch(batch))
	}
	aclPolicies := sortACLPolicies(s.aclPolicies.Values())
	for i := range aclPolicies {
		snap.ACLPolicies.Add(cloneACLPolicy(aclPolicies[i]))
	}
	for _, entry := range sortDLQIndexes(s.dlqIndexes.Values()) {
		snap.DLQIndexes.Add(cloneDLQIndexEntry(entry))
	}
	if s.brokerState != nil {
		cp := *s.brokerState
		snap.BrokerState = &cp
	}
	return snap, nil
}

func (s *MemoryStore) Restore(snapshot Snapshot) error {
	state, err := buildMemoryRestoreState(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = state.topics
	s.topicNames = state.topicNames
	s.records = state.records
	s.nextOffsets = state.nextOffsets
	s.offsets = state.offsets
	s.offsetStates = state.offsetStates
	s.consumerPauses = state.consumerPauses
	s.placements = state.placements
	s.members = state.members
	s.assignments = state.assignments
	s.transactions = state.transactions
	s.producerBatches = state.producerBatches
	s.aclPolicies = state.aclPolicies
	s.dlqIndexes = state.dlqIndexes
	s.nextTxID = state.nextTxID
	s.brokerState = state.brokerState
	return nil
}

type memoryRestoreState struct {
	topics          *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames      *collectionset.Set[string]
	records         *collectionmapping.Map[TopicPartition, []Record]
	nextOffsets     *collectionmapping.Map[TopicPartition, uint64]
	offsets         *collectionmapping.Map[string, uint64]
	offsetStates    *collectionmapping.Map[string, ConsumerOffsetState]
	consumerPauses  *collectionmapping.Map[string, ConsumerPauseState]
	placements      *collectionmapping.Map[TopicPartition, ShardPlacement]
	members         *collectionmapping.Map[string, ConsumerGroupMember]
	assignments     *collectionmapping.Map[string, ConsumerGroupAssignment]
	transactions    *collectionmapping.Map[uint64, TransactionState]
	producerBatches *collectionmapping.Map[string, ProducerPublishedBatch]
	aclPolicies     *collectionmapping.Map[string, ACLPolicy]
	dlqIndexes      *collectionmapping.Map[string, DLQIndexEntry]
	nextTxID        uint64
	brokerState     *BrokerState
}

func buildMemoryRestoreState(snapshot Snapshot) (memoryRestoreState, error) {
	topics := collectionmapping.NewOrderedMap[string, TopicConfig]()
	topicNames := collectionset.NewSet[string]()
	records := collectionmapping.NewMap[TopicPartition, []Record]()
	restoreMemoryTopics(snapshot.Topics, topics, topicNames, records)
	if err := restoreMemoryRecords(snapshot.Records, records); err != nil {
		return memoryRestoreState{}, err
	}
	return memoryRestoreState{
		topics:          topics,
		topicNames:      topicNames,
		records:         records,
		nextOffsets:     restoreMemoryLogOffsets(snapshot.LogOffsets, records),
		offsets:         restoreMemoryOffsets(snapshot.Offsets),
		offsetStates:    restoreMemoryOffsetStates(snapshot.OffsetStates, snapshot.Offsets),
		consumerPauses:  restoreMemoryConsumerPauses(snapshot.ConsumerPauses),
		placements:      restoreMemoryShardPlacements(snapshot.Placements),
		members:         restoreMemoryMembers(snapshot.Members),
		assignments:     restoreMemoryAssignments(snapshot.Assignments),
		transactions:    restoreMemoryTransactions(snapshot.Transactions),
		producerBatches: restoreMemoryProducerBatches(snapshot.ProducerBatches),
		aclPolicies:     restoreMemoryACLPolicies(snapshot.ACLPolicies),
		dlqIndexes:      restoreMemoryDLQIndexes(snapshot.DLQIndexes),
		nextTxID:        restoreMemoryNextTransactionID(snapshot),
		brokerState:     cloneBrokerState(snapshot.BrokerState),
	}, nil
}

func restoreMemoryLogOffsets(snapshotOffsets collectionmapping.Map[string, uint64], records *collectionmapping.Map[TopicPartition, []Record]) *collectionmapping.Map[TopicPartition, uint64] {
	nextOffsets := collectionmapping.NewMap[TopicPartition, uint64]()
	records.Range(func(tp TopicPartition, topicRecords []Record) bool {
		nextOffsets.Set(tp, nextOffsetFromRecords(topicRecords))
		return true
	})
	snapshotOffsets.Range(func(key string, value uint64) bool {
		tp, err := parsePartitionKey(key)
		if err == nil {
			nextOffsets.Set(tp, value)
		}
		return true
	})
	return nextOffsets
}

func restoreMemoryTopics(topics collectionlist.List[TopicConfig], out *collectionmapping.OrderedMap[string, TopicConfig], names *collectionset.Set[string], records *collectionmapping.Map[TopicPartition, []Record]) {
	topics.Range(func(_ int, topic TopicConfig) bool {
		normalizeTopic(&topic)
		out.Set(topic.Name, cloneTopic(topic))
		names.Add(topic.Name)
		for partition := range topic.Partitions {
			records.Set(NewTopicPartition(topic.Name, partition), nil)
		}
		return true
	})
}

func restoreMemoryRecords(snapshotRecords collectionmapping.Map[string, []Record], records *collectionmapping.Map[TopicPartition, []Record]) error {
	var resultErr error
	snapshotRecords.Range(func(key string, topicRecords []Record) bool {
		tp, err := parsePartitionKey(key)
		if err != nil {
			resultErr = err
			return false
		}
		records.Set(tp, cloneRecords(topicRecords))
		return true
	})
	return resultErr
}

func restoreMemoryOffsets(snapshotOffsets collectionmapping.Map[string, uint64]) *collectionmapping.Map[string, uint64] {
	offsets := collectionmapping.NewMapWithCapacity[string, uint64](snapshotOffsets.Len())
	snapshotOffsets.Range(func(key string, value uint64) bool {
		offsets.Set(key, value)
		return true
	})
	return offsets
}

func restoreMemoryConsumerPauses(snapshotPauses collectionlist.List[ConsumerPauseState]) *collectionmapping.Map[string, ConsumerPauseState] {
	pauses := collectionmapping.NewMapWithCapacity[string, ConsumerPauseState](snapshotPauses.Len())
	snapshotPauses.Range(func(_ int, state ConsumerPauseState) bool {
		if validateConsumerPauseState(state) == nil && state.Paused {
			pauses.Set(consumerPauseKey(state.Consumer, state.TopicPartition()), state)
		}
		return true
	})
	return pauses
}

func restoreMemoryShardPlacements(snapshotPlacements collectionlist.List[ShardPlacement]) *collectionmapping.Map[TopicPartition, ShardPlacement] {
	placements := collectionmapping.NewMapWithCapacity[TopicPartition, ShardPlacement](snapshotPlacements.Len())
	snapshotPlacements.Range(func(_ int, placement ShardPlacement) bool {
		if validateShardPlacement(placement) == nil {
			placements.Set(placement.TopicPartition(), cloneShardPlacement(placement))
		}
		return true
	})
	return placements
}

func restoreMemoryMembers(snapshotMembers collectionlist.List[ConsumerGroupMember]) *collectionmapping.Map[string, ConsumerGroupMember] {
	members := collectionmapping.NewMapWithCapacity[string, ConsumerGroupMember](snapshotMembers.Len())
	snapshotMembers.Range(func(_ int, member ConsumerGroupMember) bool {
		member.Topics = sortedStrings(member.Topics)
		members.Set(groupMemberKey(member.Group, member.MemberID), member)
		return true
	})
	return members
}

func restoreMemoryAssignments(snapshotAssignments collectionlist.List[ConsumerGroupAssignment]) *collectionmapping.Map[string, ConsumerGroupAssignment] {
	assignments := collectionmapping.NewMapWithCapacity[string, ConsumerGroupAssignment](snapshotAssignments.Len())
	snapshotAssignments.Range(func(_ int, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		assignments.Set(assignment.Group, assignment)
		return true
	})
	return assignments
}

func restoreMemoryTransactions(snapshotTransactions collectionlist.List[TransactionState]) *collectionmapping.Map[uint64, TransactionState] {
	transactions := collectionmapping.NewMapWithCapacity[uint64, TransactionState](snapshotTransactions.Len())
	snapshotTransactions.Range(func(_ int, state TransactionState) bool {
		if state.TxID != 0 {
			transactions.Set(state.TxID, cloneTransactionState(state))
		}
		return true
	})
	return transactions
}
