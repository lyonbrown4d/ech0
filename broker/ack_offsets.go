package broker

import (
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func (b *Broker) applyAckOffset(req commitOffsetCommand) (struct{}, error) {
	tp := store.NewTopicPartition(req.Topic, req.Partition)
	nextOffset, pending, err := b.advanceAckedOffsets(req.Consumer, tp, *req.AckOffset)
	if err != nil {
		return struct{}{}, err
	}
	state := store.ConsumerOffsetState{
		Consumer:       req.Consumer,
		Topic:          req.Topic,
		Partition:      req.Partition,
		NextOffset:     nextOffset,
		PendingOffsets: pending,
		Metadata:       req.Metadata,
		UpdatedAtMS:    req.UpdatedAtMS,
	}
	if state.UpdatedAtMS == 0 {
		state.UpdatedAtMS = store.NowMS()
	}
	return struct{}{}, wrapBrokerStore(b.meta.SaveConsumerOffsetState(state), "ack record offset")
}

func (b *Broker) advanceAckedOffsets(consumer string, tp store.TopicPartition, ackOffset uint64) (uint64, []uint64, error) {
	offsets, err := b.queue.PartitionOffsets(tp)
	if err != nil {
		return 0, nil, wrapBrokerStore(err, "load partition offsets for ack")
	}
	nextOffset := offsets.LogStartOffset
	var pending []uint64
	state, err := b.meta.LoadConsumerOffsetState(consumer, tp)
	if err != nil {
		return 0, nil, wrapBrokerStore(err, "load consumer offset for ack")
	}
	if state != nil {
		nextOffset = max(state.NextOffset, offsets.LogStartOffset)
		pending = append(pending, state.PendingOffsets...)
	}
	if ackOffset < nextOffset {
		return nextOffset, pending, nil
	}
	pending = append(pending, ackOffset)
	nextOffset, pending = drainAckedOffsets(nextOffset, pending)
	return nextOffset, pending, nil
}

func drainAckedOffsets(nextOffset uint64, pending []uint64) (uint64, []uint64) {
	state := store.NormalizeConsumerOffsetState(store.ConsumerOffsetState{
		NextOffset:     nextOffset,
		PendingOffsets: pending,
	})
	pending = state.PendingOffsets
	for len(pending) > 0 && pending[0] == nextOffset {
		nextOffset++
		pending = pending[1:]
	}
	state = store.NormalizeConsumerOffsetState(store.ConsumerOffsetState{
		NextOffset:     nextOffset,
		PendingOffsets: pending,
	})
	return state.NextOffset, state.PendingOffsets
}

func (b *Broker) removePendingAckedRecords(consumer string, tp store.TopicPartition, records []store.Record) ([]store.Record, error) {
	if len(records) == 0 {
		return records, nil
	}
	state, err := b.meta.LoadConsumerOffsetState(consumer, tp)
	if err != nil {
		return nil, wrapBrokerStore(err, "load pending acked offsets")
	}
	if state == nil || len(state.PendingOffsets) == 0 {
		return records, nil
	}
	return recordsWithoutPendingOffsets(records, state.PendingOffsets), nil
}

func recordsWithoutPendingOffsets(records []store.Record, pendingOffsets []uint64) []store.Record {
	pending := collectionset.NewSetWithCapacity[uint64](len(pendingOffsets), pendingOffsets...)
	out := lo.Filter(records, func(record store.Record, _ int) bool {
		return !pending.Contains(record.Offset)
	})
	if len(out) == 0 {
		return nil
	}
	return out
}
