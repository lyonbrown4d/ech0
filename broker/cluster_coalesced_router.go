package broker

import (
	"context"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

type produceBatchShardGroup struct {
	shardID  store.ShardID
	indexes  []int
	requests []produceBatchCommand
}

type commitOffsetShardGroup struct {
	shardID  store.ShardID
	indexes  []int
	requests []commitOffsetCommand
}

func (b *Broker) routeProduceBatchesCommand(ctx context.Context, req produceBatchesCommand) (produceBatchesResult, error) {
	if b.canRouteCoalescedByShard() {
		return b.routeProduceBatchesByShard(ctx, req)
	}
	return b.routeProduceBatchesSingle(ctx, req)
}

func (b *Broker) routeCommitOffsetsCommand(ctx context.Context, req commitOffsetsCommand) (commitOffsetsResult, error) {
	if b.canRouteCoalescedByShard() {
		return b.routeCommitOffsetsByShard(ctx, req)
	}
	return b.routeCommitOffsetsSingle(ctx, req)
}

func (b *Broker) canRouteCoalescedByShard() bool {
	return b != nil && b.usesClusterCommandRouter() && b.shards != nil
}

func (b *Broker) routeProduceBatchesSingle(ctx context.Context, req produceBatchesCommand) (produceBatchesResult, error) {
	value, err := b.applyRoutedPartitionCommand(
		ctx,
		produceBatchesCommandTarget(req),
		raftCommandProduceBatches,
		req,
		func(ctx context.Context) (any, error) {
			return b.applyProduceBatches(ctx, req)
		},
	)
	if err != nil {
		return produceBatchesResult{}, err
	}
	return raftValueAs[produceBatchesResult](value)
}

func (b *Broker) routeCommitOffsetsSingle(ctx context.Context, req commitOffsetsCommand) (commitOffsetsResult, error) {
	value, err := b.applyRoutedPartitionCommand(
		ctx,
		commitOffsetsCommandTarget(req),
		raftCommandCommitOffsets,
		req,
		func(ctx context.Context) (any, error) {
			return b.applyCommitOffsets(ctx, req)
		},
	)
	if err != nil {
		return commitOffsetsResult{}, err
	}
	return raftValueAs[commitOffsetsResult](value)
}

func (b *Broker) routeProduceBatchesByShard(ctx context.Context, req produceBatchesCommand) (produceBatchesResult, error) {
	groups, items := b.planProduceBatchShardGroups(req)
	if len(groups) == 0 {
		return produceBatchesResult{Items: items}, nil
	}
	if err := runBounded(ctx, int64(len(groups)), len(groups), func(groupCtx context.Context, index int) error {
		b.routeProduceBatchShardGroup(groupCtx, groups[index], items)
		return nil
	}); err != nil {
		return produceBatchesResult{}, err
	}
	return produceBatchesResult{Items: items}, nil
}

func (b *Broker) planProduceBatchShardGroups(req produceBatchesCommand) ([]produceBatchShardGroup, []produceBatchItemResult) {
	items := make([]produceBatchItemResult, len(req.Requests))
	groups := collectionmapping.NewMapWithCapacity[store.ShardID, produceBatchShardGroup](len(req.Requests))
	for index, command := range req.Requests {
		shardID, planned, err := b.planProduceBatchShardCommand(command)
		if err != nil {
			items[index].Error = errorMessage(err)
			continue
		}
		group := groups.GetOrDefault(shardID, produceBatchShardGroup{shardID: shardID})
		group.indexes = append(group.indexes, index)
		group.requests = append(group.requests, planned)
		groups.Set(shardID, group)
	}
	return groups.Values(), items
}

func (b *Broker) planProduceBatchShardCommand(req produceBatchCommand) (store.ShardID, produceBatchCommand, error) {
	plan, err := b.planProduceBatch(0, req)
	if err != nil {
		return 0, produceBatchCommand{}, err
	}
	placement, err := b.shards.Resolve(plan.tp)
	if err != nil {
		return 0, produceBatchCommand{}, err
	}
	planned := plan.request
	planned.Partitioning = PublishPartitioning{Mode: PartitionExplicit, Partition: plan.tp.Partition}
	return placement.ShardID, planned, nil
}

func (b *Broker) routeProduceBatchShardGroup(ctx context.Context, group produceBatchShardGroup, items []produceBatchItemResult) {
	groupReq := produceBatchesCommand{Requests: group.requests}
	value, err := b.applyRoutedPartitionCommand(
		ctx,
		shardCommandTarget(group.shardID),
		raftCommandProduceBatches,
		groupReq,
		func(ctx context.Context) (any, error) {
			return b.applyProduceBatches(ctx, groupReq)
		},
	)
	if err != nil {
		setProduceBatchGroupError(items, group.indexes, err)
		return
	}
	result, err := raftValueAs[produceBatchesResult](value)
	if err != nil {
		setProduceBatchGroupError(items, group.indexes, err)
		return
	}
	mergeProduceBatchShardGroupResult(group, result, items)
}

func mergeProduceBatchShardGroupResult(group produceBatchShardGroup, result produceBatchesResult, items []produceBatchItemResult) {
	if len(result.Items) != len(group.indexes) {
		setProduceBatchGroupError(items, group.indexes, brokerStoreError(store.CodeCodec, "produce shard group result length mismatch: got %d want %d", len(result.Items), len(group.indexes)))
		return
	}
	for index, item := range result.Items {
		items[group.indexes[index]] = item
	}
}

func (b *Broker) routeCommitOffsetsByShard(ctx context.Context, req commitOffsetsCommand) (commitOffsetsResult, error) {
	groups, items := b.planCommitOffsetShardGroups(req)
	if len(groups) == 0 {
		return commitOffsetsResult{Items: items}, nil
	}
	if err := runBounded(ctx, int64(len(groups)), len(groups), func(groupCtx context.Context, index int) error {
		b.routeCommitOffsetShardGroup(groupCtx, groups[index], items)
		return nil
	}); err != nil {
		return commitOffsetsResult{}, err
	}
	return commitOffsetsResult{Items: items}, nil
}

func (b *Broker) planCommitOffsetShardGroups(req commitOffsetsCommand) ([]commitOffsetShardGroup, []commitOffsetItemResult) {
	items := make([]commitOffsetItemResult, len(req.Requests))
	groups := collectionmapping.NewMapWithCapacity[store.ShardID, commitOffsetShardGroup](len(req.Requests))
	for index, command := range req.Requests {
		placement, err := b.shards.Resolve(store.NewTopicPartition(command.Topic, command.Partition))
		if err != nil {
			items[index].Error = errorMessage(err)
			continue
		}
		group := groups.GetOrDefault(placement.ShardID, commitOffsetShardGroup{shardID: placement.ShardID})
		group.indexes = append(group.indexes, index)
		group.requests = append(group.requests, command)
		groups.Set(placement.ShardID, group)
	}
	return groups.Values(), items
}

func (b *Broker) routeCommitOffsetShardGroup(ctx context.Context, group commitOffsetShardGroup, items []commitOffsetItemResult) {
	groupReq := commitOffsetsCommand{Requests: group.requests}
	value, err := b.applyRoutedPartitionCommand(
		ctx,
		shardCommandTarget(group.shardID),
		raftCommandCommitOffsets,
		groupReq,
		func(ctx context.Context) (any, error) {
			return b.applyCommitOffsets(ctx, groupReq)
		},
	)
	if err != nil {
		setCommitOffsetGroupError(items, group.indexes, err)
		return
	}
	result, err := raftValueAs[commitOffsetsResult](value)
	if err != nil {
		setCommitOffsetGroupError(items, group.indexes, err)
		return
	}
	mergeCommitOffsetShardGroupResult(group, result, items)
}

func mergeCommitOffsetShardGroupResult(group commitOffsetShardGroup, result commitOffsetsResult, items []commitOffsetItemResult) {
	if len(result.Items) != len(group.indexes) {
		setCommitOffsetGroupError(items, group.indexes, brokerStoreError(store.CodeCodec, "commit shard group result length mismatch: got %d want %d", len(result.Items), len(group.indexes)))
		return
	}
	for index, item := range result.Items {
		items[group.indexes[index]] = item
	}
}

func setCommitOffsetGroupError(items []commitOffsetItemResult, indexes []int, err error) {
	message := errorMessage(err)
	for _, index := range indexes {
		items[index].Error = message
	}
}
