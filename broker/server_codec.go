package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/arcgolabs/mapper"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
	"github.com/samber/lo"
	"github.com/samber/oops"
)

var controlPlaneMapper = mapper.New()

func (s *TCPServer) recordCommandError(ctx context.Context, frame transport.Frame) {
	if s == nil || s.metrics == nil || frame.Header.Command != protocol.CmdErrorResponse {
		return
	}
	var out protocol.ErrorResponse
	if err := protocolbinary.DecodeBody(frame.Header.Command, frame.Body, &out); err != nil || out.Code == "" {
		s.metrics.RecordCommandError(ctx, "internal_error")
		return
	}
	s.metrics.RecordCommandError(ctx, out.Code)
}

func decode(frame transport.Frame, target any) error {
	return wrapBroker("frame_decode_failed", protocolbinary.DecodeBody(frame.Header.Command, frame.Body, target), "decode request frame")
}

func okFrame(command uint16, value any) (transport.Frame, error) {
	body, err := protocolbinary.EncodeBody(command, value)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_encode_failed", err, "encode response frame")
	}
	frame, err := transport.NewFrame(protocol.Version, command, body)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_frame_create_failed", err, "create response frame")
	}
	return frame, nil
}

func errorFrame(code, message string) transport.Frame {
	body, err := protocolbinary.EncodeBody(protocol.CmdErrorResponse, protocol.ErrorResponse{Code: code, Message: message})
	if err != nil {
		body = nil
	}
	frame, err := transport.NewFrame(protocol.Version, protocol.CmdErrorResponse, body)
	if err != nil {
		bodyLen, lenErr := errorFrameBodyLen(len(body))
		if lenErr != nil {
			bodyLen = 0
		}
		return transport.Frame{
			Header: errorFrameHeader(bodyLen),
			Body:   body,
		}
	}
	frame.Header.Status = transport.StatusError
	return frame
}

func errorFrameHeader(bodyLen uint32) transport.FrameHeader {
	header := transport.NewFrameHeader(protocol.Version, protocol.CmdErrorResponse, bodyLen)
	header.Status = transport.StatusError
	return header
}

func errorFrameBodyLen(length int) (uint32, error) {
	if length < 0 {
		return 0, errors.New("negative frame body length")
	}
	if uint64(length) > uint64(^uint32(0)) {
		return 0, oops.In("broker").Code("frame_body_len_failed").With("body_len", length).New("frame body length exceeds uint32")
	}
	return uint32(length), nil
}

func errorFromErr(err error) transport.Frame {
	return errorFrame(string(store.ErrorCode(err)), err.Error())
}

func topicConfigFromProtocol(req protocol.CreateTopicRequest) store.TopicConfig {
	topic := store.NewTopicConfig(req.Topic)
	topic.Partitions = req.Partitions
	topic.RetentionMS = req.RetentionMS
	topic.DeadLetterTopic = req.DeadLetterTopic
	topic.MessageTTLMS = req.MessageTTLMS
	topic.CompactionTombstoneRetentionMS = req.CompactionTombstoneRetentionMS
	applyTopicLimitOptions(&topic, req)
	applyTopicPolicyOptions(&topic, req)
	return topic
}

func applyTopicLimitOptions(topic *store.TopicConfig, req protocol.CreateTopicRequest) {
	if req.RetentionMaxBytes != nil {
		topic.RetentionMaxBytes = *req.RetentionMaxBytes
	}
	if req.MaxMessageBytes != nil {
		topic.MaxMessageBytes = *req.MaxMessageBytes
	}
	if req.MaxBatchBytes != nil {
		topic.MaxBatchBytes = *req.MaxBatchBytes
	}
}

func applyTopicPolicyOptions(topic *store.TopicConfig, req protocol.CreateTopicRequest) {
	if req.CleanupPolicy != nil {
		topic.CleanupPolicy = store.TopicCleanupPolicy(*req.CleanupPolicy)
	}
	if req.RetryPolicy != nil {
		topic.RetryPolicy = store.TopicRetryPolicy{
			MaxAttempts:         req.RetryPolicy.MaxAttempts,
			BackoffInitialMS:    req.RetryPolicy.BackoffInitialMS,
			BackoffMaxMS:        req.RetryPolicy.BackoffMaxMS,
			BackoffJitterFactor: req.RetryPolicy.BackoffJitterFactor,
		}
	}
	if req.DelayEnabled != nil {
		topic.DelayEnabled = *req.DelayEnabled
	}
	if req.MessageExpiryAction != nil {
		topic.MessageExpiryAction = store.MessageExpiryAction(*req.MessageExpiryAction)
	}
	if req.OrderingPolicy != nil {
		topic.OrderingPolicy = store.TopicOrderingPolicy(*req.OrderingPolicy)
	}
	if req.PriorityPolicy != nil {
		topic.PriorityPolicy = store.TopicPriorityPolicy{
			Enabled: req.PriorityPolicy.Enabled,
			Min:     req.PriorityPolicy.Min,
			Max:     req.PriorityPolicy.Max,
			Default: req.PriorityPolicy.Default,
		}
	}
	if req.CompactionEnabled != nil {
		topic.CompactionEnabled = *req.CompactionEnabled
	}
}

func partitioningFromProtocol(mode protocol.ProducePartitioning, partition *uint32, routingKey string) PublishPartitioning {
	switch mode {
	case protocol.ProducePartitioningExplicit:
		return explicitPartitioning(partition)
	case protocol.ProducePartitioningKeyHash:
		return PublishPartitioning{Mode: PartitionKeyHash}
	case protocol.ProducePartitioningRoutingKeyHash:
		return PublishPartitioning{Mode: PartitionRoutingKeyHash, RoutingKey: routingKey}
	case protocol.ProducePartitioningRoundRobin:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	default:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	}
}

func produceIdempotencyFromProtocol(id *protocol.ProduceIdempotency) *ProduceIdempotency {
	if id == nil {
		return nil
	}
	return &ProduceIdempotency{
		ProducerID:    id.ProducerID,
		ProducerEpoch: id.ProducerEpoch,
		BaseSequence:  id.BaseSequence,
	}
}

func explicitPartitioning(partition *uint32) PublishPartitioning {
	if partition == nil {
		return PublishPartitioning{Mode: PartitionExplicit, Partition: 0}
	}
	return PublishPartitioning{Mode: PartitionExplicit, Partition: *partition}
}

func mergeProduceBatchesResponse(
	requests []protocol.ProduceBatchesItemRequest,
	base []protocol.ProduceBatchesItemResponse,
	results []produceBatchItemResult,
) []protocol.ProduceBatchesItemResponse {
	if len(base) == 0 {
		return nil
	}
	return lo.Map(base, func(item protocol.ProduceBatchesItemResponse, index int) protocol.ProduceBatchesItemResponse {
		return mergeProduceBatchesItemResponse(index, requests, item, results)
	})
}

func mergeProduceBatchesItemResponse(
	index int,
	requests []protocol.ProduceBatchesItemRequest,
	item protocol.ProduceBatchesItemResponse,
	results []produceBatchItemResult,
) protocol.ProduceBatchesItemResponse {
	if index < len(requests) {
		item.Topic = requests[index].Topic
	}
	if item.Error != "" {
		return item
	}
	if index >= len(results) {
		item.Error = "produce batch result missing"
		return item
	}
	return produceBatchItemToProtocol(item, results[index])
}

func produceBatchItemToProtocol(
	item protocol.ProduceBatchesItemResponse,
	result produceBatchItemResult,
) protocol.ProduceBatchesItemResponse {
	if result.Error != "" {
		item.Error = result.Error
		return item
	}
	item.Partition = result.Result.Partition
	item.Appended = len(result.Result.Records)
	if len(result.Result.Records) == 0 {
		return item
	}
	item.BaseOffset = result.Result.Records[0].Offset
	item.LastOffset = result.Result.Records[len(result.Result.Records)-1].Offset
	item.NextOffset = item.LastOffset + 1
	return item
}

func leaseFromStore(member store.ConsumerGroupMember) (protocol.ConsumerGroupMemberLease, error) {
	var out protocol.ConsumerGroupMemberLease
	if err := controlPlaneMapper.MapInto(&out, member); err != nil {
		return protocol.ConsumerGroupMemberLease{}, fmt.Errorf("map consumer group member lease: %w", err)
	}
	out.ExpiresAtMS = member.ExpiresAtMS()
	out.PollExpiresAtMS = member.PollExpiresAtMS()
	return out, nil
}

func assignmentToProtocol(assignment store.ConsumerGroupAssignment) (protocol.ConsumerGroupAssignment, error) {
	var out protocol.ConsumerGroupAssignment
	if err := controlPlaneMapper.MapInto(&out, assignment); err != nil {
		return protocol.ConsumerGroupAssignment{}, fmt.Errorf("map consumer group assignment: %w", err)
	}
	return out, nil
}

func optionalAssignmentToProtocol(assignment *store.ConsumerGroupAssignment) (protocol.ConsumerGroupAssignment, bool, error) {
	if assignment == nil {
		return protocol.ConsumerGroupAssignment{}, false, nil
	}
	converted, err := assignmentToProtocol(*assignment)
	if err != nil {
		return protocol.ConsumerGroupAssignment{}, false, err
	}
	return converted, true, nil
}

func directMessagesToProtocol(records []direct.InboxRecord) []protocol.DirectMessageRecord {
	if len(records) == 0 {
		return nil
	}
	return lo.Map(records, func(record direct.InboxRecord, _ int) protocol.DirectMessageRecord {
		return protocol.DirectMessageRecord{
			Offset:         record.Offset,
			MessageID:      record.Message.MessageID,
			ConversationID: record.Message.ConversationID,
			Sender:         record.Message.Sender,
			Recipient:      record.Message.Recipient,
			TimestampMS:    record.Message.TimestampMS,
			Payload:        record.Message.Payload,
		}
	})
}
