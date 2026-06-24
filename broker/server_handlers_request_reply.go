package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
	"github.com/samber/lo"
)

func (s *TCPServer) handleStartRequestFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.StartRequestRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("request payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	pending, err := s.broker.StartRequest(ctx, req.Subject, req.Payload, requestOptionsFromProtocol(req))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdStartRequestResponse, startRequestResponseFromBroker(pending))
}

func (s *TCPServer) handleFetchRequestsFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchRequestsRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.FetchRequestsWithIsolation(ctx, req.Consumer, req.Subject, req.Partition, req.Offset, req.MaxRecords, isolationFromProtocol(req.Isolation))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchRequestsResponse, fetchRequestsResponseFromBroker(result))
}

func (s *TCPServer) handleReplyFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ReplyRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.Reply(ctx, requestMessageFromReplyRequest(req), req.ResponderID, req.Payload)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdReplyResponse, replyResponseFromDirect(result))
}

func (s *TCPServer) handleReplyErrorFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ReplyErrorRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.ReplyError(ctx, requestMessageFromReplyErrorRequest(req), req.ResponderID, req.Error)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdReplyErrorResponse, replyResponseFromDirect(result))
}

func (s *TCPServer) handleAwaitReplyFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.AwaitReplyRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	reply, err := s.broker.AwaitReply(ctx, pendingRequestFromProtocol(req))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdAwaitReplyResponse, protocol.AwaitReplyResponse{Reply: replyRecordFromBroker(reply)})
}

func (s *TCPServer) handleAwaitRepliesFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.AwaitRepliesRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	replies, err := s.broker.AwaitReplies(ctx, pendingRequestFromAwaitRepliesProtocol(req), req.MaxReplies)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdAwaitRepliesResponse, protocol.AwaitRepliesResponse{Replies: replyRecordsFromBroker(replies)})
}

func requestOptionsFromProtocol(req protocol.StartRequestRequest) RequestOptions {
	return RequestOptions{
		InstanceID:   req.InstanceID,
		Timeout:      durationFromOptionalMS(req.TimeoutMS),
		PollInterval: durationFromOptionalMS(req.PollIntervalMS),
		Partitioning: partitioningFromProtocol(req.Partitioning, req.Partition, ""),
		ReplyMode:    RequestReplyMode(req.ReplyMode),
		Headers:      storeHeadersFromProtocol(req.Headers),
	}
}

func startRequestResponseFromBroker(pending PendingRequest) protocol.StartRequestResponse {
	return protocol.StartRequestResponse{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAtMS:   pending.ExpiresAtMS,
		ReplyMode:     string(pending.ReplyMode),
		Partition:     pending.Produce.Partition,
		Offset:        pending.Produce.Record.Offset,
		NextOffset:    pending.Produce.Record.Offset + 1,
	}
}

func fetchRequestsResponseFromBroker(result RequestPollResult) protocol.FetchRequestsResponse {
	return protocol.FetchRequestsResponse{
		Subject:        result.Subject,
		Partition:      result.Partition,
		Requests:       requestRecordsFromBroker(result.Requests),
		NextOffset:     result.NextOffset,
		HighWatermark:  result.HighWatermark,
		LowWatermark:   result.LowWatermark,
		LogStartOffset: result.LogStartOffset,
	}
}

func requestRecordsFromBroker(requests []RequestMessage) []protocol.RequestRecord {
	if len(requests) == 0 {
		return nil
	}
	return lo.Map(requests, func(req RequestMessage, _ int) protocol.RequestRecord {
		return protocol.RequestRecord{
			Offset:        req.Record.Offset,
			TimestampMS:   req.Record.TimestampMS,
			Subject:       req.Subject,
			ReplyTo:       req.ReplyTo,
			CorrelationID: req.CorrelationID,
			SenderID:      req.SenderID,
			ExpiresAtMS:   req.ExpiresAtMS,
			ReplyMode:     string(req.ReplyMode),
			Headers:       protocolHeadersFromStore(req.Headers),
			Payload:       append([]byte(nil), req.Payload...),
		}
	})
}

func requestMessageFromReplyRequest(req protocol.ReplyRequest) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		ExpiresAtMS:   req.ExpiresAtMS,
	}
}

func requestMessageFromReplyErrorRequest(req protocol.ReplyErrorRequest) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		ExpiresAtMS:   req.ExpiresAtMS,
	}
}

func replyResponseFromDirect(result direct.SendResult) protocol.ReplyResponse {
	return protocol.ReplyResponse{
		MessageID:      result.MessageID,
		ConversationID: result.ConversationID,
		Offset:         result.Offset,
		NextOffset:     result.NextOffset,
	}
}

func pendingRequestFromProtocol(req protocol.AwaitReplyRequest) PendingRequest {
	return pendingRequestFromAwaitFields(req.ReplyTo, req.CorrelationID, req.ExpiresAtMS, req.TimeoutMS, req.PollIntervalMS, RequestReplyModeFirstResponseWins)
}

func pendingRequestFromAwaitRepliesProtocol(req protocol.AwaitRepliesRequest) PendingRequest {
	return pendingRequestFromAwaitFields(req.ReplyTo, req.CorrelationID, req.ExpiresAtMS, req.TimeoutMS, req.PollIntervalMS, RequestReplyModeMultiReplier)
}

func pendingRequestFromAwaitFields(
	replyTo string,
	correlationID string,
	expiresAtMS uint64,
	timeoutMS *uint64,
	pollIntervalMS *uint64,
	replyMode RequestReplyMode,
) PendingRequest {
	timeout := durationFromOptionalMS(timeoutMS)
	if expiresAtMS == 0 {
		if timeout <= 0 {
			timeout = defaultRequestTimeout
		}
		expiresAtMS = store.NowMS() + durationMillis(timeout)
	}
	pollInterval := durationFromOptionalMS(pollIntervalMS)
	if pollInterval <= 0 {
		pollInterval = defaultRequestPollInterval
	}
	return PendingRequest{
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
		ExpiresAtMS:   expiresAtMS,
		PollInterval:  pollInterval,
		ReplyMode:     replyMode,
	}
}

func replyRecordFromBroker(reply ReplyMessage) protocol.ReplyRecord {
	return protocol.ReplyRecord{
		Offset:        reply.Offset,
		MessageID:     reply.Message.MessageID,
		TimestampMS:   reply.Message.TimestampMS,
		Subject:       reply.Subject,
		CorrelationID: reply.CorrelationID,
		ResponderID:   reply.SenderID,
		Error:         cloneStringPointer(reply.Error),
		Payload:       append([]byte(nil), reply.Payload...),
	}
}

func replyRecordsFromBroker(replies []ReplyMessage) []protocol.ReplyRecord {
	if len(replies) == 0 {
		return nil
	}
	return lo.Map(replies, func(reply ReplyMessage, _ int) protocol.ReplyRecord {
		return replyRecordFromBroker(reply)
	})
}

func durationFromOptionalMS(value *uint64) time.Duration {
	if value == nil || *value == 0 {
		return 0
	}
	return time.Duration(safeUint64ToInt64(*value)) * time.Millisecond
}
