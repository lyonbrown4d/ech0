package broker

import (
	"encoding/base64"
	"strconv"
	"strings"
	"unicode/utf8"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func gatewayRecordAppend(payload, payloadBase64, key, keyBase64 string, headers []gatewayHeader) (store.RecordAppend, error) {
	rawPayload, err := gatewayBytes(payload, payloadBase64, "payload")
	if err != nil {
		return store.RecordAppend{}, err
	}
	rawKey, err := gatewayBytes(key, keyBase64, "key")
	if err != nil {
		return store.RecordAppend{}, err
	}
	record := store.NewRecordAppend(rawPayload)
	record.Key = rawKey
	record.Headers, err = gatewayHeaders(headers)
	return record, err
}

func gatewayBytes(value, base64Value, field string) ([]byte, error) {
	if strings.TrimSpace(base64Value) == "" {
		return []byte(value), nil
	}
	out, err := base64.StdEncoding.DecodeString(base64Value)
	if err != nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "invalid base64 %s", field)
	}
	return out, nil
}

func gatewayHeaders(headers []gatewayHeader) ([]store.RecordHeader, error) {
	out := collectionlist.NewListWithCapacity[store.RecordHeader](len(headers))
	for index := range headers {
		value, err := gatewayBytes(headers[index].Value, headers[index].ValueBase64, "header "+headers[index].Key)
		if err != nil {
			return nil, err
		}
		out.Add(store.RecordHeader{Key: headers[index].Key, Value: value})
	}
	return out.Values(), nil
}

func gatewayPartitioning(in *gatewayProduceInput) PublishPartitioning {
	if in.Body.Partition != nil {
		return PublishPartitioning{Mode: PartitionExplicit, Partition: *in.Body.Partition}
	}
	if in.Body.RoutingKey != "" {
		return PublishPartitioning{Mode: PartitionRoutingKeyHash, RoutingKey: in.Body.RoutingKey}
	}
	if in.Body.Key != "" || in.Body.KeyBase64 != "" {
		return PublishPartitioning{Mode: PartitionKeyHash}
	}
	return PublishPartitioning{Mode: PartitionRoundRobin}
}

func gatewayFetchIsolation(value string) FetchIsolation {
	if value == string(FetchIsolationReadCommitted) {
		return FetchIsolationReadCommitted
	}
	return FetchIsolationReadUncommitted
}

func gatewayFetchOffset(value string) (uint64, bool, error) {
	if strings.TrimSpace(value) == "" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false, brokerStoreError(store.CodeInvalidArgument, "invalid fetch offset %q", value)
	}
	return parsed, true, nil
}

func gatewayRecords(records []store.Record) []gatewayRecordResponse {
	return collectionlist.MapList(collectionlist.NewList(records...), func(_ int, record store.Record) gatewayRecordResponse {
		return gatewayRecord(record)
	}).Values()
}

func gatewayRecord(record store.Record) gatewayRecordResponse {
	return gatewayRecordResponse{
		Offset:        record.Offset,
		TimestampMS:   record.TimestampMS,
		RoutingKey:    recordRoutingKey(record),
		KeyBase64:     base64.StdEncoding.EncodeToString(record.Key),
		Headers:       gatewayHeadersFromStore(record.Headers),
		Tombstone:     record.IsTombstone(),
		ExpiresAtMS:   cloneUint64Ptr(record.ExpiresAtMS),
		Payload:       gatewayUTF8(record.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(record.Payload),
		NextOffset:    record.Offset + 1,
	}
}

func gatewayHeadersFromStore(headers []store.RecordHeader) []gatewayHeader {
	return collectionlist.MapList(collectionlist.NewList(headers...), func(_ int, header store.RecordHeader) gatewayHeader {
		return gatewayHeader{
			Key:         header.Key,
			Value:       gatewayUTF8(header.Value),
			ValueBase64: base64.StdEncoding.EncodeToString(header.Value),
		}
	}).Values()
}

func gatewayUTF8(value []byte) string {
	if !utf8.Valid(value) {
		return ""
	}
	return string(value)
}
