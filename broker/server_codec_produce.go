package broker

import (
	"errors"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func batchRecordsFromProtocol(req protocol.ProduceBatchRequest) ([]store.RecordAppend, error) {
	if len(req.Payloads) == 0 && len(req.Records) == 0 {
		return nil, errors.New("produce_batch requires payloads or records")
	}
	if len(req.Payloads) > 0 && len(req.Records) > 0 {
		return nil, errors.New("produce_batch must provide only one of payloads or records")
	}
	if len(req.Records) > 0 {
		return batchRecordItemsFromProtocol(req.Records), nil
	}
	return batchPayloadsFromProtocol(req.Payloads), nil
}

func batchRecordItemsFromProtocol(records []protocol.ProduceBatchRecord) []store.RecordAppend {
	if len(records) == 0 {
		return nil
	}
	return lo.Map(records, func(record protocol.ProduceBatchRecord, _ int) store.RecordAppend {
		return recordItemFromProtocol(record)
	})
}

func recordItemFromProtocol(record protocol.ProduceBatchRecord) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.Key = append([]byte(nil), record.Key...)
	appendRecord.Headers = storeHeadersFromProtocol(record.Headers)
	applyRoutingKey(&appendRecord, record.RoutingKey)
	appendRecord.ExpiresAtMS = cloneUint64Ptr(record.ExpiresAtMS)
	if record.Tombstone {
		appendRecord.Attributes |= store.RecordAttributeTombstone
	}
	return appendRecord
}

func batchPayloadsFromProtocol(payloads [][]byte) []store.RecordAppend {
	if len(payloads) == 0 {
		return nil
	}
	return lo.Map(payloads, func(payload []byte, _ int) store.RecordAppend {
		return store.NewRecordAppend(payload)
	})
}

func batchRecordsWithRequestRoutingKey(req protocol.ProduceBatchRequest, records []store.RecordAppend) []store.RecordAppend {
	if req.RoutingKey == "" {
		return records
	}
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index := range records {
		record := records[index]
		applyRoutingKey(&record, req.RoutingKey)
		out.Add(record)
	}
	return out.Values()
}

func fanoutResultToProtocol(result FanoutResult) protocol.ProduceFanoutResponse {
	if len(result.Records) == 0 {
		return protocol.ProduceFanoutResponse{}
	}
	records := lo.Map(result.Records, func(record FanoutRecordResult, _ int) protocol.ProduceFanoutRecordResponse {
		return protocol.ProduceFanoutRecordResponse{
			Partition:  record.Partition,
			Offset:     record.Record.Offset,
			NextOffset: record.NextOffset,
		}
	})
	return protocol.ProduceFanoutResponse{Records: records}
}
