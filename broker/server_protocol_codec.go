package broker

import (
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func storeHeadersFromProtocol(headers []protocol.MessageHeader) []store.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	return lo.Map(headers, func(header protocol.MessageHeader, _ int) store.RecordHeader {
		return store.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	})
}

func protocolHeadersFromStore(headers []store.RecordHeader) []protocol.MessageHeader {
	if len(headers) == 0 {
		return nil
	}
	return lo.Map(headers, func(header store.RecordHeader, _ int) protocol.MessageHeader {
		return protocol.MessageHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	})
}

func isolationFromProtocol(value protocol.FetchIsolation) FetchIsolation {
	if value == protocol.FetchIsolationReadCommitted {
		return FetchIsolationReadCommitted
	}
	return FetchIsolationReadUncommitted
}

func transactionIdentityFromProtocol(identity protocol.TransactionIdentity) TransactionIdentity {
	return TransactionIdentity{
		TxID:          identity.TxID,
		ProducerID:    identity.ProducerID,
		ProducerEpoch: identity.ProducerEpoch,
	}
}
