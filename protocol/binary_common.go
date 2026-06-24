package protocol

import "github.com/samber/oops"

func encodeWith[T any](value any, write func(*binaryWriter, T) error) ([]byte, error) {
	typed, ok := value.(T)
	if !ok {
		return nil, typeMismatch[T](value)
	}
	writer := newBinaryWriter()
	if err := write(writer, typed); err != nil {
		writer.release()
		return nil, err
	}
	return writer.bytes(), nil
}

func decodeWith[T any](data []byte, target any, read func(*binaryReader) (T, error)) error {
	out, ok := target.(*T)
	if !ok {
		return typeMismatch[*T](target)
	}
	reader := newBinaryReader(data)
	value, err := read(reader)
	if err != nil {
		return err
	}
	if err := reader.ensureEOF(); err != nil {
		return err
	}
	*out = value
	return nil
}

type decodedList[T any] struct {
	values []T
	next   int
}

func newDecodedList[T any](capacity int) decodedList[T] {
	if capacity == 0 {
		return decodedList[T]{}
	}
	return decodedList[T]{values: make([]T, capacity)}
}

func (l *decodedList[T]) Add(value T) {
	l.values[l.next] = value
	l.next++
}

func (l decodedList[T]) Values() []T {
	return l.values
}

func typeMismatch[T any](value any) error {
	var zero T
	return oops.In("protocol").Code("binary_type_mismatch").With("want", zero, "got", value).New("binary codec type mismatch")
}

func writePartitioning(writer *binaryWriter, value ProducePartitioning) {
	switch value {
	case ProducePartitioningExplicit:
		writer.writeU8(1)
	case ProducePartitioningKeyHash:
		writer.writeU8(3)
	case ProducePartitioningRoutingKeyHash:
		writer.writeU8(4)
	case ProducePartitioningRoundRobin:
		writer.writeU8(2)
	default:
		writer.writeU8(0)
	}
}

func readPartitioning(reader *binaryReader) (ProducePartitioning, error) {
	value, err := reader.readU8()
	if err != nil {
		return "", err
	}
	switch value {
	case 1:
		return ProducePartitioningExplicit, nil
	case 2:
		return ProducePartitioningRoundRobin, nil
	case 3:
		return ProducePartitioningKeyHash, nil
	case 4:
		return ProducePartitioningRoutingKeyHash, nil
	default:
		return "", oops.In("protocol").Code("binary_unknown_partitioning").With("value", value).New("unknown partitioning")
	}
}

func writeFetchIsolation(writer *binaryWriter, value FetchIsolation) {
	switch value {
	case "", FetchIsolationReadUncommitted:
		writer.writeU8(1)
	case FetchIsolationReadCommitted:
		writer.writeU8(2)
	default:
		writer.writeU8(0)
	}
}

func readFetchIsolation(reader *binaryReader) (FetchIsolation, error) {
	value, err := reader.readU8()
	if err != nil {
		return "", err
	}
	switch value {
	case 1:
		return FetchIsolationReadUncommitted, nil
	case 2:
		return FetchIsolationReadCommitted, nil
	default:
		return "", oops.In("protocol").Code("binary_unknown_fetch_isolation").With("value", value).New("unknown fetch isolation")
	}
}

func writeTransactionStatus(writer *binaryWriter, status TransactionStatus) {
	switch status {
	case "", TransactionStatusOpen:
		writer.writeU8(1)
	case TransactionStatusCommitted:
		writer.writeU8(2)
	case TransactionStatusAborted:
		writer.writeU8(3)
	default:
		writer.writeU8(0)
	}
}

func readTransactionStatus(reader *binaryReader) (TransactionStatus, error) {
	value, err := reader.readU8()
	if err != nil {
		return "", err
	}
	switch value {
	case 1:
		return TransactionStatusOpen, nil
	case 2:
		return TransactionStatusCommitted, nil
	case 3:
		return TransactionStatusAborted, nil
	default:
		return "", oops.In("protocol").Code("binary_unknown_transaction_status").With("value", value).New("unknown transaction status")
	}
}

func writeTransactionControlType(writer *binaryWriter, controlType TransactionControlType) {
	switch controlType {
	case TransactionControlNone:
		writer.writeU8(0)
	case TransactionControlCommit:
		writer.writeU8(1)
	case TransactionControlAbort:
		writer.writeU8(2)
	default:
		writer.writeU8(255)
	}
}

func readTransactionControlType(reader *binaryReader) (TransactionControlType, error) {
	value, err := reader.readU8()
	if err != nil {
		return "", err
	}
	switch value {
	case 0:
		return TransactionControlNone, nil
	case 1:
		return TransactionControlCommit, nil
	case 2:
		return TransactionControlAbort, nil
	default:
		return "", oops.In("protocol").Code("binary_unknown_transaction_control").With("value", value).New("unknown transaction control type")
	}
}

func writeCleanupPolicy(writer *binaryWriter, value *TopicCleanupPolicy) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	switch *value {
	case TopicCleanupDelete:
		writer.writeU8(1)
	case TopicCleanupCompact:
		writer.writeU8(2)
	case TopicCleanupCompactAndDelete:
		writer.writeU8(3)
	default:
		writer.writeU8(0)
	}
}

func readCleanupPolicy(reader *binaryReader) (*TopicCleanupPolicy, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	var out TopicCleanupPolicy
	switch value {
	case 1:
		out = TopicCleanupDelete
	case 2:
		out = TopicCleanupCompact
	case 3:
		out = TopicCleanupCompactAndDelete
	default:
		return nil, oops.In("protocol").Code("binary_unknown_cleanup_policy").With("value", value).New("unknown cleanup policy")
	}
	return &out, nil
}

func writeHeaders(writer *binaryWriter, headers []MessageHeader) error {
	count, err := checkedUint16(len(headers), "headers")
	if err != nil {
		return err
	}
	writer.writeU16(count)
	for _, header := range headers {
		if err := writer.writeString(header.Key); err != nil {
			return err
		}
		if err := writer.writeBytes(header.Value); err != nil {
			return err
		}
	}
	return nil
}

func readHeaders(reader *binaryReader) ([]MessageHeader, error) {
	count, err := reader.readU16()
	if err != nil {
		return nil, err
	}
	headers := newDecodedList[MessageHeader](int(count))
	for range count {
		key, err := reader.readString()
		if err != nil {
			return nil, err
		}
		value, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		headers.Add(MessageHeader{Key: key, Value: value})
	}
	return headers.Values(), nil
}

func writeStringSlice(writer *binaryWriter, values []string) error {
	count, err := checkedUint32(len(values), "strings")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, value := range values {
		if err := writer.writeString(value); err != nil {
			return err
		}
	}
	return nil
}

func readStringSlice(reader *binaryReader) ([]string, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	out := newDecodedList[string](size)
	for range size {
		value, err := reader.readString()
		if err != nil {
			return nil, err
		}
		out.Add(value)
	}
	return out.Values(), nil
}
