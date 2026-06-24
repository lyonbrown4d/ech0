package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/lyonbrown4d/ech0/internal/bufferpool"
	"github.com/samber/oops"
)

type binaryWriter struct {
	buf *bufferpool.Buffer
}

func newBinaryWriter() *binaryWriter {
	return &binaryWriter{buf: bufferpool.Get()}
}

func (w *binaryWriter) bytes() []byte {
	out := bufferpool.CopyAndPut(w.buf)
	w.buf = nil
	return out
}

func (w *binaryWriter) release() {
	bufferpool.Put(w.buf)
	w.buf = nil
}

func (w *binaryWriter) writeU8(value uint8) {
	if err := w.buf.WriteByte(value); err != nil {
		panic(err)
	}
}

func (w *binaryWriter) writeBool(value bool) {
	if value {
		w.writeU8(1)
		return
	}
	w.writeU8(0)
}

func (w *binaryWriter) writeU16(value uint16) {
	var raw [2]byte
	binary.BigEndian.PutUint16(raw[:], value)
	if _, err := w.buf.Write(raw[:]); err != nil {
		panic(err)
	}
}

func (w *binaryWriter) writeU32(value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	if _, err := w.buf.Write(raw[:]); err != nil {
		panic(err)
	}
}

func (w *binaryWriter) writeU64(value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	if _, err := w.buf.Write(raw[:]); err != nil {
		panic(err)
	}
}

func (w *binaryWriter) writeF64(value float64) {
	w.writeU64(math.Float64bits(value))
}

func (w *binaryWriter) writeString(value string) error {
	size, err := checkedUint16(len(value), "string")
	if err != nil {
		return err
	}
	w.writeU16(size)
	if _, err := w.buf.WriteString(value); err != nil {
		panic(err)
	}
	return nil
}

func (w *binaryWriter) writeBytes(value []byte) error {
	size, err := checkedUint32(len(value), "bytes")
	if err != nil {
		return err
	}
	w.writeU32(size)
	if _, err := w.buf.Write(value); err != nil {
		panic(err)
	}
	return nil
}

func (w *binaryWriter) writeOptionalU32(value *uint32) {
	if value == nil {
		w.writeBool(false)
		return
	}
	w.writeBool(true)
	w.writeU32(*value)
}

func (w *binaryWriter) writeOptionalU64(value *uint64) {
	if value == nil {
		w.writeBool(false)
		return
	}
	w.writeBool(true)
	w.writeU64(*value)
}

func (w *binaryWriter) writeOptionalInt(value *int) error {
	if value == nil {
		w.writeBool(false)
		return nil
	}
	out, err := checkedUint32(*value, "int")
	if err != nil {
		return err
	}
	w.writeBool(true)
	w.writeU32(out)
	return nil
}

func (w *binaryWriter) writeOptionalString(value *string) error {
	if value == nil {
		w.writeBool(false)
		return nil
	}
	w.writeBool(true)
	return w.writeString(*value)
}

func (w *binaryWriter) writeOptionalBool(value *bool) {
	if value == nil {
		w.writeBool(false)
		return
	}
	w.writeBool(true)
	w.writeBool(*value)
}

type binaryReader struct {
	inner *bytes.Reader
}

func newBinaryReader(data []byte) *binaryReader {
	return &binaryReader{inner: bytes.NewReader(data)}
}

func (r *binaryReader) readU8() (uint8, error) {
	value, err := r.inner.ReadByte()
	if err != nil {
		return 0, decodeWrap(err, "read u8")
	}
	return value, nil
}

func (r *binaryReader) readBool() (bool, error) {
	value, err := r.readU8()
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (r *binaryReader) readU16() (uint16, error) {
	var raw [2]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, decodeWrap(err, "read u16")
	}
	return binary.BigEndian.Uint16(raw[:]), nil
}

func (r *binaryReader) readU32() (uint32, error) {
	var raw [4]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, decodeWrap(err, "read u32")
	}
	return binary.BigEndian.Uint32(raw[:]), nil
}

func (r *binaryReader) readU64() (uint64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, decodeWrap(err, "read u64")
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func (r *binaryReader) readF64() (float64, error) {
	value, err := r.readU64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(value), nil
}

func (r *binaryReader) readString() (string, error) {
	size, err := r.readU16()
	if err != nil {
		return "", err
	}
	raw, err := r.readFixedBytes(uint32(size))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (r *binaryReader) readBytes() ([]byte, error) {
	size, err := r.readU32()
	if err != nil {
		return nil, err
	}
	return r.readFixedBytes(size)
}

func (r *binaryReader) readOptionalU32() (*uint32, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readU32()
	return &value, err
}

func (r *binaryReader) readOptionalU64() (*uint64, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readU64()
	return &value, err
}

func (r *binaryReader) readOptionalInt() (*int, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readU32()
	if err != nil {
		return nil, err
	}
	out, err := intFromUint32(value)
	return &out, err
}

func (r *binaryReader) readOptionalString() (*string, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readString()
	return &value, err
}

func (r *binaryReader) readOptionalBool() (*bool, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readBool()
	return &value, err
}

func (r *binaryReader) readFixedBytes(size uint32) ([]byte, error) {
	length, err := intFromUint32(size)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, length)
	if length == 0 {
		return raw, nil
	}
	if _, err := io.ReadFull(r.inner, raw); err != nil {
		return nil, decodeWrap(err, "read bytes")
	}
	return raw, nil
}

func (r *binaryReader) ensureEOF() error {
	if r.inner.Len() != 0 {
		return oops.In("protocol").Code("binary_trailing_data").With("remaining", r.inner.Len()).New("binary body has trailing data")
	}
	return nil
}
