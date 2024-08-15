package data

import (
	"hash/crc32"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestEncodeNormal(t *testing.T) {
	// 构造正常的LogRecord
	logRecord := &LogRecord{
		Key: []byte("abcd"),
		Value: []byte("1111"),
		Typ: LogRecordNormal,
	}
	// encode
	b1, sz := EncodeLogRecord(logRecord)
	assert.NotNil(t, b1)
	assert.Greater(t, sz, int64(5))
	assert.NotEqual(t, sz, 0)
	// value为空
	logRecord2 := &LogRecord{
		Key: []byte("abc"),
		Typ: LogRecordNormal,
	}
	b2, sz := EncodeLogRecord(logRecord2)
	assert.NotNil(t, b2)
	assert.Greater(t, sz, int64(5))
	assert.NotEqual(t, sz, 0)
	// 被删除
	logRecord3 := &LogRecord{
		Key: []byte("ab"),
		Value: []byte("aaaa"),
		Typ: LogRecordDeleted,
	}
	b3, sz := EncodeLogRecord(logRecord3)
	assert.NotNil(t, b3)
	assert.Greater(t, sz, int64(5))
	assert.NotEqual(t, sz, 0)
}

func TestDecodeNormal(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, byte(LogRecordNormal), h1.logRecordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, byte(LogRecordNormal), h2.logRecordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, byte(LogRecordDeleted), h3.logRecordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Typ:  LogRecordNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Typ:  LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Typ:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}