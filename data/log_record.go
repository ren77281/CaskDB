package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal = iota
	LogRecordDeleted
	LogRecordFinished
)

// header的最大size: 4 + 1 + 5 + 5
const maxLogRecordHeadSize = binary.MaxVarintLen32*2 + 5

// LogRecord 描述k-v记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Typ   LogRecordType
}

// +---------+----------+-------------------+--------------------+
// |   crc   |   type   |     key size      |     value size     |
// +---------+----------+-------------------+--------------------+
//    4 byte    1 byte   varint(max 5 byte)   varint(max 5 byte)

// LogRecordHeader LogRecored的头部信息
type LogRecordHeader struct {
	crc           uint32        // crc校验值
	logRecordType LogRecordType // 记录的类型(是否被删除)
	keySize       uint32        // key的大小
	valueSize     uint32        // value的大小
}

// LogRecordPos 描述记录在磁盘中的具体位置
type LogRecordPos struct {
	Fid    uint32 // Fid 唯一标识文件
	Offset int64  // Offset 记录在文件中的偏移量
}

// 存储WriteBatch的record信息
type WBLogRecord struct {
	Key []byte
	Pos *LogRecordPos
	Typ LogRecordType
}

// encodeRecordPos 将LogRecordPos序列化成[]byte
func EncodeLogRecordPos(logRecordPos *LogRecordPos) []byte {
	encLogRecordPos := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var idx = 0
	idx += binary.PutUvarint(encLogRecordPos[idx:], uint64(logRecordPos.Fid))
	idx += binary.PutVarint(encLogRecordPos[idx:], logRecordPos.Offset)
	return encLogRecordPos[:idx]
}

func DecodeLogRecordPos(datas []byte) *LogRecordPos {
	var idx = 0
	fid, n := binary.Uvarint(datas[idx:])
	idx += n
	offset, _ := binary.Varint(datas[idx:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
	}
}

// EncodeRecord 将LogRecord序列化成[]byte
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// encode header部分
	header := make([]byte, maxLogRecordHeadSize)
	header[4] = logRecord.Typ
	// encode key size, value size
	var idx = 5
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Key)))
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Value)))
	// 计算logRecord的总长度并定义bytes存储logRecord
	sz := idx + len(logRecord.Key) + len(logRecord.Value)
	encodeRecord := make([]byte, sz)
	// 拷贝已经encode完成的header
	copy(encodeRecord[:idx], header[:idx])
	// 拷贝已经是byte的key-value
	copy(encodeRecord[idx:], logRecord.Key)
	copy(encodeRecord[idx+len(logRecord.Key):], logRecord.Value)
	// 对 crc 字段外的数据，计算crc检验和
	crc := crc32.ChecksumIEEE(encodeRecord[4:])
	binary.LittleEndian.PutUint32(encodeRecord[:4], crc)
	return encodeRecord, int64(sz)
}

// 解码LogRecord的Header信息，同时返回 Header 长度
func decodeLogRecordHeader(datas []byte) (*LogRecordHeader, int64) {
	// 固定字段的长度为5，若datas < 5则无法解码
	if len(datas) < 5 {
		return nil, 0
	}
	// 构造 LogRecordHeader
	logRecordHeader := &LogRecordHeader{
		crc:           binary.LittleEndian.Uint32(datas[:4]),
		logRecordType: datas[4],
	}
	var idx = 5
	// 获取key size
	keySize, n := binary.Varint(datas[idx:])
	logRecordHeader.keySize = uint32(keySize)
	idx += n
	// 获取value size
	valueSize, n := binary.Varint(datas[idx:])
	logRecordHeader.valueSize = uint32(valueSize)
	idx += n
	return logRecordHeader, int64(idx)
}

// getLogRecordCRC 获取校验值
func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}
