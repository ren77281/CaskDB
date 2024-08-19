package redis

import (
	"encoding/binary"
	"math"
)

// ==================== metaData ====================
type metaData struct {
	dataType  RedisDataType // key对应的数据类型
	versionId int64         // 用来快速删除的版本号
	expire    int64         // 过期时间
	numFields uint32        // 字段的数量
	head      uint64        // 如果key为List类型，需要head字段
	tail      uint64        // 如果key为List类型，需要tail字段
}

var (
	baseMetaDataSize         = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraMetaDataSize        = binary.MaxVarintLen64 * 2
	initialListMark   uint64 = math.MaxUint64 / 2
)

func (meta *metaData) encode() []byte {
	sz := baseMetaDataSize
	if meta.dataType == List {
		sz += extraMetaDataSize
	}
	buf := make([]byte, sz)
	buf[0] = meta.dataType
	idx := 1
	idx += binary.PutVarint(buf[idx:], meta.versionId)
	idx += binary.PutVarint(buf[idx:], meta.expire)
	idx += binary.PutUvarint(buf[idx:], uint64(meta.numFields))
	if meta.dataType == List {
		idx += binary.PutUvarint(buf[idx:], meta.head)
		idx += binary.PutUvarint(buf[idx:], meta.tail)
	}
	return buf[:idx]
}

func decodeMeta(datas []byte) *metaData {
	dataType := datas[0]
	idx := 1
	versionId, n := binary.Varint(datas[idx:])
	idx += n
	expire, n := binary.Varint(datas[idx:])
	idx += n
	numFields, n := binary.Uvarint(datas[idx:])
	idx += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(datas[idx:])
		idx += n
		tail, _ = binary.Uvarint(datas[idx:])
	}
	return &metaData{
		dataType:  dataType,
		versionId: versionId,
		expire:    expire,
		numFields: uint32(numFields),
		head:      head,
		tail:      tail,
	}
}

// ==================== hash ====================
type hashField struct {
	key       []byte
	field     []byte
	versionId int64
}

// 编码field，作为底层存储引擎的key
func (hf *hashField) encode() []byte {
	buf := make([]byte, len(hf.key)+len(hf.field)+binary.MaxVarintLen64)
	idx := 0
	copy(buf[idx:], hf.key)
	idx += len(hf.key)
	copy(buf[idx:], hf.field)
	idx += len(hf.field)
	idx += binary.PutVarint(buf[idx:], hf.versionId)
	return buf[:idx]
}

// ==================== set ====================
type setField struct {
	key       []byte
	versionId int64
	member    []byte
}

func (sf *setField) encode() []byte {
	buf := make([]byte, len(sf.key)+len(sf.member)+binary.MaxVarintLen64)
	idx := 0
	copy(buf[idx:], sf.key)
	idx += len(sf.key)
	idx += binary.PutVarint(buf[idx:], sf.versionId)
	copy(buf[idx:], sf.member)
	idx += len(sf.member)
	return buf[:idx]
}

// ==================== list ====================
type listNode struct {
	key       []byte
	versionId int64
	idx       uint64
}

func (ln *listNode) encode() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2+len(ln.key))
	copy(buf, ln.key)
	idx := len(ln.key)
	idx += binary.PutVarint(buf[idx:], ln.versionId)
	idx += binary.PutUvarint(buf[idx:], ln.idx)
	return buf[:idx]
}