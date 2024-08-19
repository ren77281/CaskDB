package redis

import (
	"encoding/binary"
	"errors"
	bitcask "kv-go/db"
	"time"
)

type RedisDataType = byte

const (
	String RedisDataType = iota + 1
	Hash
	Set
	Zset
	List
)

var (
	ErrWrongTypeOperation    = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrEmptyValue            = errors.New("value is empty")
	ErrEmptyMetaData         = errors.New("meta data is empty")
	ErrNumFieldNotEqualValue = errors.New("fields and values are not equal in size")
)

type RedisDataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(opts bitcask.DBOptions) (*RedisDataStructure, error) {
	db, err := bitcask.Open(opts)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ==================== String 结构 ====================

func (rds *RedisDataStructure) Set(key, value []byte, ttl time.Duration) error {
	if len(value) == 0 || len(key) == 0 {
		return nil
	}
	// 对value进行编码：type + expire + payload
	var expire int64 = 0 // 0 表示永不过期
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	buf := make([]byte, 1+binary.MaxVarintLen64)
	// type编码
	buf[0] = String
	idx := 1
	// expire编码
	idx += binary.PutVarint(buf[idx:], expire)
	encValue := make([]byte, idx+len(value))
	// 连接得到最终的编码
	copy(encValue[:idx], buf)
	copy(encValue[idx:], value)
	// 调用存储引擎的Put接口
	if err := rds.db.Put(key, encValue); err != nil {
		return err
	}
	return nil
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	// 调用存储引擎的Get接口
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	// 解码encValue中的type
	typ := encValue[0]
	if typ != String {
		return nil, ErrWrongTypeOperation
	}
	// 解码encValue中的expire判断是否过期
	idx := 1
	expire, n := binary.Varint(encValue[idx:])
	idx += n
	if expire != 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[idx:], nil
}

// ==================== Hash 结构 ====================

func (rds *RedisDataStructure) HSet(key []byte, fields, values [][]byte) (int, error) {
	if len(key) == 0 {
		return 0, bitcask.ErrEmptyKey
	}
	if len(fields) != len(values) {
		return 0, ErrNumFieldNotEqualValue
	}
	// 获取元数据
	meta, err := rds.getOrCreateMetaData(key, Hash)
	if err != nil {
		return 0, err
	}
	cnt := 0
	n := len(fields)
	wb := rds.db.NewWriteBatch(bitcask.DefaultWBOptions)
	for i := 0; i < n; i++ {
		ok, err := rds.hSet(meta, key, fields[i], values[i], wb)
		if err != nil {
			return 0, err
		}
		if ok {
			cnt++
		}
	}
	if cnt != 0 {
		meta.numFields += uint32(cnt)
		if err := wb.Put(key, meta.encode()); err != nil {
			return 0, err
		}
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (rds *RedisDataStructure) hSet(meta *metaData, key, field, value []byte, wb *bitcask.WriteBatch) (bool, error) {
	// 构造field
	fieldKey := &hashField{
		key:       key,
		field:     field,
		versionId: meta.versionId,
	}
	encFieldKey := fieldKey.encode()
	// 查询是否存在field
	_, err := rds.db.Get(encFieldKey)
	exist := true
	// 不存在，更新meta
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			exist = false
		} else {
			return false, err
		}
	}
	if err = wb.Put(encFieldKey, value); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, bitcask.ErrEmptyKey
	}
	// 先获取元数据
	encMetaData, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	// 验证元数据的有效性
	if len(encMetaData) == 0 {
		return nil, ErrEmptyMetaData
	}
	meta := decodeMeta(encMetaData)
	if meta.numFields == 0 {
		return nil, nil
	}
	// 构造field
	fieldKey := &hashField{
		key: key,
		field: field,
		versionId: meta.versionId,
	}
	// 返回这个字段的查找结果
	return rds.db.Get(fieldKey.encode())
}

func (rds *RedisDataStructure) HDel(key []byte, fields [][]byte) (int, error) {
	if len(key) == 0 {
		return 0, bitcask.ErrEmptyKey
	}
	// 先获取元数据
	encMetaData, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	// 验证元数据的有效性
	if len(encMetaData) == 0 {
		return 0, ErrEmptyMetaData
	}
	meta := decodeMeta(encMetaData)
	// 过期的hash
	if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
		return 0, nil
	}
	// 没有field
	if meta.numFields == 0 {
		return 0, nil
	}
	cnt := 0
	wb := rds.db.NewWriteBatch(bitcask.DefaultWBOptions)
	for _, field := range fields {
		ok, err := rds.hDel(meta, key, field, wb)
		if err != nil {
			return 0, err
		}
		if ok {
			cnt++
		}
	}
	if cnt != 0 {
		meta.numFields -= uint32(cnt)
		if err := wb.Put(key, meta.encode()); err != nil {
			return 0, err
		}
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (rds *RedisDataStructure) hDel(meta *metaData, key, field []byte, wb *bitcask.WriteBatch) (bool, error) {
	// 构造field字段
	fieldKey := &hashField{
		key:       key,
		field:     field,
		versionId: meta.versionId,
	}
	encFieldKey := fieldKey.encode()
	_, err := rds.db.Get(encFieldKey)
	// 不存在则直接返回
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// 存在则删除field
	if err := wb.Delete(encFieldKey); err != nil {
		return false, err
	}
	return true, nil	
}

// ==================== Set结构 ====================
func (rds *RedisDataStructure) SAdd(key []byte, members [][]byte) (int, error) {
	// 查找元数据
	meta, err := rds.getOrCreateMetaData(key, Set)
	if err != nil {
		return 0, err
	}
	var cnt = 0
	// 开启writebatch
	wb := rds.db.NewWriteBatch(bitcask.DefaultWBOptions)
	for _, member := range members {
		ok, err := rds.sAdd(meta, key, member, wb)
		if err != nil {
			return 0, err
		}
		if ok {
			cnt++
		}
	}
	// 提交元数据和member的更新
	if cnt != 0 {
		meta.numFields += uint32(cnt)
		if err := wb.Put(key, meta.encode()); err != nil {
			return 0, err
		}
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (rds *RedisDataStructure) sAdd(meta *metaData, key, member []byte, wb *bitcask.WriteBatch) (bool, error) {
	if len(key) == 0 || len(member) == 0 {
		return false, bitcask.ErrEmptyKey
	}
	// 构造member
	memberKey := &setField{
		key:       key,
		member:    member,
		versionId: meta.versionId,
	}
	// 查找是否存在member
	encMemberKey := memberKey.encode()
	_, err := rds.db.Get(encMemberKey)
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			// 不存在则插入
			if err := wb.Put(encMemberKey, nil); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	if len(key) == 0 || len(member) == 0 {
		return false, bitcask.ErrEmptyKey
	}
	// 获取元数据
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	// 构造memberKey
	memberKey := &setField{
		key:       key,
		member:    member,
		versionId: meta.versionId,
	}
	_, err = rds.db.Get(memberKey.encode())
	if err != nil {
		if err == bitcask.ErrDataFileNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key []byte, members [][]byte) (int, error) {
	// 查找元数据
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return 0, err
	}
	if meta.numFields == 0 {
		return 0, nil
	}
	var cnt = 0
	// 开启writebatch
	wb := rds.db.NewWriteBatch(bitcask.DefaultWBOptions)
	for _, member := range members {
		ok, err := rds.sRem(meta, key, member, wb)
		if err != nil {
			return 0, err
		}
		if ok {
			cnt++
		}
	}
	// 提交元数据和member的更新
	if cnt != 0 {
		meta.numFields -= uint32(cnt)
		if err := wb.Put(key, meta.encode()); err != nil {
			return 0, err
		}
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (rds *RedisDataStructure) sRem(meta *metaData, key, member []byte, wb *bitcask.WriteBatch) (bool, error) {
	// 构造member
	memberKey := &setField{
		key:       key,
		member:    member,
		versionId: meta.versionId,
	}
	// 直接删除member，根据返回值判断member是否存在
	err := wb.Delete(memberKey.encode())
	if err != nil {
		if err == bitcask.ErrDataFileNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ==================== list ====================
func (rds *RedisDataStructure) LPush(key []byte, field []byte) (uint32, error) {
	return rds.push(key, field, true)
}

func (rds *RedisDataStructure) RPush(key []byte, field []byte) (uint32, error) {
	return rds.push(key, field, false)
}

func (rds *RedisDataStructure) push(key []byte, field []byte, head bool) (uint32, error) {
	// 先查找或创建元数据
	meta, err := rds.getOrCreateMetaData(key, List)
	if err != nil {
		return 0, err
	}
	// 再根据元数据构造node
	liseNode := &listNode{
		key: key,
		versionId: meta.versionId,
	}
	// 先移动head/tail，再将其作为idx
	meta.numFields++
	if head {
		meta.head--
		liseNode.idx = meta.head
	} else {
		meta.tail++
		liseNode.idx = meta.tail
	}
	// 新建wb更新元数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWBOptions)
	if err := wb.Put(liseNode.encode(), field); err != nil {
		return 0, err
	}
	if err := wb.Put(key, meta.encode()); err != nil {
		return 0, err
	}
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return uint32(meta.tail-meta.head), nil
}

func (rds *RedisDataStructure) LPop(key []byte, field []byte) ([]byte, error) {
	return rds.pop(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte, field []byte) ([]byte, error) {
	return rds.pop(key, false)
}

func (rds *RedisDataStructure) pop(key []byte, head bool) ([]byte, error) {
	// 先查找元数据
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}
	// meta的numFields为0，则直接返回
	if meta.numFields == 0 {
		return nil, nil
	}
	// 构造listNode，直接删除
	listNode := &listNode{
		key: key,
		versionId: meta.versionId,
	}
	// 移动前的head/tail是要被删除的元素位置
	meta.numFields--
	if head {
		listNode.idx = meta.head
		meta.head++
	} else {
		listNode.idx = meta.tail
		meta.tail--
	}
	// 判断是否存在
	val, err := rds.db.Get(listNode.encode())
	if err != nil {
		return nil, err
	}
	// 这里可以不删除，如果后续继续push这些field将被覆盖
	// 或者写一个后台清理线程TODO
	if err := rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return val, nil
}
