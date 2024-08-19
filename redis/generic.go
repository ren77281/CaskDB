package redis

import (
	"time"
	bitcask "kv-go/db"
)

// TODO: 对于具有versionId的结构，如何删除其字段呢？
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	if len(key) == 0 {
		return 0, bitcask.ErrEmptyKey
	}
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, ErrEmptyValue
	}
	return encValue[0], nil
}

// 获取元数据，不存在则创建
func (rds *RedisDataStructure) getOrCreateMetaData(key []byte, dataType RedisDataType) (*metaData, error) {
	if len(key) == 0 {
		return nil, bitcask.ErrEmptyKey
	}
	encMetaData, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	exist := (err != bitcask.ErrKeyNotFound)

	var meta *metaData
	if exist {
		if len(encMetaData) == 0 {
			return nil, ErrEmptyMetaData
		}
		// 存在则检查过期时间，与数据类型
		meta := decodeMeta(encMetaData)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 如果存在但超时，设置为不存在
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		// 不存在则构造一个新的meta
		meta = &metaData{
			dataType:  dataType,
			versionId: time.Now().UnixNano(),
			expire:    0,
			numFields: 0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

// 查找meta，不会新建meta
func (rds *RedisDataStructure) findMetaData(key []byte, dataType RedisDataType) (*metaData, error) {
	if len(key) == 0 {
		return nil, bitcask.ErrEmptyKey
	}
	val, err := rds.db.Get(key)
	// 如果不存在或者有其他问题，则返回
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, ErrEmptyValue
	}
	// 存在，则判断类型与过期时间，再返回
	meta := decodeMeta(val)
	if meta.dataType != dataType {
		return nil, ErrWrongTypeOperation
	}
	if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return meta, nil
}
