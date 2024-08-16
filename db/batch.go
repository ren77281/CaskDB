package db

import (
	"encoding/binary"
	"kv-go/data"
	"sync"
	"sync/atomic"
)

const (
	zeroWbId uint64 = 0
	wbIbKey  string = "wbidkey"
)

var wbFinKey = []byte("wb-finsh") // 最后提交的finsh record的key

type WriteBatch struct {
	opts          WBOptions                  // 配置选项
	db            *DB                        // 保存DB实例
	pendingWrites map[string]*data.LogRecord // 暂存事务的写入操作
	mu            *sync.Mutex                // 保证WriteBatch操作的原子性，用户可能用多个线程访问WriteBatch

}

// 写入数据到暂存区
func (writeBatch *WriteBatch) Put(key []byte, value []byte) error {
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()
	// key不能为空
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// TODO: LogRecord与pending都存储了key，是否不够优雅？
	writeBatch.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
		Typ:   data.LogRecordNormal,
	}
	return nil
}

// 从暂存区中删除数据
func (writeBatch *WriteBatch) Delete(key []byte) error {
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// 先判断key是否存在
	logRecordPos := writeBatch.db.index.Get(key)
	if logRecordPos == nil {
		delete(writeBatch.pendingWrites, string(key))
		return nil
	}
	// 如果key存在，更新一条delete record即可
	writeBatch.pendingWrites[string(key)] = &data.LogRecord{
		Key: key,
		Typ: data.LogRecordDeleted,
	}
	return nil
}

// 将暂存区中的数据提交到data file
func (writeBatch *WriteBatch) Commit() error {
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()
	// 空事务无需提交
	if len(writeBatch.pendingWrites) == 0 {
		return nil
	}
	// 不能超过最多能够写入的数量
	if uint(len(writeBatch.pendingWrites)) > writeBatch.opts.MaxWriteNum {
		return ErrExceedMaxWriteNum
	}
	// 修改DB时，需要保证串行化
	writeBatch.db.mu.Lock()
	defer writeBatch.db.mu.Unlock()
	// 获取wbId
	id := atomic.AddUint64(&writeBatch.db.wbId, 1)
	// TODO:如果先大量更新，然后再全部删除，那么维护index时，也先更新再删除，是否是无效操作？
	// 还是说这里的两个map不够优雅？
	updatePos := make(map[string]*data.LogRecordPos)
	deletePos := make(map[string]struct{})
	// 将writes写入磁盘
	for _, record := range writeBatch.pendingWrites {
		realKey := record.Key
		// 磁盘中的key需要带有id
		record.Key = serializeKeyId(realKey, id)
		// 向磁盘中的data file追加数据
		logRecordPos, err := writeBatch.db.appendLogRecord(record)
		if err != nil {
			return err
		}
		// 暂存pos信息，所有record追加完成后，统一更新index
		strRealKey := string(realKey)
		if record.Typ == data.LogRecordNormal {
			updatePos[strRealKey] = logRecordPos
		} else if record.Typ == data.LogRecordDeleted {
			delete(updatePos, strRealKey)
			deletePos[strRealKey] = struct{}{}
		} else {
			return ErrInvalidRecordType
		}
	}
	// 最后写入一条finish record
	finLogRecord := &data.LogRecord{
		Key: serializeKeyId(wbFinKey, id),
		Typ: data.LogRecordFinished,
	}
	_, err := writeBatch.db.appendLogRecord(finLogRecord)
	if err != nil {
		return nil
	}
	// 根据配置信息决定是否持久化(这里不能调用db.Sync(), 因为死锁)
	if writeBatch.opts.Sync {
		if err := writeBatch.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	// 所有record写入磁盘后，更新索引，TODO:[]byte->string的转换开销下，但顶不住频繁的转换
	for key, pos := range updatePos {
		writeBatch.db.index.Put([]byte(key), pos)
	}
	for key := range deletePos {
		writeBatch.db.index.Delete([]byte(key))
	}
	// 清空wb中暂存的record
	writeBatch.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 将key与id序列化到一起
func serializeKeyId(key []byte, id uint64) []byte {
	serId := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(serId[:], id)
	keyId := make([]byte, n+len(key))
	copy(keyId[:n], serId[:n])
	copy(keyId[n:], key)
	return keyId
}

// 解析序列化后的KeyId
func parseKeyId(keyId []byte) ([]byte, uint64) {
	id, n := binary.Uvarint(keyId)
	key := keyId[n:]
	return key, id
}
