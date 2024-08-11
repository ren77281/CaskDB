package db

import (
	"bytes"
	"kv-go/index"
)

type DBIterator struct {
	indexIter index.Iterator // index迭代器，用来在内存中遍历key
	db        *DB         // DB实例，用来访问磁盘中的value
	opts      ItOptions   // 迭代器配置选项
}

func (dbIter *DBIterator) NextByPrefix() {
	var n = len(dbIter.opts.Prefix)
	if n != 0 {
		return
	}
	// 往后遍历，找到一个前缀相同的key
	for ; !dbIter.indexIter.IsEnd(); dbIter.indexIter.Next() {
		if n != 0 {
			var key = dbIter.indexIter.Key()
			if len(key) >= n && bytes.Equal(dbIter.opts.Prefix, key[:n]) {
				break
			}
		}
	}
}

func (dbIter *DBIterator) Rewind() {
	dbIter.indexIter.Rewind()
	dbIter.NextByPrefix()
}

func (dbIter *DBIterator) Seek(key []byte) {
	dbIter.indexIter.Seek(key)
	dbIter.NextByPrefix()
}

func (dbIter *DBIterator) Next() {
	dbIter.indexIter.Next()
	dbIter.NextByPrefix()
}

func (dbIter *DBIterator) IsEnd() bool {
	return dbIter.indexIter.IsEnd()
}

func (dbIter *DBIterator) Key() []byte {
	return dbIter.indexIter.Key()
}

func (dbIter *DBIterator) Value() ([]byte, error) {
	logRecordPos := dbIter.indexIter.Value()
	dbIter.db.mu.RLock()
	defer dbIter.db.mu.RUnlock()
	// 通过LogRecordPos获取磁盘中的value
	val, err := dbIter.db.GetValueByPos(logRecordPos)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (dbIter *DBIterator) Close() {
	dbIter.indexIter.Close()
}