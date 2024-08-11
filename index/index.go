package index

import (
	"kv-go/data"
)

type IndexType = int8

const (
	// BTree index
	BTreeType IndexType = iota
)

// 索引特征
type Indexer interface {
	// 插入key-LogRecordPos
	Put(key []byte, pos *data.LogRecordPos) bool
	// 根据key获取LogRecordPos
	Get(key []byte) *data.LogRecordPos
	// 删除key-LogRecordPos
	Delete(key []byte) bool
	// 创建索引上的迭代器
	NewIterator(reverse bool) Iterator
}

// 索引的迭代器特征
type Iterator interface {
	// 使迭代器指向起点
	Rewind()
	// 找到key值满足大于等于/小于等于关系的位置，开始迭代
	Seek(key []byte)
	// 使迭代器往后遍历
	Next()
	// 判断迭代器是否遍历完成
	IsEnd() bool
	// 取key
	Key() []byte
	// 取value
	Value() *data.LogRecordPos
	// 索引结构中元素的数量
	Size() int
	// 关闭迭代器
	Close()
}

// TODO:添加更多index type
func NewIndexer(indexerType IndexType) Indexer {
	switch indexerType {
	case BTreeType:
		return NewBTree()
	default:
		return nil
	}
}