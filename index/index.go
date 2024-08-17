package index

import (
	"kv-go/data"
)

type IndexType = int8

const (
	// BTree index
	BTreeType IndexType = iota
	BPlusTreeType
	ARTreeType
)

// 索引的节点封装，将k-v封装成Item, 实现Less特征即可
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 索引特征
type Indexer interface {
	// 插入key-LogRecordPos
	Put(key []byte, pos *data.LogRecordPos) (bool, *data.LogRecordPos)
	// 根据key获取LogRecordPos
	Get(key []byte) *data.LogRecordPos
	// 删除key-LogRecordPos
	Delete(key []byte) (bool, *data.LogRecordPos)
	// 创建索引上的迭代器
	NewIterator(reverse bool) Iterator
	// 索引中的数据数量
	Size() int
	// 关闭索引
	Close() error
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
	// 关闭迭代器
	Close()
}

// TODO:添加更多index type
func NewIndexer(indexerType IndexType, dirPath string, sync bool) Indexer {
	switch indexerType {
	case BTreeType:
		return NewBTree()
	case BPlusTreeType:
		return NewBPlusTree(dirPath, sync)
	case ARTreeType:
		return NewARTree()
	default:
		return nil
	}
}
