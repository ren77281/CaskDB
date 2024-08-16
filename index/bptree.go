package index

import (
	"kv-go/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	indexByBPTreeFileName = "index-bptree"
	bucketName            = []byte("index-bucket")
)

// 封装bolt的B+树索引
type BPlusTree struct {
	tree *bbolt.DB
}

type BPlusTreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor // 当前迭代器在bucket中的游标
	reverse bool          // 是否反向等待
	key     []byte
	val     []byte
}

func NewBPlusTree(dirPath string, isSync bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !isSync
	bptree, err := bbolt.Open(filepath.Join(dirPath, indexByBPTreeFileName), 0644, opts)
	if err != nil {
		return nil
	}
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		return nil
	}
	return &BPlusTree{tree: bptree}
}

// 插入key-LogRecordPos
func (bp *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	// 不允许插入空的key
	if len(key) == 0 {
		return false
	}
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}
		bucket.Put(key, data.EncodeLogRecordPos(pos))
		return nil
	}); err != nil {
		return false
	}
	return true
}

// 根据key获取LogRecordPos
func (bp *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos = nil
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}
		val := bucket.Get(key)
		if len(val) != 0 {
			pos = data.DecodeLogRecordPos(val)
		}
		return nil
	}); err != nil {
		return nil
	}
	return pos
}

// 删除key-LogRecordPos
func (bp *BPlusTree) Delete(key []byte) bool {
	var ok bool = false
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}
		if val := bucket.Get(key); len(val) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		return false
	}
	return ok
}

func (bp *BPlusTree) Size() int {
	var sz int
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}
		sz = bucket.Stats().KeyN
		return nil
	}); err != nil {
		return -1
	}
	return sz
}

func (bp *BPlusTree) Close() error {
	return bp.tree.Close()
}

// 创建索引上的迭代器
func (bp *BPlusTree) NewIterator(reverse bool) Iterator {
	return NewBPlusTreeIterator(bp, reverse)
}

func NewBPlusTreeIterator(bp *BPlusTree, reverse bool) *BPlusTreeIterator {
	tx, err := bp.tree.Begin(false)
	if err != nil {
		return nil
	}
	it := &BPlusTreeIterator{
		tx: tx,
		cursor: tx.Bucket(bucketName).Cursor(),
		reverse: reverse,
	}
	it.Rewind()
	return it
}

// 使迭代器指向起点
func (it *BPlusTreeIterator) Rewind() {
	if it.reverse {
		it.key, it.val = it.cursor.Last()
	} else {
		it.key, it.val = it.cursor.First()
	}
}

// 找到key值满足大于等于/小于等于关系的位置，开始迭代
func (it *BPlusTreeIterator) Seek(key []byte) {
	it.key, it.val = it.cursor.Seek(key)
	if it.reverse && len(it.key) == 0 {
		it.key, it.val = it.cursor.Last()
	}
}

// 使迭代器往后遍历
func (it *BPlusTreeIterator) Next() {
	if it.reverse {
		it.key, it.val = it.cursor.Prev()
	} else {
		it.key, it.val = it.cursor.Next()
	}
}

// 判断迭代器是否遍历完成
func (it *BPlusTreeIterator) IsEnd() bool {
	return len(it.key) == 0
}

// 取key
func (it *BPlusTreeIterator) Key() []byte {
	return it.key
}

// 取value
func (it *BPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(it.val)
}

// 关闭迭代器
func (it *BPlusTreeIterator) Close() {
	it.tx.Rollback()
}
