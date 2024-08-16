package index

import (
	"bytes"
	"kv-go/data"
	"sort"
	"sync"

	"github.com/google/btree"
)

// 封装google的btree，TODO: 是否有必要加锁？
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// 封装btree的迭代器(只读？)
type BTreeIterator struct {
	datas   []*Item // 类似快照，迭代器保存了当前数据库的所有value
	idx     int     // 用来访问vals, 表示当前遍历到的位置
	reverse bool    // 是否反向遍历
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	item := bt.tree.Get(it)
	if item == nil {
		return nil
	}
	return item.(*Item).pos
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if len(key) == 0 {
		return false
	}
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	item := bt.tree.Delete(it)
	bt.lock.Unlock()
	return item != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (l *Item) Less(r btree.Item) bool {
	return bytes.Compare(l.key, r.(*Item).key) == -1
}

// 返回迭代器接口
func (bt *BTree) NewIterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return NewBTreeIterator(bt.tree, reverse)
}

// 创建btree的迭代器
func NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var i = 0
	datas := make([]*Item, tree.Len())
	// 创建闭包，以遍历btree
	saveItems := func (item btree.Item) bool {
		datas[i] = item.(*Item)
		i++
		return true
	}
	// 根据reverse决定遍历btree的方向
	if !reverse {
		tree.Ascend(saveItems)
	} else {
		tree.Descend(saveItems)
	}
	// 构造迭代器并返回
	return &BTreeIterator{
		datas: datas,
		idx: 0,
		reverse: reverse,
	}
}

func (it *BTreeIterator) Rewind() {
	it.idx = 0
}

func (it *BTreeIterator) Key() []byte {
	return it.datas[it.idx].key
}

func (it *BTreeIterator) Value() *data.LogRecordPos {
	return it.datas[it.idx].pos
}

func (it *BTreeIterator) Next() {
	it.idx++
}

func (it *BTreeIterator) IsEnd() bool {
	return it.idx >= len(it.datas)
}

func (it *BTreeIterator) Close() {
	it.datas = nil
}

func (it *BTreeIterator) Seek(key []byte) {
	if !it.reverse {
		// sort.Search是二分查找，闭包将把数组划分成两个区间，Search将返回区间的分界点
		it.idx = sort.Search(len(it.datas), func(i int) bool {
			return bytes.Compare(it.datas[i].key, key) >= 0
		})
	} else {
		it.idx = sort.Search(len(it.datas), func(i int) bool {
			return bytes.Compare(it.datas[i].key, key) <= 0
		})
	}
}