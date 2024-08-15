package index

import (
	"kv-go/data"
	"sync"
	"sort"
	"bytes"

	goart "github.com/plar/go-adaptive-radix-tree"
)

type ARTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

type ARTIterator struct {
	datas   []*Item // 类似快照，迭代器保存了当前数据库的所有value
	idx     int     // 用来访问vals, 表示当前遍历到的位置
	reverse bool    // 是否反向遍历
}

func NewARTree() *ARTree {
	return &ARTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// 插入key-LogRecordPos
func (art *ARTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

// 根据key获取LogRecordPos
func (art *ARTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	if res, ok := art.tree.Search(key); ok {
		return res.(*data.LogRecordPos)
	}
	return nil
}

// 删除key-LogRecordPos
func (art *ARTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, ok := art.tree.Delete(key)
	return ok
}

func (art *ARTree) Size() int {
	return art.tree.Size()
}

func (art *ARTree) Close() error {
	return nil
}

// 创建索引上的迭代器
func (art *ARTree) NewIterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art, reverse)
}

func NewARTIterator(art *ARTree, reverse bool) *ARTIterator {
	var i int = 0
	if reverse {
		i = art.Size() - 1
	}
	datas := make([]*Item, 0)
	saveItems := func (item goart.Node) bool {
		key := item.Key()
		val := item.Value().(*data.LogRecordPos)
		datas = append(datas, &Item{
			key: key,
			pos: val,
		})
		if reverse {
			i--
		} else {
			i++
		}
		return true
	}
	art.tree.ForEach(saveItems)
	return &ARTIterator{
		datas: datas,
		idx: 0,
		reverse: reverse,
	}
}

// 使迭代器指向起点
func (it *ARTIterator) Rewind() {
	it.idx = 0
}

// 找到key值满足大于等于/小于等于关系的位置，开始迭代
func (it *ARTIterator) Seek(key []byte) {
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

// 使迭代器往后遍历
func (it *ARTIterator) Next() {
	it.idx++
}
// 判断迭代器是否遍历完成
func (it *ARTIterator) IsEnd() bool {
	return it.idx >= len(it.datas)
}
// 取key
func (it *ARTIterator) Key() []byte {
	return it.datas[it.idx].key
}
// 取value
func (it *ARTIterator) Value() *data.LogRecordPos {
	return it.datas[it.idx].pos
}
// 关闭迭代器
func (it *ARTIterator) Close() {
	it.datas = nil
}