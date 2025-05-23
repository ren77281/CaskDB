package index

import (
	"bytes"
	"hash/crc32"
	"hash/fnv"
	"kv-go/data"
)

type entry struct {
	key []byte
	pos *data.LogRecordPos
}

type CuckooHash struct {
	size    int
	table1  []*entry
	table2  []*entry
	maxKick int
}

func (c *CuckooHash) NewIterator(reverse bool) Iterator {
	return nil
}

func (c *CuckooHash) Size() int {
	return c.size
}

func (c *CuckooHash) Close() error {
	return nil
}

func NewCuckooHash(size int) *CuckooHash {
	return &CuckooHash{
		size:    size,
		table1:  make([]*entry, size),
		table2:  make([]*entry, size),
		maxKick: 16, // 最大踢出次数
	}
}

func (ch *CuckooHash) hash1(key []byte) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % ch.size
}

func (ch *CuckooHash) hash2(key []byte) int {
	return int(crc32.ChecksumIEEE(key)) % ch.size
}

func equalKey(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Put 插入或更新 key
func (ch *CuckooHash) Put(key []byte, pos *data.LogRecordPos) (bool, *data.LogRecordPos) {
	h1 := ch.hash1(key)
	if e := ch.table1[h1]; e != nil && equalKey(e.key, key) {
		old := e.pos
		e.pos = pos
		return true, old
	}

	h2 := ch.hash2(key)
	if e := ch.table2[h2]; e != nil && equalKey(e.key, key) {
		old := e.pos
		e.pos = pos
		return true, old
	}

	newEntry := &entry{key: key, pos: pos}
	for i := 0; i < ch.maxKick; i++ {
		if ch.table1[h1] == nil {
			ch.table1[h1] = newEntry
			return false, nil
		}
		newEntry, ch.table1[h1] = ch.table1[h1], newEntry

		h2 = ch.hash2(newEntry.key)
		if ch.table2[h2] == nil {
			ch.table2[h2] = newEntry
			return false, nil
		}
		newEntry, ch.table2[h2] = ch.table2[h2], newEntry

		h1 = ch.hash1(newEntry.key)
	}

	// 插入失败（建议你扩容）
	return false, nil
}

// Get 查找 key
func (ch *CuckooHash) Get(key []byte) *data.LogRecordPos {
	if e := ch.table1[ch.hash1(key)]; e != nil && equalKey(e.key, key) {
		return e.pos
	}
	if e := ch.table2[ch.hash2(key)]; e != nil && equalKey(e.key, key) {
		return e.pos
	}
	return nil
}

// Delete 删除 key
func (ch *CuckooHash) Delete(key []byte) (bool, *data.LogRecordPos) {
	h1 := ch.hash1(key)
	if e := ch.table1[h1]; e != nil && equalKey(e.key, key) {
		ch.table1[h1] = nil
		return true, e.pos
	}

	h2 := ch.hash2(key)
	if e := ch.table2[h2]; e != nil && equalKey(e.key, key) {
		ch.table2[h2] = nil
		return true, e.pos
	}

	return false, nil
}

type CuckooIndexer struct{}

func NewCuckooIndexer() *CuckooIndexer {
	return &CuckooIndexer{}
}

func (c *CuckooIndexer) NewIterator(reverse bool) Iterator {
	return &CuckooIterator{}
}

func (c *CuckooIndexer) Size() int {
	return 0
}

func (c *CuckooIndexer) Close() error {
	return nil
}

type CuckooIterator struct{}

func (it *CuckooIterator) Rewind() {}

func (it *CuckooIterator) Seek(key []byte) {}

func (it *CuckooIterator) Next() {}

func (it *CuckooIterator) IsEnd() bool {
	return true
}

func (it *CuckooIterator) Key() []byte {
	return nil
}

func (it *CuckooIterator) Value() *data.LogRecordPos {
	return nil
}

func (it *CuckooIterator) Close() {}
