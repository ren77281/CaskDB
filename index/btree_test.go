package index

import (
	"kv-go/data"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestBTreePut(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res3)
}

func TestBTreeGet(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res3)

	res4 := bt.Get([]byte("a"))
	t.Log(res4)
	assert.Equal(t, res4.Fid, uint32(1))
	assert.Equal(t, res4.Offset, int64(2))
	
	res5 := bt.Get(nil)
	assert.Equal(t, res5.Fid, uint32(2))
	assert.Equal(t, res5.Offset, int64(2))
}

func TestBTreeDelete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)

	res5 := bt.Delete([]byte("b"))
	assert.False(t, res5)
	
	res6 := bt.Get([]byte("a"))
	assert.Nil(t, res6)

	res7 := bt.Get(nil)
	assert.NotNil(t, res7)

	res8 := bt.Delete(nil)
	assert.True(t, res8)

	res9 := bt.Get(nil)
	assert.Nil(t, res9)
}