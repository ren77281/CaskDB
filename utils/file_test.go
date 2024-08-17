package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	sz, err := DirSize("/home/kv-go")
	assert.Nil(t, err)
	fmt.Println(sz)
	assert.GreaterOrEqual(t, sz, int64(1))
}

func TestDiskSize(t *testing.T) {
	sz, err := AvailableDiskSize()
	assert.Nil(t, err)
	fmt.Println(sz)
	assert.GreaterOrEqual(t, sz, int64(1))
}