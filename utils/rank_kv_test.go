package utils

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestGetKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, GetTestKey(i))
	}
}

func TestGetValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, GetTestValue(100))
	}
}