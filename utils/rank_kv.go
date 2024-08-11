package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	rander = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(x int) []byte {
	return []byte(fmt.Sprintf("go-kv-key-%09d", x))
}

func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rander.Intn(len(letters))]
	}	
	return []byte("go-kv-value-" + string(b))
}