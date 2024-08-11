package main

import (
	bitcask "kv-go/db"
	"fmt"
)

var DirPath string = "/home/kv-go/tmp"

func main() {
	opts := bitcask.DefaultDBOptions
	opts.DirPath = DirPath
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Print("val=", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}