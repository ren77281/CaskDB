package main

import (
	bitcask "kv-go/db"
	bitcask_redis "kv-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

var addr = "127.0.0.1:8888"

type BitcaskServer struct {
	dbs map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu sync.RWMutex
}

func main() {
	// 创建rds结构
	rds, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultDBOptions)
	if err != nil {
		panic(err)
	}
	// 创建server
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure, 0),
	}
	bitcaskServer.dbs[0] = rds
	// 创建redis服务端(使用redcon框架)
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accpet, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server is running...")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}