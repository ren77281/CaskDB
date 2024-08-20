package main

import (
	"fmt"
	bitcask "kv-go/db"
	bitcask_redis "kv-go/redis"
	"log"
	"strings"

	"github.com/tidwall/redcon"
)

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

// cmdHandle是对redis数据结构提供的方法的封装
// 主要是重组args，使其满足底层方法的形参要求
type cmdHandle func(client *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandle{
	"set":       set,
	"get":       get,
	"del":       del,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"quit":      nil,
	"ping":      nil,
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	// 获取用户输入的命令
	command := strings.ToLower(string(cmd.Args[0]))
	// 将命令转换成参数
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("unsupport command '" + command + "'")
		return
	}
	// 根据命令匹配对应的函数
	switch command {
	case "quit":
		_  = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		// 执行函数需要client
		client, ok := conn.Context().(*BitcaskClient)
		if !ok {
			log.Println("invalid BitcaskClient")
			conn.WriteError("system error")
			return
		}
		// 执行函数
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		// 将执行结果返回给客户端
		conn.WriteAny(res)
	}
}

func newNumberError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for command '%s'", cmd)
}

func del(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newNumberError("del")
	}
	if err := client.db.Del(args[0]); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

// ==================== String ====================
// TODO:超时设置
func set(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("set")
	}
	if err := client.db.Set(args[0], args[1], 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newNumberError("get")
	}
	val, err := client.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return val, nil
}

// ==================== Hash ====================
func hset(client *BitcaskClient, args [][]byte) (interface{}, error) {
	n := len(args)
	if n % 2 != 1 {
		return nil, newNumberError("hset")
	}
	// 重组命令中的field与value
	fields := make([][]byte, 0, n / 2)
	values := make([][]byte, 0, n / 2)
	for i := 1; i < n; i += 2 {
		fields = append(fields, args[i])
		values = append(values, args[i+1])
	}
	cnt, err := client.db.HSet(args[0], fields, values)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cnt), nil
}

func hget(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("hget")
	}
	val, err := client.db.HGet(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return val, nil
}

func hdel(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newNumberError("hdel")
	}
	cnt, err := client.db.HDel(args[0], args[1:])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cnt), nil
}

// ==================== Set ====================
func sadd(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newNumberError("sadd")
	}
	cnt, err := client.db.SAdd(args[0], args[1:])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cnt), nil
}

func sismember(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("sismember")
	}
	isMember, err := client.db.SIsMember(args[0], args[1])
	if err != nil {
		return nil, err
	}
	if isMember {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func srem(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newNumberError("srem")
	}
	cnt, err := client.db.SRem(args[0], args[1:])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(cnt), nil
}

// ==================== List ====================
func lpush(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("lpush")
	}
	sz, err := client.db.LPush(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(sz), nil
}

func rpush(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("rpush")
	}
	sz, err := client.db.RPush(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(sz), nil
}

// TODO:LPOP key [count], count功能
func lpop(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("lpop")
	}
	val, err := client.db.LPop(args[0])
	if err != nil {
		return nil, err
	}
	return val, nil
}

func rpop(client *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newNumberError("rpop")
	}
	val, err := client.db.RPop(args[0])
	if err != nil {
		return nil, err
	}
	return val, nil
}