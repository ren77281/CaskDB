package db

import (
	"kv-go/index"
	"os"
)

// DB配置选项
type DBOptions struct {
	// 数据文件所在目录
	DirPath string
	// 数据文件阈值
	DataFileSize int64
	// 每次写入都持久化
	AlwaysSync bool
	// !AlwaysSync时，持久化的峰值
	BytesSync int64
	// index type
	Indexer index.IndexType
	// 首次加载时，是否使用mmap加载文件
	MMapStartUp bool
}

// 默认DB配置
var DefaultDBOptions = DBOptions{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	AlwaysSync:   false,
	Indexer:      index.BTreeType,
	BytesSync:    0,
	MMapStartUp:  true,
}

// 迭代器配置选项
type ItOptions struct {
	Prefix  []byte // key的前缀信息
	Reverse bool   // 是否反向遍历
}

// 默认迭代器配置
var DefaultItOptions = ItOptions{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatch配置选项
type WBOptions struct {
	Sync        bool // 是否序列化
	MaxWriteNum uint // 最大写入数量
}

// 默认WriteBatch配置
var DefaultWBOptions = WBOptions{
	Sync:        true,
	MaxWriteNum: 100000,
}
