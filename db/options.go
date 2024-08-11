package db

import (
	"kv-go/index"
	"os"
)

// 用户的DB配置选项
type DBOptions struct {
	// 数据文件所在目录
	DirPath string
	// 数据文件阈值
	DataFileSize int64
	// 每次写入都持久化
	AlwaysSync bool
	// index type
	Indexer index.IndexType
}

// 默认DB配置
var DefaultDBOptions = DBOptions{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	AlwaysSync:   false,
	Indexer:      index.BTreeType,
}

// 用户的迭代器配置选项
type ItOptions struct {
	Prefix  []byte // key的前缀信息
	Reverse bool   // 是否反向遍历
}

// 默认迭代器配置
var DefaultItOptions = ItOptions{
	Prefix: nil,
	Reverse: false,
}
