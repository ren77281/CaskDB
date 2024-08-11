package db

import (
	"errors"
)

// 描述数据库运行时可能出现的错误
var (
	errEmptyKey              = errors.New("the key is empty")
	errUpdateIndexFailed     = errors.New("can not update index")
	errKeyNotFound           = errors.New("can not found the key")
	errDataFileNotFound      = errors.New("can not found data file")
	errDeletedKey            = errors.New("the key is deleted")
	errInvalidDirPath        = errors.New("directory path is invailded")
	errInvalidDataFileSize   = errors.New("data file size is invailded")
	errDataFileNameCorrupted = errors.New("data file name is corrupted")
)
