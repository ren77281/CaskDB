package db

import (
	"errors"
)

// 描述数据库运行时可能出现的错误
var (
	ErrEmptyKey              = errors.New("the key is empty")
	ErrUpdateIndexFailed     = errors.New("can not update index")
	ErrKeyNotFound           = errors.New("can not found the key")
	ErrDataFileNotFound      = errors.New("can not found data file")
	ErrDeletedKey            = errors.New("the key is deleted")
	ErrInvalidDirPath        = errors.New("directory path is invailded")
	ErrInvalidDataFileSize   = errors.New("data file size is invailded")
	ErrDataFileNameCorrupted = errors.New("data file name is corrupted")
	ErrExceedMaxWriteNum     = errors.New("too many writes")
	ErrInvalidRecordType     = errors.New("invalid record type exists")
	ErrDBMerging             = errors.New("db is merging")
	ErrDBUsing               = errors.New("db is using")
	ErrInvalidMergeRatio     = errors.New("merge ratio must be in ther range [0, 1]")
	ErrMergeRatioUnreached   = errors.New("merge ratio unreach")
	ErrDiskSpaceNotEnough    = errors.New("disk space not enough")
)
