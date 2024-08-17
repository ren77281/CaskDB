package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)


func DirSize(dirPath string) (int64, error) {
	var sz int64 = 0
	err := filepath.Walk(dirPath, func (_ string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			sz += info.Size()
		}
		return nil
	})
	return sz, err
}

func AvailableDiskSize() (int64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
