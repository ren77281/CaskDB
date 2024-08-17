package utils

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

func CopyDir(src, dest string, exclude map[string]struct{}) error {
	// 目录不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 得到当前文件名
		fileName := strings.Replace(path, src, "", 1)
		if len(fileName) == 0 {
			return nil
		}
		// 检查是否需要排除
		tmp := filepath.Clean(info.Name())
		if _, ok := exclude[tmp]; ok {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// 处理目录
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}
		// 将文件拷贝到目标目录下
		// 先打开源文件
		sourceFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer sourceFile.Close()
		// 再调用io.Copy拷贝源文件
		destFile, err := os.Create(filepath.Join(dest, fileName))
		if err != nil {
			return err
		}
		_, err = io.Copy(destFile, sourceFile)
		if err != nil {
			return err
		}
		// 保留文件权限
		return os.Chmod(filepath.Join(dest, fileName), info.Mode())
	})
}