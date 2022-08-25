package e

import "errors"

var (
	ErrFileDoesNotExist = errors.New("文件不存在")
	ErrDuplicatedWrite  = errors.New("文件重复写")
	ErrSubDirTree       = errors.New("子目录不存在")

	ErrInternalBusy = errors.New("内部出错")
)
