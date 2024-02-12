package utils

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	"os"
	"path"
)

func CheckAndCreateFile(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	dir, _ := path.Split(filePath)
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(dir, 0770); err != nil {
			return nil, consts.NewMkdirErr().WithErr(err)
		}
	} else if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return nil, consts.NewFileNoPermissionErr().WithErr(err)
		}
		return nil, consts.NewFileStatErr().WithErr(err)
	}

	fd, err := os.OpenFile(filePath, flag, perm)
	if err != nil {
		return nil, consts.NewOpenFileErr().WithErr(err)
	}
	return fd, nil
}
