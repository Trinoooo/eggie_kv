package utils

import (
	"errors"
	errs "github.com/Trinoooo/eggie_kv/errs"
	"os"
	"path"
)

func CheckAndCreateFile(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	dir, _ := path.Split(filePath)
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(dir, 0770); err != nil {
			return nil, errs.NewMkdirErr().WithErr(err)
		}
	} else if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return nil, errs.NewFileNoPermissionErr().WithErr(err)
		}
		return nil, errs.NewFileStatErr().WithErr(err)
	}

	fd, err := os.OpenFile(filePath, flag, perm)
	if err != nil {
		return nil, errs.NewOpenFileErr().WithErr(err)
	}
	return fd, nil
}
