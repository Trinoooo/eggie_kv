package utils

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
)

func CheckAndCreateFile(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	dir, _ := path.Split(filePath)
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(dir, 0770); err != nil {
			log.Error("mkdir failed. err:", err)
			return nil, consts.MkdirErr
		}
	} else if err != nil {
		log.Error("check dir stat err:", err)
		if errors.Is(err, os.ErrPermission) {
			return nil, consts.FileNoPermissionErr
		}
		return nil, consts.DirStatErr
	}

	fd, err := os.OpenFile(filePath, flag, perm)
	if err != nil {
		log.Error("open file err:", err)
		return nil, consts.OpenFileErr
	}
	return fd, nil
}
