package utils

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"path"
	"syscall"
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
		return nil, consts.FileStatErr
	}

	fd, err := os.OpenFile(filePath, flag, perm)
	if err != nil {
		log.Error("open file err:", err)
		return nil, consts.OpenFileErr
	}
	return fd, nil
}

func AtomicFileWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	origin := path
	if err := exec.Command("cp", origin, tmp).Run(); err != nil {
		log.Error("exec shell command failed. err:", err)
		return consts.ExecCmdErr
	}

	tmpFd, err := CheckAndCreateFile(tmp, syscall.O_APPEND|syscall.O_CREAT|syscall.O_RDWR, 0660)
	if err != nil {
		return err
	}

	_, err = tmpFd.Write(data)
	if err != nil {
		log.Error("write file failed. err:", err)
		return consts.WriteFileErr
	}

	if err = tmpFd.Sync(); err != nil {
		log.Error("sync file failed. err:", err)
		return consts.SyncFileErr
	}

	if err = tmpFd.Close(); err != nil {
		log.Error("close file failed. err:", err)
		return consts.CloseFileErr
	}

	if err = os.Rename(tmp, origin); err != nil {
		log.Error("exec rename command failed. err:", err)
		return consts.ExecCmdErr
	}

	return nil
}
