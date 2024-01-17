package wal

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	mask = 0x010814
)

var DefaultOptions = &Options{
	dataPerm:         0660,
	dirPerm:          0770,
	segmentSize:      10 * consts.MB,
	segmentCacheSize: 50,
}

// Options wal日志选项
type Options struct {
	dataPerm, dirPerm os.FileMode
	segmentSize       uint64
	segmentCacheSize  int
	noSync            bool
}

func NewOptions() *Options {
	return DefaultOptions
}

func (opts *Options) SetDataPerm(dataPerm os.FileMode) *Options {
	opts.dataPerm = dataPerm
	return opts
}

func (opts *Options) SetDirPerm(dirPerm os.FileMode) *Options {
	opts.dirPerm = dirPerm
	return opts
}

func (opts *Options) SetSegmentSize(segmentSize uint64) *Options {
	opts.segmentSize = segmentSize
	return opts
}

func (opts *Options) SetSegmentCacheSize(segmentCacheSize int) *Options {
	opts.segmentCacheSize = segmentCacheSize
	return opts
}

func (opts *Options) SetNoSync() *Options {
	opts.noSync = true
	return opts
}

func (opts *Options) check() error {
	if opts.dataPerm == 0 || opts.dataPerm > 0777 {
		return consts.InvalidParamErr
	}

	if opts.dirPerm == 0 || opts.dirPerm > 0777 {
		return consts.InvalidParamErr
	}

	if opts.segmentCacheSize < 0 {
		return consts.InvalidParamErr
	}

	return nil
}

// Log 先行日志
type Log struct {
	mu sync.Mutex
	// dirPath 先行日志的目录路径
	dirPath string
	// dirPerm 先行日志目录权限位
	dirPerm os.FileMode
	// dataPerm 先行日志数据权限位
	dataPerm os.FileMode

	// segments缓存，采用LRU缓存策略
	segmentCache *Lru

	// activeSegment 当前活跃数据文件
	// 支持立刻持久化和写满持久化，通过
	// noSync 控制。
	activeSegment *segment

	// latestBlockId 最新日志块idx
	latestBlockId int64

	// noSync 当设置为true时，只有segment写满
	// 才持久化到磁盘。可以提高写性能，但需要承担
	// 数据丢失的风险。
	noSync bool
}

func Open(dirPath string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}

	err := opts.check()
	if err != nil {
		return nil, err
	}

	wal := &Log{
		dirPath:      dirPath,
		dirPerm:      opts.dirPerm,
		dataPerm:     opts.dataPerm,
		segmentCache: NewLru(opts.segmentCacheSize),
		noSync:       opts.noSync,
	}

	err = wal.init()
	if err != nil {
		return nil, err
	}

	return wal, nil
}

// init 读文件，初始化wal
func (wal *Log) init() error {
	// 整理path，使其成为clean的绝对路径
	path := filepath.Clean(wal.dirPath)
	if !filepath.IsAbs(path) {
		wd, err := os.Getwd()
		if err != nil {
			log.Error(err)
			return consts.GetWdErr
		}
		path = filepath.Join(wd, path)
	}

	// 检查目录是否存在，如不存在则创建
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, wal.dirPerm); err != nil {
			log.Error(err)
			return consts.MkdirErr
		}
	} else if err != nil {
		log.Error(err)
		return consts.FileStatErr
	}

	if !stat.IsDir() {
		return consts.InvalidParamErr
	}

	// 在先行日志目录下找latest
	err = filepath.WalkDir(path, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if de.IsDir() {
			// 跳过，认为存放wal segment文件的目录下不应该有其他目录，有则忽略
			return filepath.SkipDir
		}

		firstBlockIdOfSegment, err := baseToBlockId(de.Name())
		if err != nil {
			return err
		}
		wal.latestBlockId = int64(math.Max(float64(wal.latestBlockId), float64(firstBlockIdOfSegment)))
		return nil
	})
	if err != nil {
		return err
	}

	/*
		utils.CheckAndCreateFile(filepath.Join(path, buildPath(wal.latestBlockId)), , wal.perm)
		if err != nil {
			log.Fatalln(err)
			return consts.WalkDirErr
		}

		defer utils.HandlePanic(func() {
			if err = fd.Close(); err != nil {
				log.Fatalln(err)
			}
		})

		bytes, err := io.ReadAll(fd)
		if err != nil {
			log.Errorln(err)
			return consts.ReadFileErr
		}

		// 初始化block
		var maxBlockId int64
		for {
			if len(bytes) == 0 {
				break
			}

			block := newEmptyBlock()
			offset, err := block.unMarshal(bytes)
			if err != nil {
				return err
			}

			maxBlockId = int64(math.Max(float64(maxBlockId), float64(block.id)))
			wal.Blocks = append(wal.Blocks, block)
			bytes = bytes[offset:]
		}
		wal.NextBlockId = maxBlockId + 1
	*/
	return nil
}

func (wal *Log) Close() error {
	return nil
}

func (wal *Log) Write([]byte) (n int, err error) {
	return 0, nil
}

func (wal *Log) Read([]byte) (n int, err error) {
	return 0, nil
}
