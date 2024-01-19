package wal

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	mask = 0x010814
)

var DefaultOptions = &Options{
	dataPerm:         0660,
	dirPerm:          0770,
	segmentSize:      10 * consts.MB,
	segmentCacheSize: 3,
	noSync:           false,
}

// Options wal日志选项
type Options struct {
	// dirPerm 先行日志目录文件权限位
	// dataPerm 先行日志数据文件权限位
	dataPerm, dirPerm os.FileMode
	segmentSize       uint64 // maxSegmentSize segment数据文件最大大小
	segmentCacheSize  int    // 内存中缓存的segment内容数量
	// noSync 当设置为true时，只有segment写满
	// 才持久化到磁盘。可以提高写性能，但需要承担
	// 数据丢失的风险。
	noSync bool
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
	mu           sync.RWMutex
	opts         *Options
	path         string     // path 先行日志的目录路径
	segmentCache *Lru       // segmentCache 记录缓存，采用LRU缓存策略
	segments     []*segment // segments 所有已知的segment

	// activeSegment 当前活跃数据文件
	// 支持立刻持久化和写满持久化，通过
	// noSync 控制。
	activeSegment *segment

	firstBlockIdx int64 // firstBlockIdx 第一个日志块idx
	lateBlockIdx  int64 // lateBlockIdx 最后一个日志块idx

	closed    bool // closed 是否已经关闭
	corrupted bool // corrupted 是否已损坏
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
		path:          dirPath,
		opts:          opts,
		segmentCache:  newLru(opts.segmentCacheSize),
		firstBlockIdx: math.MaxInt64,
		lateBlockIdx:  math.MinInt64,
	}

	err = wal.init()
	if err != nil {
		return nil, err
	}

	return wal, nil
}

// init 读文件，初始化wal
func (wal *Log) init() error {
	// 检查目录是否存在，如不存在则创建
	stat, err := os.Stat(wal.path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(wal.path, wal.opts.dirPerm); err != nil {
			return consts.MkdirErr
		}
	} else if err != nil {
		return consts.FileStatErr
	}

	if !stat.IsDir() {
		return consts.InvalidParamErr
	}

	// 在先行日志目录下找latest
	if err = filepath.WalkDir(wal.path, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if de.IsDir() {
			// 跳过，认为存放wal segment文件的目录下不应该有其他目录，有则忽略
			return filepath.SkipDir
		}

		wal.segments = append(wal.segments, newSegment(filepath.Join(wal.path, de.Name())))
		firstBlockIdOfSegment, err := baseToBlockId(de.Name())
		if err != nil {
			return err
		}

		wal.firstBlockIdx = int64(math.Min(float64(wal.firstBlockIdx), float64(firstBlockIdOfSegment)))
		wal.lateBlockIdx = int64(math.Max(float64(wal.lateBlockIdx), float64(firstBlockIdOfSegment)))
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(wal.segments, func(i, j int) bool {
		return wal.segments[i].getStartBlockId() < wal.segments[j].getStartBlockId()
	})
	wal.activeSegment = wal.segments[len(wal.segments)-1]
	err = wal.activeSegment.open(wal.opts.dataPerm)
	if err != nil {
		if errors.Is(err, consts.CorruptErr) {
			wal.corrupted = true
		}
		return err
	}

	return nil
}

func (wal *Log) Close() (err error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		err = consts.FileClosedErr
		return
	}

	e := wal.activeSegment.close()
	if e != nil {
		err = e
		return
	}

	wal.closed = true
	return
}

func (wal *Log) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return 0, consts.FileClosedErr
	} else if wal.corrupted {
		return 0, consts.CorruptErr
	}

	// todo: 循环写日志
	// wal.lateBlockIdx = (wal.lateBlockIdx + 1) % mask
	wal.lateBlockIdx += 1
	if uint64(wal.activeSegment.size())+uint64(len(buildBinary(data))) > wal.opts.segmentSize {
		err := wal.activeSegment.close()
		if err != nil {
			return 0, err
		}

		wal.segments = append(wal.segments, newSegment(filepath.Join(wal.path, blockIdToBase(wal.lateBlockIdx))))
		wal.activeSegment = wal.segments[len(wal.segments)-1]
		err = wal.activeSegment.open(wal.opts.dataPerm)
		if err != nil {
			return 0, err
		}
	}

	idx := wal.lateBlockIdx
	err := wal.activeSegment.write(data)
	if err != nil {
		return 0, err
	}

	if !wal.opts.noSync {
		err = wal.activeSegment.sync()
		if err != nil {
			return 0, err
		}
	}

	return idx, nil
}

func (wal *Log) Read(idx int64) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if wal.closed {
		return nil, consts.FileClosedErr
	} else if wal.corrupted {
		return nil, consts.CorruptErr
	}

	if idx < wal.firstBlockIdx || idx > wal.lateBlockIdx {
		return nil, consts.InvalidParamErr
	}

	targetSegment := wal.findSegment(idx)

	cacheSeg := wal.segmentCache.read(targetSegment.getStartBlockId()).(*segment)
	if cacheSeg != nil {
		return cacheSeg.read(idx)
	}
	err := targetSegment.open(wal.opts.dataPerm)
	if err != nil {
		return nil, err
	}
	wal.segmentCache.write(targetSegment.getStartBlockId(), targetSegment)
	return targetSegment.read(idx)
}

func (wal *Log) Sync() error {
	return wal.activeSegment.sync()
}

func (wal *Log) Truncate(idx int64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return consts.FileClosedErr
	} else if wal.corrupted {
		return consts.CorruptErr
	}

	if idx < wal.firstBlockIdx || idx > wal.lateBlockIdx {
		return consts.InvalidParamErr
	}

	for i, seg := range wal.segments {
		if seg.startBlockId <= idx {
			wal.segments = wal.segments[i:]
			break
		}
	}

	if wal.segments[0] == wal.activeSegment {
		return wal.activeSegment.truncate(idx)
	}

	err := wal.segments[0].open(wal.opts.dataPerm)
	if err != nil {
		return err
	}

	err = wal.segments[0].truncate(idx)
	if err != nil {
		return err
	}

	err = wal.segments[0].close()
	if err != nil {
		return err
	}
	return nil
}

func (wal *Log) findSegment(idx int64) *segment {
	target := sort.Search(len(wal.segments), func(i int) bool {
		return wal.segments[i].getStartBlockId() >= idx
	})

	return wal.segments[target]
}
