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
	"time"
)

const (
	mask = 1e10
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
	segmentSize       int64 // maxSegmentSize segment数据文件最大大小
	segmentCacheSize  int   // 内存中缓存的segment内容数量
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

func (opts *Options) SetSegmentSize(segmentSize int64) *Options {
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
	mu           sync.Mutex
	opts         *Options
	path         string     // path 先行日志的目录路径
	segmentCache *Lru       // segmentCache 记录缓存，采用LRU缓存策略
	segments     []*segment // segments 所有已知的segment

	// activeSegment 当前活跃数据文件
	// 支持立刻持久化和写满持久化，通过
	// noSync 控制。
	activeSegment *segment

	firstBlockIdx int64 // firstBlockIdx 第一个日志块idx
	lastBlockIdx  int64 // lastBlockIdx 最后一个日志块idx

	closed    bool       // closed 是否已经关闭
	corrupted bool       // corrupted 是否已损坏
	bgfailed  bool       // bgfailed 后台协程执行失败
	notifier  chan error // notifier wal主协程与子协程的同步渠道
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
		lastBlockIdx:  0,
		notifier:      make(chan error),
	}

	err = wal.init()
	if err != nil {
		return nil, err
	}

	wal.notifier <- nil
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

		stat, err = os.Stat(wal.path)
		if err != nil {
			return consts.FileStatErr
		}
	} else if err != nil {
		return consts.FileStatErr
	}

	if !stat.IsDir() {
		return consts.InvalidParamErr
	}

	var activeSegment *segment
	// 遍历日志目录，获取全量数据文件信息
	if err = filepath.WalkDir(wal.path, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == wal.path {
			return err
		}

		if de.IsDir() {
			// 跳过，认为存放wal segment文件的目录下不应该有其他目录，有则忽略
			return filepath.SkipDir
		}

		cs := newSegment(filepath.Join(wal.path, de.Name()), wal.opts.segmentSize)
		wal.segments = append(wal.segments, cs)
		firstBlockIdOfSegment, hasSuffix, err := baseToBlockId(de.Name())
		if err != nil {
			return err
		}

		wal.firstBlockIdx = int64(math.Min(float64(wal.firstBlockIdx), float64(firstBlockIdOfSegment)))

		if hasSuffix {
			// wal目录下多个segment文件存在.active后缀时
			// 清除startBlockId小的segment文件.active后缀
			if activeSegment == nil {
				activeSegment = cs
			} else {
				if firstBlockIdOfSegment > activeSegment.getStartBlockId() {
					err := activeSegment.rename()
					if err != nil {
						return err
					}

					activeSegment = cs
				} else {
					err := cs.rename()
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 目录下没有数据文件
	if len(wal.segments) == 0 {
		wal.firstBlockIdx = wal.lastBlockIdx
		first := newSegment(filepath.Join(wal.path, blockIdToBase(wal.lastBlockIdx, true)), wal.opts.segmentSize)
		wal.segments = append(wal.segments, first)
		wal.activeSegment = first
	} else {
		sort.Slice(wal.segments, func(i, j int) bool {
			return wal.segments[i].getStartBlockId() < wal.segments[j].getStartBlockId()
		})
		wal.activeSegment = activeSegment
	}

	// 打开当前活跃的segment文件
	// 新日志顺序写入当前活跃的segment文件
	err = wal.activeSegment.open(wal.opts.dataPerm)
	if err != nil {
		if errors.Is(err, consts.CorruptErr) {
			wal.corrupted = true
		}
		return err
	}
	wal.lastBlockIdx = wal.activeSegment.getStartBlockId() + int64(len(wal.activeSegment.bpos))

	// 最终一致性场景下使用后台协程定时刷盘
	// 刷盘周期默认1s
	if wal.opts.noSync {
		go func() {
			<-wal.notifier
			ticker := time.NewTicker(time.Second)
			for {
				select {
				case <-wal.notifier:
					return
				case <-ticker.C:
					err := wal.Sync()
					if err != nil {
						wal.mu.Lock()
						wal.bgfailed = true
						wal.mu.Unlock()
						return
					}
				}
			}
		}()
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

	// 关闭管道通知后台子协程结束
	close(wal.notifier)
	return
}

// Write 写入wal日志数据
// 当返回 WalFullErr 错误时意味着wal日志文件夹下存储内容已满
// 外部使用者应当读取并截断早时写入的日志数据
// 当返回 WalFullErr 错误时，wal不会关闭，因此对wal的操作是安全的
func (wal *Log) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return 0, consts.FileClosedErr
	} else if wal.corrupted {
		return 0, consts.CorruptErr
	} else if wal.bgfailed {
		return 0, consts.BackgroundErr
	}

	wal.lastBlockIdx = (wal.lastBlockIdx + 1) % mask
	// 循环写日志时可能日志文件夹下内容写满
	// 需要考虑lastBlockIdx追上firstBlockIdx的情况
	if wal.lastBlockIdx == wal.firstBlockIdx {
		return 0, consts.WalFullErr
	}

	err := wal.activeSegment.write(data)
	if err != nil {
		// 如果当前segment满了
		// 那么新开一个segment
		if errors.Is(err, consts.SegmentFullErr) {
			err := wal.activeSegment.close()
			if err != nil {
				return 0, err
			}

			wal.segments = append(wal.segments, newSegment(filepath.Join(wal.path, blockIdToBase(wal.lastBlockIdx, true)), wal.opts.segmentSize))
			err = wal.segments[len(wal.segments)-1].open(wal.opts.dataPerm)
			if err != nil {
				return 0, err
			}

			// 在下个segment文件创建成功后再去掉activeSegment文件名
			// 避免出现wal目录下出现没有segment文件有.active后缀的情况
			// 如果rename失败，wal目录下会出现多个segment文件有.active
			// 后缀的情况，wal.init()会选择有.active后缀且startBlockId
			// 最大的segment文件作为activeSegment
			err = wal.activeSegment.rename()
			if err != nil {
				return 0, err
			}
			wal.activeSegment = wal.segments[len(wal.segments)-1]

			// 如果segmentSize设置小于一次单wal日志数据最大体积
			// 那么可能出现新建一个segment也写入失败的问题
			err = wal.activeSegment.write(data)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	if !wal.opts.noSync {
		err = wal.activeSegment.sync()
		if err != nil {
			return 0, err
		}
	}

	return wal.lastBlockIdx, nil
}

func (wal *Log) Read(idx int64) ([]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil, consts.FileClosedErr
	} else if wal.corrupted {
		return nil, consts.CorruptErr
	} else if wal.bgfailed {
		return nil, consts.BackgroundErr
	}

	if idx < wal.firstBlockIdx || idx >= wal.lastBlockIdx {
		return nil, consts.NotFoundErr
	}

	targetSegment := wal.findSegment(idx)
	blockId := targetSegment.getStartBlockId()
	if cacheSeg := wal.segmentCache.read(blockId); cacheSeg != nil {
		return cacheSeg.(*segment).read(idx)
	}

	err := targetSegment.open(wal.opts.dataPerm)
	if err != nil {
		return nil, err
	}

	wal.segmentCache.write(blockId, targetSegment)

	return targetSegment.read(idx)
}

func (wal *Log) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.activeSegment.sync()
}

func (wal *Log) Truncate(idx int64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return consts.FileClosedErr
	} else if wal.corrupted {
		return consts.CorruptErr
	} else if wal.bgfailed {
		return consts.BackgroundErr
	}

	if idx < wal.firstBlockIdx || idx > wal.lastBlockIdx {
		return consts.InvalidParamErr
	}

	targetSegment := wal.findSegment(idx)
	for i, seg := range wal.segments {
		if seg == targetSegment {
			wal.segments = wal.segments[i:]
			break
		}

		// idx之前的文件删除
		err := os.Remove(seg.path)
		if err != nil {
			return err
		}
	}

	if targetSegment == wal.activeSegment {
		err := wal.activeSegment.truncate(idx)
		if err != nil {
			return err
		}

		err = wal.activeSegment.sync()
		if err != nil {
			return err
		}

		return nil
	} else {
		err := targetSegment.open(wal.opts.dataPerm)
		if err != nil {
			return err
		}

		err = targetSegment.truncate(idx)
		if err != nil {
			return err
		}

		err = targetSegment.close()
		if err != nil {
			return err
		}
		return nil
	}
}

func (wal *Log) findSegment(idx int64) *segment {
	target := sort.Search(len(wal.segments), func(i int) bool {
		return wal.segments[i].getStartBlockId() >= idx
	})

	if target != 0 {
		target -= 1
	}

	return wal.segments[target]
}
