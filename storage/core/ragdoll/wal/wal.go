package wal

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/utils"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// todo：加日志！

func getMask() int64 {
	return utils.GetValueOnEnv(1e10, 1e8).(int64)
}

// Options wal日志选项
type Options struct {
	// dirPerm 先行日志目录文件权限位
	// dataPerm 先行日志数据文件权限位
	dataPerm, dirPerm os.FileMode
	segmentFileSize   int64 // segmentFileSize segment数据文件最大大小
	// todo: 命名
	segmentCacheSize int // 内存中缓存的segment内容数量
	// noSync 当设置为true时，只有segment写满
	// 才持久化到磁盘。可以提高写性能，但需要承担
	// 数据丢失的风险。
	noSync bool
}

func NewOptions() *Options {
	return &Options{
		dataPerm:         0660,
		dirPerm:          0770,
		segmentFileSize:  10 * consts.MB,
		segmentCacheSize: 3,
		noSync:           false,
	}
}

func (opts *Options) SetDataPerm(dataPerm os.FileMode) *Options {
	opts.dataPerm = dataPerm
	return opts
}

func (opts *Options) SetDirPerm(dirPerm os.FileMode) *Options {
	opts.dirPerm = dirPerm
	return opts
}

func (opts *Options) SetSegmentSize(segmentFileSize int64) *Options {
	opts.segmentFileSize = segmentFileSize
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

// todo：考虑特殊权限位
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

	// todo：对segmentFileSize做校验
	return nil
}

// Log 先行日志
type Log struct {
	mu           sync.Mutex
	opts         *Options
	dirPath      string     // dirPath 先行日志的目录路径
	segmentCache *Lru       // segmentCache 记录缓存，采用LRU缓存策略
	segments     []*segment // segments 所有已知的segment

	// activeSegment 当前活跃数据文件
	// 支持立刻持久化和写满持久化，通过
	// noSync 控制。
	activeSegment *segment

	firstBlockIdx int64 // firstBlockIdx 第一个日志块idx
	lastBlockIdx  int64 // lastBlockIdx 最后一个日志块idx

	closed    bool // closed 是否已经关闭
	corrupted bool // corrupted 是否已损坏
	bgfailed  bool // bgfailed 后台协程执行失败

	// notifier wal主协程与子协程的同步管道
	// 只有设置 opts.noSync 为true时会用到
	notifier chan error
}

func Open(dirPath string, opts *Options) (_ *Log, err error) {
	if opts == nil {
		opts = NewOptions()
	}

	err = opts.check()
	if err != nil {
		return nil, err
	}

	wal := &Log{
		dirPath:       dirPath,
		opts:          opts,
		segmentCache:  newLru(opts.segmentCacheSize),
		firstBlockIdx: 0,
		lastBlockIdx:  0,
	}

	if wal.opts.noSync {
		wal.notifier = make(chan error)
		defer func() {
			if err != nil {
				return
			}
			wal.notifier <- nil
		}()
	}

	err = wal.init()
	if err != nil {
		return nil, err
	}

	return wal, nil
}

// init 读文件，初始化wal
func (wal *Log) init() error {
	// todo: 封装方法
	// 检查目录是否存在，如不存在则创建
	stat, err := os.Stat(wal.dirPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(wal.dirPath, wal.opts.dirPerm); err != nil {
			return consts.MkdirErr
		}

		stat, err = os.Stat(wal.dirPath)
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
	if err = filepath.WalkDir(wal.dirPath, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// todo：看下为啥这样写
		if path == wal.dirPath {
			return err
		}

		if de.IsDir() {
			// 跳过，认为存放wal segment文件的目录下不应该有其他目录，有则忽略
			return filepath.SkipDir
		}

		currentSegment := newSegment(filepath.Join(wal.dirPath, de.Name()), wal.opts.segmentFileSize)
		wal.segments = append(wal.segments, currentSegment)
		firstBlockIdOfSegment, hasSuffix, err := baseToBlockId(de.Name())
		if err != nil {
			return err
		}

		if hasSuffix {
			// wal目录下多个segment文件存在.active后缀时
			// 清除startBlockId小的segment文件.active后缀
			if activeSegment == nil {
				activeSegment = currentSegment
			} else {
				// todo：bugfix 有多个文件存在.active后缀的情况下，可能idx小的文件是真正的activeSegment
				segmentToRename := currentSegment
				if firstBlockIdOfSegment > activeSegment.getStartBlockId() {
					segmentToRename = activeSegment
					activeSegment = currentSegment
				}

				err := segmentToRename.rename()
				if err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 目录下没有segment的情况
	// 首次启动 & truncate
	if activeSegment == nil {
		activeSegment = newSegment(filepath.Join(wal.dirPath, blockIdToBase(0, true)), wal.opts.segmentFileSize)
		wal.segments = append(wal.segments, activeSegment)
	}
	wal.activeSegment = activeSegment

	err = activeSegment.open(wal.opts.dataPerm)
	if err != nil {
		// todo：调研 损坏恢复
		if errors.Is(err, consts.CorruptErr) {
			wal.corrupted = true
		}
		return err
	}

	// 构建firstBlockId和lastBlockId
	if len(wal.segments) == 0 {
		// todo：idx为0的block会被跳过
		wal.lastBlockIdx = wal.activeSegment.getStartBlockId()
	} else {
		wal.sortSegments()
		for idx, seg := range wal.segments {
			if seg == wal.activeSegment {
				firstSegmentIdx := (idx + 1) % len(wal.segments)
				wal.firstBlockIdx = wal.segments[firstSegmentIdx].getStartBlockId()
			}
		}
		// note: 必须在打开文件后才能读取到bpos的长度
		wal.lastBlockIdx = wal.activeSegment.getStartBlockId() + int64(len(wal.activeSegment.bpos)) - 1
	}

	// 最终一致性场景下使用后台协程定时刷盘
	// 刷盘周期默认1s
	if wal.opts.noSync {
		go func() {
			<-wal.notifier
			// todo: 暴露刷盘周期
			ticker := time.NewTicker(time.Second)
			for {
				select {
				case <-wal.notifier:
					return
				case <-ticker.C:
					// todo: 去掉close之后再看下是否有问题
					// note: ticker和notifier同时可消费时notifier更高优
					wal.mu.Lock()
					if wal.closed {
						wal.mu.Unlock()
						return
					}
					wal.mu.Unlock()

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

func (wal *Log) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return consts.FileClosedErr
	}

	// todo：cache里打开的文件没关
	err := wal.activeSegment.close()
	if err != nil {
		return err
	}

	wal.closed = true

	// 关闭管道通知后台子协程结束
	close(wal.notifier)
	return nil
}

// Write 写入wal日志数据
// 当返回 WalFullErr 错误时意味着wal日志文件夹下存储内容已满
// 外部使用者应当读取并截断早时写入的日志数据
// 当返回 WalFullErr 错误时，wal不会关闭，因此对wal的操作是安全的
func (wal *Log) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.stateCheck()
	if err != nil {
		return 0, err
	}

	// 循环写日志时可能日志文件夹下内容写满
	// 需要考虑lastBlockIdx追上firstBlockIdx的情况
	nextLastBlockIdx := (wal.lastBlockIdx + 1) % getMask()
	if nextLastBlockIdx == wal.firstBlockIdx {
		return 0, consts.WalFullErr
	}
	wal.lastBlockIdx = nextLastBlockIdx

	err = wal.activeSegment.write(data)
	if err != nil {
		// 如果当前segment满了
		// 那么新开一个segment
		if errors.Is(err, consts.SegmentFullErr) {
			err := wal.activeSegment.close()
			if err != nil {
				return 0, err
			}

			nextActiveSegment := newSegment(filepath.Join(wal.dirPath, blockIdToBase(wal.lastBlockIdx, true)), wal.opts.segmentFileSize)
			wal.segments = append(wal.segments, nextActiveSegment)
			err = nextActiveSegment.open(wal.opts.dataPerm)
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
			wal.activeSegment = nextActiveSegment

			// 如果segmentSize设置小于一次单wal日志数据最大体积
			// 那么可能出现新建一个segment也写入失败的问题
			err = wal.activeSegment.write(data)
			if err != nil {
				// todo: 加对consts.SegmentFullErr的特判
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

// todo：加mread

func (wal *Log) Read(idx int64) ([]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.stateCheck()
	if err != nil {
		return nil, err
	}

	err = wal.checkRange(idx)
	if err != nil {
		return nil, err
	}

	targetSegment := wal.findSegment(idx)
	blockId := targetSegment.getStartBlockId()
	if cacheSeg := wal.segmentCache.read(blockId); cacheSeg != nil {
		return cacheSeg.(*segment).read(idx)
	}

	if targetSegment != wal.activeSegment {
		err = targetSegment.open(wal.opts.dataPerm)
		if err != nil {
			return nil, err
		}
	}

	eliminatedSegment := wal.segmentCache.write(blockId, targetSegment)
	if eliminatedSegment != nil {
		err := eliminatedSegment.(*segment).close()
		if err != nil {
			return nil, err
		}
	}

	return targetSegment.read(idx)
}

func (wal *Log) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.stateCheck()
	if err != nil {
		return err
	}

	return wal.activeSegment.sync()
}

// todo：加注释
func (wal *Log) Truncate(idx int64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.stateCheck()
	if err != nil {
		return err
	}

	err = wal.checkRange(idx)
	if err != nil {
		return err
	}

	// todo：idx比firstSegment小
	targetSegment := wal.findSegment(idx)
	firstSegment := wal.findSegment(wal.firstBlockIdx)
	for _, seg := range wal.segments {
		// note: c1 和 c2 对应 firstBlockIdx 与 lastBlockIdx 之间的两种情况。见 checkRange
		c1 := wal.firstBlockIdx <= wal.lastBlockIdx && (firstSegment.getStartBlockId() <= seg.getStartBlockId() && seg.getStartBlockId() <= targetSegment.getStartBlockId())
		c2 := wal.firstBlockIdx > wal.lastBlockIdx && (seg.getStartBlockId() <= targetSegment.getStartBlockId() && firstSegment.getStartBlockId() >= seg.getStartBlockId())
		if c1 || c2 {
			// note：清除缓存，否则原缓存key会因截断后startBlockId修改而导致不可访问/浪费内存
			wal.segmentCache.remove(seg.getStartBlockId())

			if seg == wal.activeSegment {
				_, err = wal.activeSegment.truncate(idx)
				if err != nil {
					return err
				}

				err = wal.activeSegment.sync()
				if err != nil {
					return err
				}

				wal.firstBlockIdx = wal.activeSegment.getStartBlockId()
			} else {
				err = seg.open(wal.opts.dataPerm)
				if err != nil {
					return err
				}

				remain, err := seg.truncate(idx)
				if err != nil {
					return err
				}

				err = seg.close()
				if err != nil {
					return err
				}

				if remain != 0 {
					wal.firstBlockIdx = seg.getStartBlockId()
				}
			}
		}
	}

	return nil
}

func (wal *Log) findSegment(idx int64) *segment {
	// note: 众所周知，二分搜索要求序列有序
	wal.sortSegments()
	// todo：bugfix 如果要找的segment是最后一个
	target := sort.Search(len(wal.segments), func(i int) bool {
		return wal.segments[i].getStartBlockId() >= idx
	})

	if target != 0 {
		target -= 1
	}

	return wal.segments[target]
}

func (wal *Log) stateCheck() error {
	if wal.closed {
		return consts.FileClosedErr
	} else if wal.corrupted {
		return consts.CorruptErr
	} else if wal.bgfailed {
		return consts.BackgroundErr
	}
	return nil
}

func (wal *Log) sortSegments() {
	// todo：设置脏位
	sort.Slice(wal.segments, func(i, j int) bool {
		return wal.segments[i].getStartBlockId() < wal.segments[j].getStartBlockId()
	})
}

func (wal *Log) checkRange(idx int64) error {
	// 正常情况，lastBlockIdx 比 firstBlockIdx 大
	// 认为idx比 firstBlockIdx 小或者idx比 lastBlockIdx 时参数非法
	if wal.firstBlockIdx <= wal.lastBlockIdx && (idx < wal.firstBlockIdx || idx > wal.lastBlockIdx) {
		return consts.InvalidParamErr
	}

	// 当日志开始循环，可能出现 lastBlockIdx 比 firstBlockIdx 小的情况
	// 此时认为idx在 lastBlockIdx 与 firstBlockIdx 之间为非法情况
	if wal.firstBlockIdx > wal.lastBlockIdx && (wal.lastBlockIdx < idx && idx < wal.firstBlockIdx) {
		return consts.InvalidParamErr
	}

	return nil
}
