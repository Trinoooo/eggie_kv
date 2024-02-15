package wal

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/utils"
	"go.uber.org/zap"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// 核心概念：
// · wal_dir：存放日志数据文件（segment）的目录
// 		· maxBlockCapacityInWAL：描述wal目录下最大能容纳的block数量
// · segment：日志数据文件，存储日志内容
// 		· segmentCapacity segment单文件能够存储的最大字节数，通常情况下由于要保证block不分开存储在多个segment文件中
//		一个segment文件填不满就会开启下一个segment文件
// · block：描述segment中的单条日志，用于定界、检查日志完整性

// getMaxBlockCapacityInWAL 获取wal目录下最大能容纳的block数量
// test环境下是1e8；prod环境下1e10
func getMaxBlockCapacityInWAL() int64 {
	return int64(utils.GetValueOnEnv(1e10, 1e8).(float64))
}

const dirLock = ".lock"

// Options wal日志选项
type Options struct {
	// dirPerm 先行日志目录文件权限位
	// dataPerm 先行日志数据文件权限位
	dataPerm, dirPerm    os.FileMode
	segmentCapacity      int64 // segmentCapacity segment 数据文件最大存储容量
	segmentCacheCapacity int   // segmentCacheCapacity 内存中缓存 segment 的最大数量
	// noSync 当设置为true时，只有 segment 写满
	// 才持久化到磁盘。可以提高写性能，但需要承担
	// 数据丢失的风险。
	noSync       bool
	syncInterval time.Duration // syncInterval 刷盘周期，单位是毫秒
}

// NewOptions 初始化wal配置选项
//
// 返回值：
// - 接收器wal配置选项（*options）
func NewOptions() *Options {
	return &Options{
		dataPerm:             0660,
		dirPerm:              0770,
		segmentCapacity:      10 * consts.MB,
		segmentCacheCapacity: 3,
		noSync:               false,
		syncInterval:         1000 * time.Millisecond,
	}
}

// SetDataPerm 设置segment数据文件权限位
//
// 该配置在创建新 segment时会用到，已经存在的segment不会受到影响
//
// 参数：
// - dataPerm segment数据文件权限位
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetDataPerm(dataPerm os.FileMode) *Options {
	opts.dataPerm = dataPerm
	return opts
}

// SetDirPerm 设置wal日志目录权限位
//
// 如果在启动wal时发现指定dirPath下不存在wal目录，该配置会在新建目录时用到
//
// 参数：
// - dirPerm wal目录权限位
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetDirPerm(dirPerm os.FileMode) *Options {
	opts.dirPerm = dirPerm
	return opts
}

// SetSegmentCapacity 设置segment数据文件最大存储容量
//
// segment文件在写入时通常不会达到文件最大存储容量就开始写入下一个segment文件
// 这是因为写入segment的最细粒度是block，单个block无法跨segment存储
//
// 参数：
// - segmentCapacity segment数据文件最大存储容量
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetSegmentCapacity(segmentCapacity int64) *Options {
	opts.segmentCapacity = segmentCapacity
	return opts
}

// SetSegmentCacheSize 设置wal中的segment最大缓存数量
//
// segment缓存只有在读取日志内容时生效
//
// 参数：
// - segmentCacheCapacity wal中的segment最大缓存数量
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetSegmentCacheSize(segmentCacheCapacity int) *Options {
	opts.segmentCacheCapacity = segmentCacheCapacity
	return opts
}

// SetNoSync 设置不要每次写入都持久化到磁盘
//
// 当设置为true时，刷盘时机有
// 1. 外部主动调用wal.Sync
// 2. 当前segment文件写满时自动持久化数据到磁盘
// 3. 后台协程每隔syncInterval毫秒周期刷盘
// 通常情况下设置noSync选项后性能会有可观提升
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetNoSync() *Options {
	opts.noSync = true
	return opts
}

// SetSyncInterval 设置后台协程刷盘时间间隔，单位是毫秒
//
// 只有在设置 SetNoSync 选项后该选项才生效
//
// 参数：
// - syncInterval 后台协程刷盘时间间隔，单位是毫秒
//
// 返回值：
// - 接收器wal配置选项（*options）
func (opts *Options) SetSyncInterval(syncInterval time.Duration) *Options {
	opts.syncInterval = syncInterval
	return opts
}

func (opts *Options) check() error {
	// note：暂时不考虑特殊权限位
	if opts.dataPerm <= 0 || opts.dataPerm > 0777 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "opts.dataPerm"), zap.Uint32(consts.Value, uint32(opts.dataPerm)))
		return e
	}

	if opts.dirPerm <= 0 || opts.dirPerm > 0777 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "opts.dirPerm"), zap.Uint32(consts.Value, uint32(opts.dirPerm)))
		return e
	}

	if opts.segmentCacheCapacity < 0 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "opts.segmentCacheCapacity"), zap.Int(consts.Value, opts.segmentCacheCapacity))
		return e
	}

	// 这个范围主要是出于性能考虑，segment文件太小会导致频繁开关文件，segment文件太大会有性能问题（占内存过大，频繁缺页等）
	if opts.segmentCapacity < consts.MB || opts.segmentCapacity > consts.GB {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "opts.segmentCapacity"), zap.Int64(consts.Value, opts.segmentCapacity))
		return e
	}

	if opts.syncInterval < 0 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "opts.syncInterval"), zap.Duration(consts.Value, opts.syncInterval))
		return e
	}
	return nil
}

// Log 先行日志
type Log struct {
	mu           sync.Mutex
	opts         *Options   // opts 传入的配置选项
	dirPath      string     // dirPath 先行日志的目录路径
	dirLockFile  *os.File   // dirLockFile 用于锁目录的锁文件
	segmentCache *utils.Lru // segmentCache 记录缓存，采用LRU缓存策略
	segments     []*segment // segments 所有已知的 segment
	// isSegmentsOrdered 用于优化 segments 排序性能的脏位
	// 当 isSegmentsOrdered 设置为true时表示 segments 处于有序状态，重复调用 sortSegments 不会执行排序过程；
	// 当 isSegmentsOrdered 设置为false时表示 segments 处于无序状态，此时调用 sortSegments 会执行排序过程
	isSegmentsOrdered bool

	// activeSegment 当前活跃数据文件
	// 支持立刻持久化和写满持久化，通过
	// opts.noSync 控制。
	activeSegment *segment

	firstBlockIdx int64 // firstBlockIdx 第一个日志块idx，nil表示不存在 firstBlockIdx 指向的block
	lastBlockIdx  int64 // lastBlockIdx 最后一个日志块idx，nil表示不存在 lastBlockIdx 指向的block

	closed    bool // closed 是否已经关闭
	corrupted bool // corrupted 是否已损坏
	bgfailed  bool // bgfailed 后台协程执行失败

	// notifier wal主协程与子协程的同步管道
	// 只有设置 opts.noSync 为true时会用到
	notifier chan error
}

// Open 打开wal
//
// 为了避免内存泄漏，结束使用wal后需要显示调用 wal.Close 关闭
// 如果 Open 指定的dir已经被其他wal实例打开，会返回 consts.NewFlockFileErr
// 外部应该检查这个错误并且重试其他目录路径
//
// 参数：
// - dirPath 存放wal日志的目录
// - opts wal配置选项，不传配置将设置成默认值，见 NewOptions
//
// 返回值：
// - wal日志（*Log）
// - errs 过程中出现的错误，类型是 *consts.KvErr
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
		segmentCache:  utils.NewLRU(opts.segmentCacheCapacity),
		firstBlockIdx: -1,
		lastBlockIdx:  -1,
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

// init 检查或创建日志目录，读日志数据文件，初始化wal
func (wal *Log) init() error {
	err := wal.checkOrInitDir()
	if err != nil {
		return err
	}

	err = wal.lockDir()
	if err != nil {
		return err
	}

	err = wal.loadSegments()
	if err != nil {
		return err
	}

	err = wal.locationBlockRangeInSegments()
	if err != nil {
		return err
	}
	// 最终一致性场景下使用后台协程定时刷盘
	// 刷盘周期默认1s
	if wal.opts.noSync {
		go wal.periodicSync()
	}

	return nil
}

// checkOrInitDir 检查目录是否存在，如不存在则创建
func (wal *Log) checkOrInitDir() error {
	stat, err := os.Stat(wal.dirPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(wal.dirPath, wal.opts.dirPerm); err != nil {
			e := errs.NewMkdirErr().WithErr(err)
			logs.Error(e.Error())
			return e
		}

		stat, err = os.Stat(wal.dirPath)
		if err != nil {
			e := errs.NewFileStatErr().WithErr(err)
			logs.Error(e.Error())
			return e
		}
	} else if err != nil {
		e := errs.NewFileStatErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	if !stat.IsDir() {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "stat"), zap.Any(consts.Value, stat))
		return e
	}

	return nil
}

// lockDir 锁住目录，避免多个wal实例重入同一个wal目录
func (wal *Log) lockDir() error {
	file, err := utils.CheckAndCreateFile(filepath.Join(wal.dirPath, dirLock), os.O_CREATE|os.O_RDWR, 0770)
	if err != nil {
		e := errs.NewOpenFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		e := errs.NewFlockFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	wal.dirLockFile = file
	return nil
}

// loadSegments 遍历日志目录，获取全量数据文件信息
// 装载 wal.segments 与 wal.activeSegments
func (wal *Log) loadSegments() error {
	var activeSegment *segment
	var err error
	if err = filepath.WalkDir(wal.dirPath, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if de.IsDir() {
			// 因为root也会包含在遍历范围内，所以特判过掉root
			// 让遍历能够正常进到wal目录下
			if path == wal.dirPath {
				return err
			}
			// 其他情况的目录我们认为是预期之外的，有则忽略
			return filepath.SkipDir
		}

		// 目录锁文件跳过
		if de.Name() == dirLock {
			return nil
		}

		currentSegment := newSegment(filepath.Join(wal.dirPath, de.Name()), wal.opts.segmentCapacity)
		wal.segments = append(wal.segments, currentSegment)
		firstBlockIdOfSegment, hasSuffix, err := baseToBlockId(de.Name())
		if err != nil {
			return err
		}

		if hasSuffix {
			if activeSegment == nil {
				activeSegment = currentSegment
			} else {
				// note：在写日志时，如果当前活跃的 segment 文件重命名失败时，会出现目录下同时存在两个带有.active后缀的 segment 文件
				// 且此时由于日志blockIdx触达上限开始循环，可能出现新的带有.active后缀的 segment 文件的firstBlockIdx比老文件小的情况
				// 但此时取到哪个文件作为activeSegment都不会影响程序的逻辑，无非是取到老文件多一次打开关闭文件操作开销。
				segmentToRename := currentSegment
				if firstBlockIdOfSegment > activeSegment.firstBlockIdx {
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
		e := errs.NewWalkDirErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	// 目录下没有segment的情况
	// 首次启动 & truncate
	if activeSegment == nil {
		activeSegment = newSegment(filepath.Join(wal.dirPath, blockIdToBase(0, true)), wal.opts.segmentCapacity)
		wal.segments = append(wal.segments, activeSegment)
	}
	wal.activeSegment = activeSegment
	err = activeSegment.open(wal.opts.dataPerm)
	if err != nil {
		// todo：调研 损坏恢复
		if errors.Is(err, errs.NewCorruptErr()) {
			wal.corrupted = true
		}
		return err
	}

	return nil
}

// locationBlockRangeInSegments 从segments中定位firstBlockId和lastBlockId
func (wal *Log) locationBlockRangeInSegments() error {
	if len(wal.segments) > 0 {
		wal.sortSegments()
		for idx, seg := range wal.segments {
			if seg == wal.activeSegment {
				firstSegment := wal.segments[(idx+1)%len(wal.segments)]
				size, err := firstSegment.size()
				if err != nil {
					return err
				}

				if size > 0 {
					wal.firstBlockIdx = firstSegment.getStartBlockIdx()
				}
			}
		}
	}

	// wal.firstBlockIdx 意味着wal中只有一个 segment，且 segment 中没有block
	// 如果 activeSegment 不是 firstSegment，且 activeSegment 中没有block
	// wal.lastBlockIdx 应该取到上一个segment的最后一个blockIdx
	if wal.firstBlockIdx != -1 {
		wal.lastBlockIdx = wal.activeSegment.getStartBlockIdx() + int64(len(wal.activeSegment.bpos)) - 1
	}
	return nil
}

// periodicSync 后台协程周期刷盘内存中的日志数据
// 只有设置 opts.noSync 为true时才会执行
func (wal *Log) periodicSync() {
	// note：等待主协程执行完open
	<-wal.notifier
	ticker := time.NewTicker(wal.opts.syncInterval)
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
				e := errs.NewBackgroundErr().WithErr(err)
				logs.Error(e.Error())
				return
			}
		}
	}
}

// Close 关闭wal，释放资源
//
// 如果在wal已经关闭后调用会返回 consts.NewFileClosedErr
//
// 返回值：
// - errs 过程中出现的错误，类型是 *consts.KvErr
func (wal *Log) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, false, false)
	if err != nil {
		return err
	}

	// 清空segmentCache，关闭其中缓存的segment文件
	var isActiveSegmentClosed bool
	err = wal.segmentCache.Traverse(func(item interface{}) error {
		segment := item.(*segment)
		err := segment.close()
		if err != nil {
			return err
		}

		if segment == wal.activeSegment {
			isActiveSegmentClosed = true
		}

		return nil
	}, false)
	if err != nil {
		return err
	}

	// 关闭activeSegment文件
	if !isActiveSegmentClosed {
		err = wal.activeSegment.close()
		if err != nil {
			return err
		}
	}

	// 关闭目录锁文件，释放文件锁
	err = syscall.Flock(int(wal.dirLockFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		e := errs.NewFlockFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	wal.closed = true
	wal.segmentCache = nil
	wal.segments = nil
	wal.activeSegment = nil
	wal.dirLockFile = nil
	// bugfix：关闭nil管道会panic
	if wal.opts.noSync {
		close(wal.notifier) // 关闭管道通知后台子协程结束
	}
	wal.opts = nil
	return nil
}

// Write 写入wal日志数据
//
// 当返回 consts.NewWalFullErr 错误时意味着wal日志文件夹下存储内容已满
// 使用者应该主动检查该类型错误并在 wal.Truncate 后重新尝试写入
// 此时wal不会关闭，因此对wal的操作是安全的
//
// 如果在wal已经关闭的情况下尝试写入，会返回 consts.NewFileClosedErr 错误
// 如果在wal数据已经被破坏的情况下尝试写入，会返回 consts.NewCorruptErr 错误
// 如果在wal后台协程执行失败的情况下尝试写入，会返回 consts.NewBackgroundErr 错误
//
// 参数：
// - data 日志数据，类型是字节数组
//
// 返回值：
// - blockIdx 写入成功后会返回写入日志在wal中的blockIdx，该值用于 Read、 MRead 和 Truncate
// - errs 过程中出现的错误，类型是 *consts.KvErr
func (wal *Log) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return 0, err
	}

	err = wal.checkDataSize(data)
	if err != nil {
		return 0, err
	}

	// 循环写日志时可能日志文件夹下内容写满
	// 需要考虑lastBlockIdx追上firstBlockIdx的情况
	nextBlockIdx := (wal.lastBlockIdx + 1) % getMaxBlockCapacityInWAL()
	if nextBlockIdx == wal.firstBlockIdx {
		e := errs.NewWalFullErr()
		logs.Error(e.Error())
		return 0, e
	}
	wal.lastBlockIdx = nextBlockIdx

	err = wal.activeSegment.write(data)
	if err != nil {
		// 1. 如果当前segment满了，那么新开一个segment
		// 2. 如果写segment时发现segment内部blockIdx已经触达blockCapacity上限，那么blockIdx从零开始计数新开一个segment
		if errs.GetCode(err) == errs.SegmentFullErrCode || errs.GetCode(err) == errs.ReachBlockIdxLimitErrCode {
			err := wal.activeSegment.close()
			if err != nil {
				return 0, err
			}

			nextActiveSegment := newSegment(filepath.Join(wal.dirPath, blockIdToBase(wal.lastBlockIdx, true)), wal.opts.segmentCapacity)
			wal.segments = append(wal.segments, nextActiveSegment)
			wal.isSegmentsOrdered = false
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
				logs.Error(err.Error())
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

// Read 读单条日志
//
// 如果在wal已经关闭的情况下尝试读取，会返回 consts.NewFileClosedErr 错误
// 如果在wal数据已经被破坏的情况下尝试读取，会返回 consts.NewCorruptErr 错误
// 如果在wal后台协程执行失败的情况下尝试读取，会返回 consts.NewBackgroundErr 错误
//
// 参数：
// - idx 指定要读的日志blockIdx
//
// 返回值：
// - data idx对应的日志数据内容，类型是字节数组
// - errs 过程中出现的错误，类型是 *consts.KvErr
func (wal *Log) Read(idx int64) ([]byte, error) {
	blocks, err := wal.read(idx, idx)
	if err != nil {
		return nil, err
	}

	return blocks[0], nil
}

// MRead 批量读日志
//
// 如果在wal已经关闭的情况下尝试读取，会返回 consts.NewFileClosedErr 错误
// 如果在wal数据已经被破坏的情况下尝试读取，会返回 consts.NewCorruptErr 错误
// 如果在wal后台协程执行失败的情况下尝试读取，会返回 consts.NewBackgroundErr 错误
//
// 参数：
// - idx 指定读取 [firstBlockIdx, idx] 范围内的日志记录
//
// 返回值：
// - datas 查询到的日志数据列表
// - errs 过程中出现的错误，类型是 *consts.KvErr
func (wal *Log) MRead(idx int64) ([][]byte, error) {
	return wal.read(wal.firstBlockIdx, (wal.firstBlockIdx+idx)%getMaxBlockCapacityInWAL())
}

func (wal *Log) read(startBlockIdx, endBlockIdx int64) ([][]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return nil, err
	}

	err = wal.checkRange(startBlockIdx, endBlockIdx)
	if err != nil {
		return nil, err
	}

	var logContents [][]byte
	for _, idx := range wal.getBlockIdxListByRange(startBlockIdx, endBlockIdx) {
		targetSegment := wal.findSegment(idx)
		blockId := targetSegment.getStartBlockIdx()
		var seg *segment
		if cachedSegment := wal.segmentCache.Read(blockId); cachedSegment != nil {
			seg = cachedSegment.(*segment)
		} else {
			if targetSegment != wal.activeSegment {
				err = targetSegment.open(wal.opts.dataPerm)
				if err != nil {
					return nil, err
				}
			}

			eliminatedSegment := wal.segmentCache.Write(blockId, targetSegment)
			if eliminatedSegment != nil {
				err := eliminatedSegment.(*segment).close()
				if err != nil {
					return nil, err
				}
			}

			seg = targetSegment
		}

		log, err := seg.read(idx)
		if err != nil {
			return nil, err
		}

		logContents = append(logContents, log)
	}

	return logContents, nil
}

// getBlockIdxListByRange 查找给定blockIdx范围内的所有blockIdx
func (wal *Log) getBlockIdxListByRange(startBlockIdx, endBlockIdx int64) []int64 {
	var idxList []int64
	blockCapacity := getMaxBlockCapacityInWAL()
	if endBlockIdx < startBlockIdx {
		endBlockIdx += blockCapacity
	}
	for i := startBlockIdx; i <= endBlockIdx; i++ {
		idxList = append(idxList, i%blockCapacity)
	}

	return idxList
}

// Sync 同步内存中的日志数据到磁盘中
//
// 如果在wal已经关闭的情况下尝试同步，会返回 consts.NewFileClosedErr 错误
// 如果在wal数据已经被破坏的情况下尝试同步，会返回 consts.NewCorruptErr 错误
// 如果在wal后台协程执行失败的情况下尝试同步，会返回 consts.NewBackgroundErr 错误
//
// 返回值：
// - errs 过程中出现的错误，类型是 *consts.KvErr
func (wal *Log) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return err
	}

	return wal.activeSegment.sync()
}

// Truncate 截断指定范围内的日志
//
// 当出现日志循环时，firstBlockIdx 比指定的 idx 大
// 此时的截断范围是 [0, idx) & [firstBlockIdx, mask]
// 截断后 firstBlockIdx 会发生改变
//
// 参数：
// - idx 指定截断 [firstBlockIdx, idx] 范围内的日志记录
//
// 返回值：
// - errs 过程中出现的错误，类型是 *consts.KvErr
//
// example1：
//
//	for i := 0; i < 100; i++ {
//			_, errs := wal.Write([]byte{1, 2, 3})
//	} // 循环执行完成后，wal中 firstBlockIdx 为0，blockId的范围是[0, 100]
//
//	/* 截断 [0, 50) 范围内的block，此时wal中维护的block范围是[50, 100]
//	firstBlockIdx 是50 */
//	wal.truncate(50)
//
// example2:
//
//	// 继续上面的例子，假设wal能够容纳的最大block数量是100，那么我们继续向wal中写的日志blockId会从0开始循环
//	for i := 0; i < 20; i++ {
//			_, errs := wal.Write([]byte{1, 2, 3})
//	} // 循环执行完成后，wal中 firstBlockIdx 为50，blockId的范围是[0, 20] & [50, 100]
//
//	/* 截断 [0, 10) & [50, 100]范围内的block，此时wal中维护的block范围是[10, 20]
//	firstBlockIdx 是10 */
//	wal.truncate(10)
func (wal *Log) Truncate(idx int64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return err
	}

	err = wal.checkRange(idx)
	if err != nil {
		return err
	}

	var segmentsTidy []*segment
	targetSegmentFirstBlockIdx := wal.findSegment(idx).getStartBlockIdx()
	for _, seg := range wal.segments {
		firstBlockIdxInSegment := seg.getStartBlockIdx()
		// 筛选需要truncate的segment范围
		c1 := wal.firstBlockIdx <= wal.lastBlockIdx && (wal.firstBlockIdx <= firstBlockIdxInSegment && firstBlockIdxInSegment <= targetSegmentFirstBlockIdx)
		c2 := wal.firstBlockIdx > wal.lastBlockIdx && (firstBlockIdxInSegment <= targetSegmentFirstBlockIdx || wal.firstBlockIdx >= firstBlockIdxInSegment)
		if c1 || c2 {
			// note：清除缓存，否则原缓存key会因截断后startBlockId修改而导致不可访问/浪费内存
			cachedSegment := wal.segmentCache.Remove(seg.getStartBlockIdx())
			isCached, isActive := cachedSegment != nil, seg == wal.activeSegment
			if !isCached && !isActive {
				err := seg.open(wal.opts.dataPerm)
				if err != nil {
					return err
				}
			}

			empty, err := seg.truncate(idx)
			if err != nil {
				return err
			}

			if empty && !isActive {
				err := seg.close()
				if err != nil {
					return err
				}

				err = os.Remove(seg.path)
				if err != nil {
					e := errs.NewRemoveFileErr()
					logs.Error(e.Error())
					return e
				}
				continue
			}

			if !isCached && !isActive {
				err := seg.close()
				if err != nil {
					return err
				}
			}

			// 缓存中的segment文件如果没有被删除，重写回缓存
			if isCached {
				wal.segmentCache.Write(cachedSegment.(*segment).getStartBlockIdx(), cachedSegment)
			}

			segmentsTidy = append(segmentsTidy, seg)
		}
	}

	wal.segments = segmentsTidy
	wal.firstBlockIdx = idx + 1
	wal.isSegmentsOrdered = false
	return nil
}

// findSegment 根据blockIdx查找存储该block的 segment
func (wal *Log) findSegment(idx int64) *segment {
	// note: 二分搜索要求序列有序
	wal.sortSegments()
	target := sort.Search(len(wal.segments), func(i int) bool {
		return wal.segments[i].getStartBlockIdx() > idx
	})

	if target != 0 {
		target -= 1
	}

	return wal.segments[target]
}

// sortSegments 顺序排序 segments
func (wal *Log) sortSegments() {
	if wal.isSegmentsOrdered {
		return
	}

	sort.Slice(wal.segments, func(i, j int) bool {
		return wal.segments[i].firstBlockIdx < wal.segments[j].firstBlockIdx
	})
	wal.isSegmentsOrdered = true
}

// checkState 检查wal中的状态位
// 根据传入的参数动态决定那些状态检查那些状态不检查
func (wal *Log) checkState(closed, corrupted, bgfailed bool) error {
	if closed && wal.closed {
		e := errs.NewFileClosedErr()
		logs.Error(e.Error())
		return e
	} else if corrupted && wal.corrupted {
		e := errs.NewCorruptErr()
		logs.Error(e.Error())
		return e
	} else if bgfailed && wal.bgfailed {
		e := errs.NewBackgroundErr()
		logs.Error(e.Error())
		return e
	}
	return nil
}

// checkRange 检查传入的blockIdx是否是有效
// 认为wal中仍在维护的blockIdx是有效的
func (wal *Log) checkRange(idxs ...int64) error {
	for _, idx := range idxs {
		// 正常情况，lastBlockIdx 比 firstBlockIdx 大
		// 认为idx比 firstBlockIdx 小或者idx比 lastBlockIdx 时参数非法
		if wal.firstBlockIdx <= wal.lastBlockIdx && (idx < wal.firstBlockIdx || idx > wal.lastBlockIdx) {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.Params, "idxs"), zap.Int64s(consts.Value, idxs))
			return e
		}

		// 当日志开始循环，可能出现 lastBlockIdx 比 firstBlockIdx 小的情况
		// 此时认为idx在 lastBlockIdx 与 firstBlockIdx 之间为非法情况
		if wal.firstBlockIdx > wal.lastBlockIdx && (wal.lastBlockIdx < idx && idx < wal.firstBlockIdx) {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.Params, "idxs"), zap.Int64s(consts.Value, idxs))
			return e
		}
	}

	return nil
}

// checkDataSize 检查写入的日志数据大小是否合法
// 当单次写入的日志数据大小为0或者超过单个 segment 的最大容量时会返回 consts.NewInvalidParamErr 错误
func (wal *Log) checkDataSize(data []byte) error {
	lengthOfData := int64(len(data))
	if lengthOfData == 0 || lengthOfData > wal.opts.segmentCapacity {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.Params, "lengthOfData"), zap.Int64(consts.Value, lengthOfData))
		return e
	}
	return nil
}

func (wal *Log) Len() (int64, error) {
	err := wal.checkState(true, true, true)
	if err != nil {
		return 0, err
	}

	if wal.firstBlockIdx <= wal.lastBlockIdx {
		return wal.lastBlockIdx - wal.firstBlockIdx, nil
	}

	return wal.lastBlockIdx + getMaxBlockCapacityInWAL() - wal.firstBlockIdx, nil
}
