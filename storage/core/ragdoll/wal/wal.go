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

// Options 日志配置选项
type Options struct {
	dataPerm             os.FileMode // dataPerm 先行日志数据文件权限位
	dirPerm              os.FileMode // dirPerm 先行日志目录文件权限位
	segmentCapacity      int64       // segmentCapacity segment 数据文件最大存储容量
	segmentCacheCapacity int         // segmentCacheCapacity 内存中缓存 segment 的最大数量
	// noSync 当设置为true时，只有 segment 写满才持久化到磁盘。
	// 可以提高写性能，但需要容忍同步周期内数据丢失的风险。
	noSync       bool
	syncInterval time.Duration // syncInterval 刷盘周期，单位是毫秒
}

// NewOptions 初始化wal配置选项
//
// 返回值：
//   - 接收器wal配置选项（*options）
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

// SetDataPerm 设置 segment 数据文件权限位
//
// 该配置在创建新 segment 时会用到，已经存在的 segment 不会受到影响
//
// 参数：
//   - dataPerm segment 数据文件权限位
//
// 返回值：
//   - 接收器wal配置选项（*options）
func (opts *Options) SetDataPerm(dataPerm os.FileMode) *Options {
	opts.dataPerm = dataPerm
	return opts
}

// SetDirPerm 设置 Log 日志目录权限位
//
// 如果在启动 Log 时发现指定dirPath下不存在wal目录，该配置会在新建目录时用到
//
// 参数：
//   - dirPerm Log 目录权限位
//
// 返回值：
//   - 接收器 Log 配置选项（*options）
func (opts *Options) SetDirPerm(dirPerm os.FileMode) *Options {
	opts.dirPerm = dirPerm
	return opts
}

// SetSegmentCapacity 设置 segment 数据文件最大存储容量
//
// segment 文件在写入时通常不会达到文件最大存储容量就开始写入下一个 segment 文件
// 这是因为写入 segment 的最细粒度是 block，单个 block 无法跨 segment 存储
//
// 参数：
//   - segmentCapacity segment 数据文件最大存储容量
//
// 返回值：
//   - 接收器 Log 配置选项（*options）
func (opts *Options) SetSegmentCapacity(segmentCapacity int64) *Options {
	opts.segmentCapacity = segmentCapacity
	return opts
}

// SetSegmentCacheSize 设置wal中的segment最大缓存数量
//
// segment 缓存只有在读取日志内容时生效
//
// 参数：
//   - segmentCacheCapacity LRU 中的 segment 最大缓存数量
//
// 返回值：
//   - 接收器 Log 配置选项（*options）
func (opts *Options) SetSegmentCacheSize(segmentCacheCapacity int) *Options {
	opts.segmentCacheCapacity = segmentCacheCapacity
	return opts
}

// SetNoSync 设置不要每次写入都持久化到磁盘
//
// 当设置为true时，刷盘时机有
//  1. 外部主动调用 wal.Sync
//  2. 当前 segment 文件写满时自动持久化数据到磁盘
//  3. 后台协程每隔 syncInterval 毫秒周期刷盘
//
// 通常情况下设置 noSync 选项后性能会有可观提升
//
// 返回值：
//   - 接收器 Log 配置选项（*options）
func (opts *Options) SetNoSync() *Options {
	opts.noSync = true
	return opts
}

// SetSyncInterval 设置后台协程刷盘时间间隔，单位是毫秒
//
// 只有在设置 SetNoSync 选项后该选项才生效
//
// 参数：
//   - syncInterval 后台协程刷盘时间间隔，单位是毫秒
//
// 返回值：
//   - 接收器 Log 配置选项（*options）
func (opts *Options) SetSyncInterval(syncInterval time.Duration) *Options {
	opts.syncInterval = syncInterval
	return opts
}

// check 校验 Options 配置
func (opts *Options) check() error {
	// note：暂时不考虑特殊权限位
	if opts.dataPerm <= 0 || opts.dataPerm > 0777 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "opts.dataPerm"), zap.Uint32(consts.LogFieldValue, uint32(opts.dataPerm)))
		return e
	}

	if opts.dirPerm <= 0 || opts.dirPerm > 0777 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "opts.dirPerm"), zap.Uint32(consts.LogFieldValue, uint32(opts.dirPerm)))
		return e
	}

	if opts.segmentCacheCapacity < 0 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "opts.segmentCacheCapacity"), zap.Int(consts.LogFieldValue, opts.segmentCacheCapacity))
		return e
	}

	// 这个范围主要是出于性能考虑，segment文件太小会导致频繁开关文件，segment文件太大会有性能问题（占内存过大，频繁缺页等）
	if opts.segmentCapacity < consts.MB || opts.segmentCapacity > consts.GB {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "opts.segmentCapacity"), zap.Int64(consts.LogFieldValue, opts.segmentCapacity))
		return e
	}

	if opts.syncInterval < 0 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "opts.syncInterval"), zap.Duration(consts.LogFieldValue, opts.syncInterval))
		return e
	}
	return nil
}

// Log 预写日志
type Log struct {
	mu           sync.Mutex
	opts         *Options   // opts 传入的配置选项
	dirPath      string     // dirPath 预写日志的目录路径
	dirLockFile  *os.File   // dirLockFile 用于锁目录的锁文件
	segmentCache *utils.Lru // segmentCache segment 缓存，采用LRU缓存策略
	segments     []*segment // segments 所有已知的 segment
	// isSegmentsOrdered 用于优化 segments 排序性能的脏位
	// 设置为true时表示 segments 处于有序状态，重复调用 sortSegments 不会执行排序过程；
	// 设置为false时表示 segments 处于无序状态，此时调用 sortSegments 会执行排序过程。
	isSegmentsOrdered bool
	activeSegment     *segment   // activeSegment 当前活跃数据文件
	firstBlockIdx     int64      // firstBlockIdx 第一个日志块索引，nil表示 Log 为空
	lastBlockIdx      int64      // lastBlockIdx 最后一个日志块索引，nil表示 Log 为空
	closed            bool       // closed 是否已经关闭
	corrupted         bool       // corrupted 是否已损坏
	bgfailed          bool       // bgfailed 后台协程执行失败
	notifier          chan error // notifier wal主协程与子协程的同步管道，只有设置 opts.noSync 为true时会用到
}

// NewLog 初始化 Log
//
// 参数：
//   - dirPath 存放 Log 日志的目录
//   - opts 配置选项，不传配置将设置成默认值，见 NewOptions
//
// 返回值：
//   - wal日志（*Log）
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewInvalidParamErr 传入 opts 校验未通过，日志中有描述具体非法参数；
//     也有可能是传入的目录路径不是一个目录
func NewLog(dirPath string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = NewOptions()
	}

	err := opts.check()
	if err != nil {
		return nil, err
	}

	return &Log{
		dirPath:       dirPath,
		opts:          opts,
		segmentCache:  utils.NewLRU(opts.segmentCacheCapacity),
		firstBlockIdx: -1,
		lastBlockIdx:  -1,
		closed:        true,
	}, nil
}

// Open 打开 Log
//
// 为了避免内存泄漏，结束使用后需要显示调用 Log.Close 关闭
//
// 返回值：
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFlockFileErr Open 指定的dir已经被其他 Log 实例打开，
//     此时外部应该检查这个错误并且重试其他目录路径；有小概率是因为初始化 Log
//     实例过程中出现其他错误（会在日志中输出）之后释放目录锁导致
//   - errs.NewMkdirErr 配置中目录不存在，且尝试创建目录失败
//   - errs.NewFileStatErr 获取目录文件信息失败
//   - errs.NewOpenFileErr 打开目录锁文件、数据失败
//   - errs.NewWalkDirErr 遍历日志目录失败，具体原因请参考日志
//   - errs.NewParseIntErr 解析 segment 文件名失败
//   - errs.NewReadFileErr 读取 segment 文件失败
//   - errs.NewCorruptErr  segment 数据被篡改、损坏
//   - errs.NewBackgroundErr 这个错误不会抛出，但会出现在日志中，表示后台协程执行出错
func (wal *Log) Open() (err error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.opts.noSync {
		wal.notifier = make(chan error)
		defer func() {
			if err != nil {
				return
			}
			wal.notifier <- nil
		}()
	}

	// 打开wal过程出现错误，需要释放目录锁
	defer func() {
		if err != nil {
			err = syscall.Flock(int(wal.dirLockFile.Fd()), syscall.LOCK_UN)
			if err != nil {
				e := errs.NewFlockFileErr().WithErr(err)
				logs.Error(e.Error())
			}
		}
	}()

	err = wal.init()
	if err != nil {
		return err
	}

	wal.closed = false
	return nil
}

// init 检查或创建日志目录，读日志数据文件，初始化 Log
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

	wal.locateBlockRange()

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
		err = os.MkdirAll(wal.dirPath, wal.opts.dirPerm)
		if err != nil {
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
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "stat"), zap.Any(consts.LogFieldValue, stat))
		return e
	}

	return nil
}

// lockDir 锁住目录，避免多个 Log 实例重入同一个目录
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
	err := filepath.WalkDir(wal.dirPath, func(path string, de fs.DirEntry, err error) error {
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

		currentSegment, err := newSegment(filepath.Join(wal.dirPath, de.Name()), wal.opts.segmentCapacity)
		if err != nil {
			return err
		}
		wal.segments = append(wal.segments, currentSegment)
		firstBlockIdOfSegment, hasSuffix, err := baseToBlockIdx(de.Name())
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
				if firstBlockIdOfSegment > activeSegment.getStartBlockIdx() {
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
	})
	if err != nil {
		e := errs.NewWalkDirErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	// 首次启动目录下没有segment
	if activeSegment == nil {
		activeSegment, err = newSegment(filepath.Join(wal.dirPath, blockIdxToBase(0, true)), wal.opts.segmentCapacity)
		if err != nil {
			return err
		}
		wal.segments = append(wal.segments, activeSegment)
	}
	wal.activeSegment = activeSegment
	err = activeSegment.open(wal.opts.dataPerm)
	if err != nil {
		// todo：调研 损坏恢复
		if errs.GetCode(err) == errs.CorruptErrCode {
			wal.corrupted = true
		}
		return err
	}

	return nil
}

// locateBlockRange 从 segments 中定位 firstBlockId 和 lastBlockId
func (wal *Log) locateBlockRange() {
	wal.sortSegments()
	for idx, seg := range wal.segments {
		if seg == wal.activeSegment {
			firstSegment := wal.segments[(idx+1)%len(wal.segments)]
			size := seg.size()
			if size > 0 {
				wal.firstBlockIdx = firstSegment.getStartBlockIdx()
			}
		}
	}

	// wal.firstBlockIdx 意味着 Log 中只有一个 segment，且 segment 中没有 block
	// 如果 activeSegment 不是 firstSegment，且 activeSegment 中没有 block
	// wal.lastBlockIdx 应该取到上一个 segment 的最后一个 blockIdx
	if wal.firstBlockIdx != -1 {
		wal.lastBlockIdx = wal.activeSegment.getStartBlockIdx() + wal.activeSegment.size() - 1
	}
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

// Close 关闭 Log，释放资源
//
// 返回值：
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFileClosedErr 在 Log 或 segment 已经关闭后再次尝试关闭
//   - errs.NewFlockFileErr 释放目录锁失败
func (wal *Log) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, false, false)
	if err != nil {
		return err
	}

	// 清空 segmentCache，关闭其中缓存的 segment 文件
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

	// 关闭 activeSegment 文件
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

// Write 写入日志数据
//
// 参数：
//   - data 日志数据，类型是字节数组
//
// 返回值：
//   - blockIdx block 索引，该值用于 Read 和 Truncate
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewWalFullErr Log 日志文件夹下存储内容已满，使用者应该主动检查该类型错误
//     并在 wal.Truncate 后重新尝试写入，此时 Log 不会关闭，因此对 Log 的操作是安全的
//   - errs.NewFileClosedErr 在wal已经关闭的情况下写入
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下写入
//   - errs.NewBackgroundErr 在wal后台协程执行失败的情况下写入
//   - errs.NewInvalidParamErr 单次写入数据超过 segment 文件容量或写入空数据
//   - errs.NewRenameFileErr 重命名 segment 文件失败
//   - errs.NewParseIntErr 解析 segment 文件名失败
//   - errs.NewCreateTempFileErr 创建临时文件失败
//   - errs.NewSeekFileErr 重定位 segment 文件偏移量失败
//   - errs.NewCopyFileErr 拷贝 segment 文件内容失败
//   - errs.NewCloseFileErr 关闭 segment 文件失败
//   - errs.NewWriteFileErr 写入 segment 文件失败
//   - errs.NewSyncFileErr 同步 segment 文件到磁盘失败
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
	// 需要考虑 lastBlockIdx 追上 firstBlockIdx 的情况
	nextBlockIdx := (wal.lastBlockIdx + 1) % getMaxBlockCapacityInWAL()
	if nextBlockIdx == wal.firstBlockIdx {
		e := errs.NewWalFullErr()
		logs.Error(e.Error())
		return 0, e
	}
	wal.lastBlockIdx = nextBlockIdx

	err = wal.activeSegment.write(data)
	if err != nil {
		// 1. 如果当前 segment 满了，那么新开一个 segment
		// 2. 如果写 segment 时发现 segment 内部 blockIdx 已经触达 blockCapacity 上限，那么 blockIdx 从零开始计数新开一个 segment
		if errs.GetCode(err) == errs.SegmentFullErrCode || errs.GetCode(err) == errs.ReachBlockIdxLimitErrCode {
			err := wal.activeSegment.close()
			if err != nil {
				return 0, err
			}

			nextActiveSegment, err := newSegment(filepath.Join(wal.dirPath, blockIdxToBase(wal.lastBlockIdx, true)), wal.opts.segmentCapacity)
			if err != nil {
				return 0, err
			}
			wal.segments = append(wal.segments, nextActiveSegment)
			wal.isSegmentsOrdered = false
			err = nextActiveSegment.open(wal.opts.dataPerm)
			if err != nil {
				return 0, err
			}

			// 在下个 segment 文件创建成功后再去掉 activeSegment 文件名
			// 避免出现目录下出现没有 segment 文件有.active后缀的情况
			// 如果 rename 失败，目录下会出现多个 segment 文件有.active
			// 后缀的情况，wal.init()会选择有.active后缀且 startBlockId
			// 最大的 segment 文件作为 activeSegment
			err = wal.activeSegment.rename()
			if err != nil {
				return 0, err
			}
			wal.activeSegment = nextActiveSegment

			// 如果 segmentSize 设置小于一次单日志数据最大体积
			// 那么可能出现新建一个 segment 也写入失败的问题
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

// Read 读指定范围内的日志
//
// 当出现日志循环时，firstBlockIdx 比合法的 idx 大
// 此时的读取范围是 [0, idx) & [firstBlockIdx, maxBlockCapacityInWAL]
// 截断后 firstBlockIdx 会发生改变
//
// 参数：
//   - idx 指定读取 firstBlockIdx ~ idx 范围内的日志记录
//
// 返回值：
//   - blockIdxToData 查询到的日志数据映射，blockIdx -> data
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFileClosedErr 在wal已经关闭的情况下读取
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下读取
//   - errs.NewBackgroundErr 在wal后台协程执行失败的情况下读取
//   - errs.NewNotFoundErr 传入idx不在有效范围内
//   - errs.NewOpenFileErr 打开目录锁文件、数据失败
//   - errs.NewReadFileErr 读取 segment 文件失败
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下写入
func (wal *Log) Read(idx int64) (map[int64][]byte, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return nil, err
	}

	err = wal.checkRange(idx)
	if err != nil {
		e := errs.NewNotFoundErr().WithErr(err)
		logs.Error(e.Error())
		return nil, e
	}

	blockIdxToData := make(map[int64][]byte)
	err = wal.traverseSegments(idx, func(seg *segment) error {
		if !seg.isOpened() {
			err := seg.open(wal.opts.dataPerm)
			if err != nil {
				return err
			}
		}

		eliminated := wal.segmentCache.Write(seg.getStartBlockIdx(), seg)
		if eliminated != nil {
			err := eliminated.(*segment).close()
			if err != nil {
				return err
			}
		}

		partial, err := seg.read(idx)
		if err != nil {
			return err
		}

		for blockIdx, data := range partial {
			blockIdxToData[blockIdx] = data
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return blockIdxToData, nil
}

// Sync 同步内存中的日志数据到磁盘中
//
// 返回值：
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFileClosedErr 在 Log、segment 已经关闭的情况下同步
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下同步
//   - errs.NewBackgroundErr 在wal后台协程执行失败的情况下同步
//   - errs.NewCreateTempFileErr 创建临时文件失败
//   - errs.NewSeekFileErr 重定位 segment 文件偏移量失败
//   - errs.NewCopyFileErr 拷贝 segment 文件内容失败
//   - errs.NewCloseFileErr 关闭 segment 文件失败
//   - errs.NewWriteFileErr 写入 segment 文件失败
//   - errs.NewSyncFileErr 同步 segment 文件到磁盘失败
func (wal *Log) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.checkState(true, true, true)
	if err != nil {
		return err
	}

	return wal.activeSegment.sync()
}

// Len 获取 Log 日志中存储的日志数量，即 block 数量
//
// 返回值：
//   - number 日志数量
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFileClosedErr 在wal已经关闭的情况下调用
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下调用
//   - errs.NewBackgroundErr 在wal后台协程执行失败的情况下调用
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

// Truncate 截断指定范围内的日志
//
// 当出现日志循环时，firstBlockIdx 比指定的 idx 大
// 此时的截断范围是 [0, idx) & [firstBlockIdx, maxBlockCapacityInWAL]
// 截断后 firstBlockIdx 会发生改变
//
// 参数：
//   - idx 指定截断 firstBlockIdx ～ idx 范围内的日志记录
//
// 返回值：
//   - errs 过程中出现的错误，类型是 *errs.KvErr
//
// 异常：
//   - errs.NewFileClosedErr 在wal已经关闭的情况下写入
//   - errs.NewCorruptErr 在wal数据已经被破坏的情况下写入
//   - errs.NewBackgroundErr 在wal后台协程执行失败的情况下写入
//   - errs.NewInvalidParamErr 单次写入数据超过 segment 文件容量或写入空数据
//   - errs.NewRenameFileErr 重命名 segment 文件失败
//   - errs.NewCreateTempFileErr 创建临时文件失败
//   - errs.NewSeekFileErr 重定位 segment 文件偏移量失败
//   - errs.NewCopyFileErr 拷贝 segment 文件内容失败
//   - errs.NewCloseFileErr 关闭 segment 文件失败
//   - errs.NewWriteFileErr 写入 segment 文件失败
//   - errs.NewSyncFileErr 同步 segment 文件到磁盘失败
//   - errs.NewOpenFileErr 打开 segment 文件失败
//   - errs.NewRemoveFileErr 移除 segment 文件失败
//
// 示例：
//
//	// 循环执行完成后，Log 中 firstBlockIdx 为0，blockId 的范围是 [0, 100]
//	for i := 0; i < 100; i++ {
//		_, errs := wal.Write([]byte{1, 2, 3})
//	}
//
//	// 截断 [0, 50) 范围内的 block，此时 Log 中维护的 block 范围是 [50, 100]，firstBlockIdx 是50
//	wal.truncate(50)
//
//	// 继续上面的例子，假设wal能够容纳的最大block数量是100，那么我们继续向wal中写的日志blockId会从0开始循环
//	// 循环执行完成后，wal中 firstBlockIdx 为50，blockId的范围是[0, 20] & [50, 100]
//	for i := 0; i < 20; i++ {
//		_, errs := wal.Write([]byte{1, 2, 3})
//	}
//
//	// 截断 [0, 10) & [50, 100] 范围内的 block，此时 Log 中维护的 block 范围是 [10, 20]，firstBlockIdx 是10
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

	segmentsToRemove := make(map[int64]*segment)
	err = wal.traverseSegments(idx, func(seg *segment) (err error) {
		cached := wal.segmentCache.Remove(seg.getStartBlockIdx())
		opened := seg.isOpened()
		if !opened {
			err := seg.open(wal.opts.dataPerm)
			if err != nil {
				return err
			}
		}

		err = seg.truncate(idx)
		if err != nil {
			return err
		}

		empty := seg.size() == 0
		isActive := seg == wal.activeSegment
		isCached := cached != nil
		if empty && !isActive {
			err := seg.remove()
			if err != nil {
				return err
			}

			segmentsToRemove[seg.getStartBlockIdx()] = seg
			return nil
		}

		if !opened {
			err = seg.close()
			if err != nil {
				return err
			}
		}

		if isCached {
			wal.segmentCache.Write(seg.getStartBlockIdx(), seg)
		}

		return nil
	})
	if err != nil {
		return err
	}

	var segmentTidy []*segment
	for _, seg := range wal.segments {
		if segmentsToRemove[seg.getStartBlockIdx()] == nil {
			segmentTidy = append(segmentTidy, seg)
		}
	}
	wal.segments = segmentTidy
	wal.firstBlockIdx = idx + 1
	// 把目录下所有日志全部截断，需要重置 seg.firstBlockIdx 与 seg.lastBlockIdx
	if wal.firstBlockIdx > wal.lastBlockIdx {
		wal.firstBlockIdx = -1
		wal.lastBlockIdx = -1
	}
	wal.isSegmentsOrdered = false
	return nil
}

// traverseSegments 遍历数据文件
// 并将包含 firstBlockIdx ~ idx 范围内 block 的 segment 传入 fn 执行外部动作
func (wal *Log) traverseSegments(idx int64, fn func(seg *segment) error) error {
	targetSegmentFirstBlockIdx := wal.findSegment(idx).getStartBlockIdx()
	for _, seg := range wal.segments {
		firstBlockIdxInSegment := seg.getStartBlockIdx()
		// 筛选需要truncate的segment范围
		c1 := wal.firstBlockIdx <= wal.lastBlockIdx && (wal.firstBlockIdx <= firstBlockIdxInSegment && firstBlockIdxInSegment <= targetSegmentFirstBlockIdx)
		c2 := wal.firstBlockIdx > wal.lastBlockIdx && (firstBlockIdxInSegment <= targetSegmentFirstBlockIdx || wal.firstBlockIdx >= firstBlockIdxInSegment)
		if c1 || c2 {
			err := fn(seg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// findSegment 根据 block 索引查找存储该 block 的 segment
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
		return wal.segments[i].getStartBlockIdx() < wal.segments[j].getStartBlockIdx()
	})
	wal.isSegmentsOrdered = true
}

// checkState 检查 Log 中的状态位
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

// checkRange 检查传入的 block 索引是否是有效
// 认为 Log 中仍在维护的 block 索引是有效的
func (wal *Log) checkRange(idxs ...int64) error {
	for _, idx := range idxs {
		// 正常情况，lastBlockIdx 比 firstBlockIdx 大
		// 认为idx比 firstBlockIdx 小或者 idx 比 lastBlockIdx 时参数非法
		if wal.firstBlockIdx <= wal.lastBlockIdx && (idx < wal.firstBlockIdx || idx > wal.lastBlockIdx) {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.LogFieldParams, "idxs"), zap.Int64s(consts.LogFieldValue, idxs))
			return e
		}

		// 当日志开始循环，可能出现 lastBlockIdx 比 firstBlockIdx 小的情况
		// 此时认为idx在 lastBlockIdx 与 firstBlockIdx 之间为非法情况
		if wal.firstBlockIdx > wal.lastBlockIdx && (wal.lastBlockIdx < idx && idx < wal.firstBlockIdx) {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.LogFieldParams, "idxs"), zap.Int64s(consts.LogFieldValue, idxs))
			return e
		}
	}

	return nil
}

// checkDataSize 检查写入的日志数据大小是否合法
func (wal *Log) checkDataSize(data []byte) error {
	lengthOfData := int64(len(data))
	if lengthOfData == 0 || lengthOfData > wal.opts.segmentCapacity {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "lengthOfData"), zap.Int64(consts.LogFieldValue, lengthOfData))
		return e
	}
	return nil
}
