package wal

import "github.com/Trinoooo/eggie_kv/utils"

const dirLock = ".lock"

// getMaxBlockCapacityInWAL 获取wal目录下最大能容纳的block数量
func getMaxBlockCapacityInWAL() int64 {
	return int64(utils.GetValueOnEnv(1e10, 1e8).(float64))
}

const (
	// 字段长度，单位字节
	headerLengthSize  = 8
	headerBlockIdSize = 8
	headerSummarySize = 16
	headerSize        = 32

	// 字段偏移量，单位字节
	headerLengthOffset  = 0
	headerBlockIdOffset = 8
	headerSummaryOffset = 16
	headerDataOffset    = 32
)

const suffix = ".active" // suffix 活跃segment文件的后缀标识

// getBaseFormat 获取segment文件名中blockIdx部分宽度
func getBaseFormat() string {
	return utils.GetValueOnEnv("%010d", "%08d").(string)
}

// SyncMode 持久化模式
// 参考 https://trinoooo.github.io/eggie_kv/docs/core/ragdoll/write_ahead_log/#%E6%8C%81%E4%B9%85%E5%8C%96%E7%BA%A7%E5%88%AB
type SyncMode int64

const (
	FullManagedSync  SyncMode = iota // 全托管 - 同步
	FullManagedAsync                 // 全托管 - 异步
	SelfManaged                      // 自托管
)
