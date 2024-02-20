// Package wal 实现预写日志（Write-Ahead-Log）
//
// Log 致力于对外提供高性能读、写、截断日志服务，这些操作通过内部加锁保证外部
// 并发调用不会出现竞态条件，需要注意的是：Open 操作会有竞态问题，请避免并发
// 打开日志，或者外部主动加锁避免竞态条件。
//
// Log 提供两种数据持久化级别：
//  1. 同步：每次写日志都会将数据同步到磁盘，一致性好，但性能差。
//  2. 异步：日志会先写入内存缓冲中，后台协程周期同步，一致性稍差，但性能优异。
//
// Log 允许通过 Options 自定义配置选项，配置包括权限、容量、持久化级别、同步
// 周期。下面简要介绍其中涉及到的内部实现概念：
//  1. Block：描述单条日志记录的逻辑概念。用于定位日志记录边界、记录完整性校验、
//     限制 Log 下最大日志数量（通过maxBlockCapacityInWAL）。
//  2. Segment：日志数据文件，由零或多个 Block 组成。一个 Log 中通常有多个 Segment
//     这是出于变更 Segment 文件时写时复制（Copy-On-Write）的性能考虑，这意味着
//     Segment 保证原子写入文件。Segment 允许自定义容量（可通过 Options 配置），
//     但这不意味只有当一个数据文件写满后才会创建下一个，Segment 会保证 Block完整地
//     存在一个数据文件中。
//  3. Log：先行日志实体，由多个 Segment 组成。Log 将 Segment 维护在指定目录
//     下（可通过 Options 配置）。内部通过 LRU 缓存最近读命中的 Segment，同样的，
//     可以通过 Options 配置 LRU 缓存大小。
//
// TODO(Trino)：日志损坏后没有修复机制，如果出现内容被篡改、损坏会导致日志不可用
package wal