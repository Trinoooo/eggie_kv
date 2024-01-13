# eggie_kv技术方案
## 功能点
1. kv存储服务化部署，提供cli与client sdk
2. 支持数据持久化存储（内存数据库）与非持久化存储（缓存）的kv nosql
3. 基于跳表维护数据记录

## 模块间关系图
```text
.
├── components
│    ├── cli // server端cli
│    │    └── cli.go
│    ├── kv // 核心存储模块
│    │    ├── channel.go // 通信管道
│    │    ├── data.go // 数据存储代理
│    │    ├── kv.go // kv数据库
│    │    ├── utils.go // 工具
│    │    └── wal.go // 先行日志代理
│    └── server // 网络通信模块
│        ├── handler.go
│        ├── mw.go // 中间件
│        └── server.go
├── consts
│   ├── common.go
│   └── error.go
├── eggie_kv_server
├── go.mod
├── go.sum
├── main.go
└── utils
├── chan.go
├── chan_test.go
└── utils_test
```

