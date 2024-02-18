ProjectPath=$(GOPATH)/src/github.com/Trinoooo/eggieKv

# 构建产物
build-storage:
	cd $(ProjectPath)/storage && \
	go mod tidy && \
	go build -o eggie_kv_server main.go

build-cli:
	cd $(ProjectPath)/cli && \
	go mod tidy && \
	go build -o eggie_kv_client main.go

build-all: build-storage build-cli

# 测试
TestPackage := $(test_package)
test-with-cover:
	EGGIE_KV_ENV='test' go test -timeout=1h $(TestPackage) -v -coverprofile=./c.out -count=1

BenchmarkPackage := $(benchmark_package)
BenchmarkTarget := $(benchmark_target)
BenchmarkCount := $(benchmark_count)
BenchmarkProfDir=$(BenchmarkPackage)/benchmark
benchmark:
	go test $(BenchmarkPackage) -benchmem -bench=$(BenchmarkTarget) -count=$(BenchmarkCount) \
	-blockprofile $(BenchmarkProfDir)/block.out -cpuprofile $(BenchmarkProfDir)/cpu.out -memprofile $(BenchmarkProfDir)/mem.out \
    -mutexprofile $(BenchmarkProfDir)/mutex.out -trace $(BenchmarkProfDir)/trace.out
