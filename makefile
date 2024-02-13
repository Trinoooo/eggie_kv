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
TestCoverageFile=$(TestPackage)/c.out
TestCoverageHtml=$(TestPackage)/coverage.html
test-with-cover:
	EGGIE_KV_ENV='test' go test $(TestPackage) -v -coverprofile=$(TestCoverageFile) -count=1 && \
	go tool cover -html=$(TestCoverageFile) -o=$(TestCoverageHtml) && \
	rm -f $(TestCoverageFile) && \
	open $(TestCoverageHtml)

BenchmarkPackage := $(benchmark_package)
BenchmarkTarget := $(benchmark_target)
BenchmarkCount := $(benchmark_count)
BenchmarkProfDir=$(BenchmarkPackage)/benchmark
benchmark:
	go test $(BenchmarkPackage) -benchmem -bench=$(BenchmarkTarget) -count=$(BenchmarkCount) \
	-blockprofile $(BenchmarkProfDir)/block.out -cpuprofile $(BenchmarkProfDir)/cpu.out -memprofile $(BenchmarkProfDir)/mem.out \
    -mutexprofile $(BenchmarkProfDir)/mutex.out -trace $(BenchmarkProfDir)/trace.out
