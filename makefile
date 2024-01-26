ProjectPath=$(GOPATH)/src/github.com/Trinoooo/eggieKv

# 构建产物
build-kv-storage:
	cd $(ProjectPath)/kv_storage && \
	go mod tidy && \
	go build -o eggie_kv_server main.go

build-interactive-cli:
	cd $(ProjectPath)/cli && \
	go mod tidy && \
	go build -o eggie_kv_client main.go

build-all: build-core build-interactive-cli

# 测试
TestPackage := $(test_package)
TestCoverageFile=$(TestPackage)/c.out
TestCoverageHtml=$(TestPackage)/coverage.html
test-with-cover:
	go test $(TestPackage) -v -coverprofile=$(TestCoverageFile) && \
	go tool cover -html=$(TestCoverageFile) -o=$(TestCoverageHtml) && \
	rm -f $(TestCoverageFile) && \
	open $(TestCoverageHtml)