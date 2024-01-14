ProjectPath=$(GOPATH)/src/github.com/Trinoooo/eggieKv

build-core:
	cd $(ProjectPath)/core && \
	go mod tidy && \
	go build -o eggie_kv_server main.go

build-interactive-cli:
	cd $(ProjectPath)/interactive/cli && \
	go mod tidy && \
	go build -o eggie_kv_client main.go

build-all: build-core build-interactive-cli