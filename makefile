PP=$(GOPATH)/src/github.com/Trinoooo/eggieKv

build-core:
	cd $(PP)/core && \
	go mod tidy && \
	go build -o eggie_kv_server main.go

build-interactive-cli:
	cd $(PP)/interactive/command-line-interface && \
	go mod tidy && \
	go build -o eggie_kv_client main.go

build-all: build-core build-interactive-cli