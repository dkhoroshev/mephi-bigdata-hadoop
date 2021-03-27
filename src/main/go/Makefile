APPNAME = generator
DIR = $(shell pwd)/../../

.PHONY: build
build:
	GOPATH=$(DIR) go build -v -o ./bin/$(APPNAME) ./cmd/$(APPNAME)

.PHONY: run
run: build
	bin/$(APPNAME) -r 1200x800 -x 2 -y 3 -t 1000,4000

.PHONY: test
test:
	GOPATH=$(DIR) go test -v -race -timeout 30s ./...

.PHONY: dev
dev: test run

.DEFAULT_GOAL := build