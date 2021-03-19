APPNAME = "generator"
GOROOT = "../../"

.PHONY: build
build:
	GOROOT="../../" go build -o ./bin/$(APPNAME) ./cmd/${APPNAME}

.PHONY: run
run: build
	./$(APPNAME)

.PHONY: test
test:
	go test -v -race -timeout 30s ./...

.PHONY: dev
dev: test run

.DEFAULT_GOAL := build