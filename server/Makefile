OBJPREFIX := "github.com/zilliztech/milvus-cdc/server/tag"
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)
BUILD_FLAGS = "-X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'"

build:
	@echo "Building CDC ..."
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc main/main.go