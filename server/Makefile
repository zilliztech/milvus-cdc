OBJPREFIX := "github.com/zilliztech/milvus-cdc/server/tag"
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo $(CDC_GIT_COMMIT))
GO_VERSION = $(shell go version)
BUILD_FLAGS = "-X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'"

build:
	@echo "Building CDC ..."
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc main/main.go