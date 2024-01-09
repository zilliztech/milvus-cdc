PWD 	  := $(shell pwd)

build:
	$(MAKE) -C server build

test-go:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

lint-fix:
	@echo "Running gofumpt fix"
	@gofumpt -l -w ./
	@echo "Running gci fix"
	@gci write ./ -s standard -s default -s "prefix(github.com/zilliztech)" --custom-order --skip-generated

static-check:
	@echo "Running go-lint check:"
	@(env bash $(PWD)/scripts/run_go_lint.sh)

CORE_API := DataHandler MessageManager MetaOp Reader ChannelManager TargetAPI Writer FactoryCreator
SERVER_API := MetaStore MetaStoreFactory CDCService

generate-mockery:
	@echo "Generating mockery server mocks..."

	@cd "$(PWD)/core"; mockery -r --name "$(shell echo $(strip $(CORE_API)) | tr ' ' '|')" --output ./mocks --case snake --with-expecter
	@cd "$(PWD)/server"; mockery -r --name "$(shell echo $(strip $(SERVER_API)) | tr ' ' '|')" --output ./mocks --case snake --with-expecter