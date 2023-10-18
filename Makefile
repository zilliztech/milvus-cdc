PWD 	  := $(shell pwd)

test-go:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

lint-fix:
	@echo "Running gofumpt fix"
	@gofumpt -l -w ./
	@echo "Running gci fix"
	@gci write ./ --skip-generated -s standard -s default -s "prefix(github.com/zilliztech)" --custom-order

static-check:
	@echo "Running go-lint check:"
	@(env bash $(PWD)/scripts/run_go_lint.sh)

# TODO use the array to generate the name list
CORE_API := DataHandler MessageManager MetaOp Reader ChannelManager Writer FactoryCreator

generate-mockery:
	@echo "Generating mockery server mocks..."
	@echo "Joined string: $(shell echo $(strip $(CORE_API)) | tr ' ' '|')"
	@echo ""

#	@cd "$(PWD)/server"; mockery -r --name "CDCService|CDCFactory|MetaStore|MetaStoreFactory" --output ./mocks --case snake --with-expecter
	@cd "$(PWD)/core"; mockery -r --name "$(shell echo $(strip $(CORE_API)) | tr ' ' '|')" --output ./mocks --case snake --with-expecter