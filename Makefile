PWD 	  := $(shell pwd)

test-go:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

static-check:
	@echo "Running go-lint check:"
	@(env bash $(PWD)/scripts/run_go_lint.sh)

# TODO use the array to generate the name list
generate-mockery:
	@echo "Generating mockery server mocks..."
	@cd "$(PWD)/server"; mockery -r --name "CDCService|CDCFactory|MetaStore|MetaStoreFactory" --output ./mocks --case snake --with-expecter
	@cd "$(PWD)/core"; mockery -r --name "CDCReader|CDCWriter|FactoryCreator|Monitor|WriteCallback|MilvusClientFactory|MilvusClientAPI|ChannelManager|TargetAPI|MetaOp" --output ./mocks --case snake --with-expecter