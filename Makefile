PWD 	  := $(shell pwd)

test-go:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)

static-check:
	@echo "Running go-lint check:"
	@(env bash $(PWD)/scripts/run_go_lint.sh)
