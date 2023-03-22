PWD 	  := $(shell pwd)

test:
	@echo "Running go unittests..."
	@(env bash $(PWD)/scripts/run_go_unittest.sh)
