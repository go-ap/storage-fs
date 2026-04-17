SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GO ?= go
TEST := $(GO) test
TEST_FLAGS ?= -tags conformance
TEST_TARGET ?= .
GO111MODULE = on
PROJECT_NAME := $(shell basename $(PWD))

.PHONY: test coverage clean download

download: go.sum

go.sum: go.mod
	$(GO) mod tidy

test: go.sum clean
	$(TEST) $(TEST_FLAGS) -test.bench=xxxx -cover $(TEST_TARGET) -json | go tool tparse -all

bench: go.sum clean
	$(TEST) $(TEST_FLAGS) -test.bench=. -test.run=xxxxx -cover $(TEST_TARGET) -json | go tool tparse -all

coverage: go.sum clean
	@mkdir ./_coverage
	$(TEST) $(TEST_FLAGS) -covermode=count -args -test.gocoverdir="$(PWD)/_coverage" $(TEST_TARGET) > /dev/null
	$(GO) tool covdata percent -i=./_coverage/ -o $(PROJECT_NAME).coverprofile
	@$(RM) -r ./_coverage

clean:
	@$(RM) -r ./_coverage
	@$(RM) -v *.coverprofile
	@$(RM) -v tests.json

