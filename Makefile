SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

LDFLAGS ?= -X main.version=$(VERSION)
TEST_FLAGS ?= -count=1

GO ?= go
APPSOURCES := $(wildcard ./*.go)
PROJECT_NAME := storage-fs

export CGO_ENABLED=0

ifneq ($(ENV), dev)
	LDFLAGS += -s -w -extldflags "-static"
	BUILDFLAGS += -trimpath
endif

TEST := $(GO) test $(BUILDFLAGS)

.PHONY: test coverage download

download:
	$(GO) mod download all
	$(GO) mod tidy

test: TEST_TARGET := . ./...
test: download
	$(TEST) $(TEST_FLAGS) -tags "$(TAGS)" $(TEST_TARGET)

coverage: TEST_TARGET := .
coverage: TEST_FLAGS += -covermode=count -coverprofile $(PROJECT_NAME).coverprofile
coverage: test

