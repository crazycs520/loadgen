PROJECT=load
PACKAGE_LIST  := go list ./...
PACKAGES  ?= $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/crazycs520/load/||' | sed 's|github.com/crazycs520/load||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'


default: build

all: default

build:
	go build -o bin/load main.go

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
