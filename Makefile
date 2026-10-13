GO := go
.DEFAULT_GOAL := test
LINT_VERSION := v2.14.0
PROTO_FILES = $(shell find proto -name '*.proto' -type f)

.PHONY: fix fmt generate test vet lint build bench-linux provenance

fix:
	$(GO) fix ./...

fmt:
	$(GO) fmt ./...

generate:
	$(GO) mod download github.com/relab/gorums
	gorums_dir=$$($(GO) list -m -f '{{.Dir}}' github.com/relab/gorums) && \
		(cd "$$gorums_dir" && GOWORK=off $(GO) build -o "$(CURDIR)/.cache/tools/protoc-gen-gorums" ./cmd/protoc-gen-gorums) && \
		protoc -I="$$gorums_dir:." \
		--plugin=protoc-gen-gorums="$(CURDIR)/.cache/tools/protoc-gen-gorums" \
		--go_out=paths=source_relative:. \
		--gorums_out=paths=source_relative:. \
		--go_opt=default_api_level=API_OPAQUE \
		$(PROTO_FILES)

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

bench-linux:
	GOOS=linux GOARCH=amd64 $(GO) build -o bin/brb-linux-amd64 ./cmd/brb

build:
	$(GO) build -o bin/brb ./cmd/brb

lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VERSION) run

BINARY ?= bin/brb-linux-amd64
provenance:
	@git rev-parse HEAD
	@git status --short
	@$(GO) version -m "$(BINARY)"
	@shasum -a 256 "$(BINARY)"
