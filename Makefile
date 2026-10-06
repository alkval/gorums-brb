GO := GOTOOLCHAIN=go1.26.7 go
PROTO_FILES = $(shell find proto -name '*.proto' -type f)

.PHONY: fmt generate test vet

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
