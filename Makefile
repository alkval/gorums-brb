GO := GOTOOLCHAIN=go1.26.7 go
GORUMS_PROTO_DIR = $(shell $(GO) list -m -f '{{.Dir}}' github.com/relab/gorums)
PROTO_FILES = $(shell find proto -name '*.proto' -type f)

.PHONY: fmt generate test vet

fmt:
	$(GO) fmt ./...

generate:
	protoc -I="$(GORUMS_PROTO_DIR):." \
		--go_out=paths=source_relative:. \
		--gorums_out=paths=source_relative:. \
		--go_opt=default_api_level=API_OPAQUE \
		$(PROTO_FILES)

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...
