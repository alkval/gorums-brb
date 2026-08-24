GOCACHE := $(CURDIR)/.cache/go-build
GO := GOCACHE=$(GOCACHE) GOTOOLCHAIN=go1.26.7 go

.PHONY: fmt test vet

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...
