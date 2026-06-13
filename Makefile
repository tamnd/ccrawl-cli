BINARY  := ccrawl
PKG     := ./cmd/ccrawl
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/tamnd/ccrawl-cli/cli.Version=$(VERSION) \
	-X github.com/tamnd/ccrawl-cli/cli.Commit=$(COMMIT) \
	-X github.com/tamnd/ccrawl-cli/cli.Date=$(DATE)

.PHONY: build install test vet lint fmt clean run

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) $(PKG)

install:
	CGO_ENABLED=0 go install -trimpath -ldflags "$(LDFLAGS)" $(PKG)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w -s .

clean:
	rm -f $(BINARY)
	rm -rf dist

run: build
	./$(BINARY) $(ARGS)
