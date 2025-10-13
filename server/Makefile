# Makefile

# Compiler / platform flags
CGO_CFLAGS := -DHAVE_STRCHRNUL -mmacosx-version-min=15.4
MACOSX_DEPLOYMENT_TARGET := 15.4

# Default target
.PHONY: all
all: build

# Run the Go program
.PHONY: run
run:
	CGO_CFLAGS="$(CGO_CFLAGS)" MACOSX_DEPLOYMENT_TARGET="$(MACOSX_DEPLOYMENT_TARGET)" \
	go run ./cmd/main.go

# Build the binary
.PHONY: build
build:
	CGO_CFLAGS="$(CGO_CFLAGS)" MACOSX_DEPLOYMENT_TARGET="$(MACOSX_DEPLOYMENT_TARGET)" \
	go build -o bin/server ./cmd/main.go

# Run tests
.PHONY: test
test:
	CGO_CFLAGS="$(CGO_CFLAGS)" MACOSX_DEPLOYMENT_TARGET="$(MACOSX_DEPLOYMENT_TARGET)" \
	go test ./...

# Clean up
.PHONY: clean
clean:
	rm -rf bin

