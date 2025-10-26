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

# Run with structured logs and graceful Ctrl-C
.PHONY: runlogs
runlogs:
	@echo "▶ Running with clean structured logs (Ctrl-C gracefully shuts down)..."
	bash -c '\
		(CGO_CFLAGS="$(CGO_CFLAGS)" MACOSX_DEPLOYMENT_TARGET="$(MACOSX_DEPLOYMENT_TARGET)" \
			go run ./cmd/main.go 2>&1 | \
			maybe-jq "\"[\\(.level|ascii_upcase)] [\\(.caller)] [\\(.ts|todate)] \\(.msg) -- \\(. | del(.ts, .level, .msg, .caller))\"" & \
			pid=$$!; \
			trap "kill -INT $$pid" INT; \
			wait $$pid)'

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
