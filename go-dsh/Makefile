GO ?= go
PROTOC ?= protoc
TOOLS_BIN := $(CURDIR)/.tools/bin
PROTO_DIR := api/proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto' -type f | sort)
PROTO_BASELINE := $(PROTO_DIR)/baseline/v1.pb
PROTO_CURRENT := .tools/proto-current.pb

.PHONY: tools proto proto-compat proto-baseline-update fmt test test-race test-integration vet build phase0-accept phase1-accept

tools:
	GOBIN=$(TOOLS_BIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	GOBIN=$(TOOLS_BIN) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1

proto: tools
	PATH="$(TOOLS_BIN):$$PATH" $(PROTOC) \
		-I $(PROTO_DIR) \
		--go_out=. --go_opt=module=github.com/yy59750901/go-dsh \
		--go-grpc_out=. --go-grpc_opt=module=github.com/yy59750901/go-dsh \
		$(PROTO_FILES)

proto-compat: proto
	$(PROTOC) -I $(PROTO_DIR) --include_imports --descriptor_set_out=$(PROTO_CURRENT) $(PROTO_FILES)
	$(GO) run ./cmd/proto-compat -baseline $(PROTO_BASELINE) -current $(PROTO_CURRENT) -owned-prefix dsh/protocol/

proto-baseline-update: proto-compat
	cp $(PROTO_CURRENT) $(PROTO_BASELINE)

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test -tags=integration ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build ./...

phase0-accept: proto-compat fmt test test-race test-integration build

phase1-accept: proto-compat fmt test test-race vet build
