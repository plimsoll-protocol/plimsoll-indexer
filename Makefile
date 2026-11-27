.PHONY: all build test lint fmt generate

SWAG ?= $(shell which swag 2>/dev/null || echo $(shell go env GOPATH)/bin/swag)

all: build

build:
	go build -o bin/plimsoll-indexer ./cmd/plimsoll-indexer

test:
	go test -race ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .

generate:
	@which swag >/dev/null 2>&1 || which $(SWAG) >/dev/null 2>&1 || go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc6
	@SWAG_BIN=$$(which swag 2>/dev/null || echo $(SWAG)); \
	$$SWAG_BIN init -d internal/api -g api.go -o docs --ot yaml --v3.1 --parseInternal
	@if [ -f docs/swagger.yaml ]; then mv docs/swagger.yaml docs/openapi.yaml; fi
