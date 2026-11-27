# syntax=docker/dockerfile:1
# Multi-stage build: compile with the go.mod toolchain, ship a distroless static image.
FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Version and commit are embedded so /v1/version and the startup log identify
# the deploy. Build scripts override them with --build-arg.
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w \
      -X github.com/plimsoll-protocol/plimsoll-indexer/internal/buildinfo.Version=${VERSION} \
      -X github.com/plimsoll-protocol/plimsoll-indexer/internal/buildinfo.Commit=${COMMIT}" \
    -o /out/plimsoll-indexer ./cmd/plimsoll-indexer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/plimsoll-indexer /plimsoll-indexer
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/plimsoll-indexer", "healthcheck"]
ENTRYPOINT ["/plimsoll-indexer"]
