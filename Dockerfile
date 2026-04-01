# ── Build Stage ──────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# Install git for module downloads and ca-certificates for TLS
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Cache dependencies before copying source (layer cache optimisation)
COPY batcher/go.mod batcher/go.sum ./
RUN go mod download

# Copy source
COPY batcher/ .

# Build a statically linked binary with all debug info stripped
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-s -w -X main.version=$(date -u +%Y%m%d%H%M%S)" \
    -o /batcher \
    ./cmd/batcher

# ── Runtime Stage ─────────────────────────────────────────────────────────────
# distroless/static has no shell, no package manager — minimal attack surface
FROM gcr.io/distroless/static-debian12:nonroot

# Copy timezone data and CA certs from builder (needed for TLS to Kafka)
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the statically linked binary
COPY --from=builder /batcher /batcher

# Run as non-root (distroless nonroot default UID is 65532)
USER nonroot:nonroot

ENTRYPOINT ["/batcher"]
