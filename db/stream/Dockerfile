# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install CA certs (for TLS connections, pgx will need them)
RUN apk add --no-cache ca-certificates

# Copy go.mod and go.sum first for caching
COPY go.mod ./
RUN go mod download

# Copy source
COPY . .

# Build static binary (no CGO, small)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /walstream main.go

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /walstream .

EXPOSE 9000

CMD ["./walstream"]
