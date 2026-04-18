FROM golang:1.24-bookworm AS builder
WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /out/bridge-only-app .

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/bridge-only-app /bridge-only-app
ENTRYPOINT ["/bridge-only-app"]
