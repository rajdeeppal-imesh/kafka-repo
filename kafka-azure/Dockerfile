FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY *.go ./
RUN apt-get update && apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross ca-certificates && rm -rf /var/lib/apt/lists/*
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=1 GOFLAGS='-mod=vendor' GOOS=$TARGETOS GOARCH=$TARGETARCH CC=x86_64-linux-gnu-gcc go build -o /out/kafka-app .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates librdkafka1 && rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=builder /out/kafka-app /kafka-app
USER 65532:65532
ENTRYPOINT ["/kafka-app"]
