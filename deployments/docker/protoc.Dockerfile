# syntax=docker/dockerfile:1
# ============================================================================
# Protobuf toolchain image — used by scripts/proto-gen.sh
#
# Exists so nobody has to install protoc on their machine (this team is on
# Windows, where that is the fiddliest part of the setup). The versions below
# are pinned to what the current generated code was built with — bump them
# deliberately, then regenerate everything in one commit.
#
#   docker build -f deployments/docker/protoc.Dockerfile -t algo-trading/protoc .
# ============================================================================

ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine

ARG PROTOC_VERSION=33.0
ARG PROTOC_GEN_GO_VERSION=v1.36.10
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.5.1

RUN apk add --no-cache curl unzip

# protoc itself reports this as "v6.33.0" in the generated file headers.
RUN curl -fsSL -o /tmp/protoc.zip \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" \
 && unzip -q /tmp/protoc.zip -d /usr/local \
 && rm /tmp/protoc.zip \
 && chmod +x /usr/local/bin/protoc

RUN go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}" \
 && go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"

ENV PATH="/go/bin:${PATH}"

WORKDIR /work
ENTRYPOINT ["protoc"]
