# All versions below should be configurable and stored somewhere beside the
# release.
ARG UBUNTU_VERSION=ubuntu@sha256:b58746c8a89938b8c9f5b77de3b8cf1fe78210c696ab03a1442e235eea65d84f
FROM ${UBUNTU_VERSION}

ARG GO_VERSION=1.14.3
ARG GO_PROTOC_GEN_GO_VERSION=1.21.0
ARG GO_PROTOC_VERSION=3.6.1
ARG GOLANGCILINT_VERSION=1.23.6
ARG GOCOVMERGE_VERSION=b5bfa59ec0adc420475f97f89b58045c721d761c
ARG RUST_NIGHTLY_VERSION=2020-05-31
ARG LIBSSL_DEV_VERSION=1.1.1-1ubuntu2.1~18.04.6
ARG LIBCURL4_OPENSSL_DEV_VERSION=7.58.0-2ubuntu3.8
ARG LIBSECCOMP_DEV_VERSION=2.4.1-0ubuntu0.18.04.2
ARG FORTANIX_SGX_TOOLS_VERSION=0.4.0
ARG SGXS_TOOLS_VERSION=0.8.2

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && apt-get install -qq \
    build-essential git curl unzip \
    # for rust itself
    pkg-config \
    # for rust grpcio dependency during compilation
    cmake \
    # for rust fortanix tools
    libssl-dev=${LIBSSL_DEV_VERSION} libcurl4-openssl-dev=${LIBCURL4_OPENSSL_DEV_VERSION} \
    # for seccomp Go bindings support
    libseccomp-dev=${LIBSECCOMP_DEV_VERSION}

ENV HOME="/root"
ENV GOPATH="/go"
ENV PATH="${HOME}/.cargo/bin:/go/bin:/usr/local/go/bin:${PATH}"

# Install protobuf (apt system v3.0 fails to compile our protos).
RUN curl -OJL https://github.com/google/protobuf/releases/download/v${GO_PROTOC_VERSION}/protoc-${GO_PROTOC_VERSION}-linux-x86_64.zip && \
    unzip protoc-${GO_PROTOC_VERSION}-linux-x86_64.zip -x readme.txt -d /usr && \
    rm protoc-${GO_PROTOC_VERSION}-linux-x86_64.zip && \
    chmod a+rx /usr/bin/protoc

# Install Rust.
RUN curl "https://sh.rustup.rs" -JLsfo rustup.sh && \
    sh rustup.sh -y --default-toolchain nightly-${RUST_NIGHTLY_VERSION} && \
    rustup target add x86_64-fortanix-unknown-sgx && \
    cargo install --version ${FORTANIX_SGX_TOOLS_VERSION} fortanix-sgx-tools && \
    cargo install --version ${SGXS_TOOLS_VERSION} sgxs-tools

# Install Go and utilities.
RUN curl -OJL https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_VERSION}.linux-amd64.tar.gz && \
    mkdir -p /go/bin && \
    # Install a specific version of protoc-gen-go.
    GO111MODULE=on go get google.golang.org/protobuf/cmd/protoc-gen-go@v${GO_PROTOC_GEN_GO_VERSION}

