#!/bin/bash -e

# Our development image sets up the PATH in .bashrc. Source that.
PS1='\$'
. ~/.bashrc

# Abort on unclean packaging area.
if [ -e target/docker-deployment/context ]; then
    cat >&2 <<EOF
Path target/docker-deployment/context already exists. Aborting.
If this was accidentally left over and you don't need anything from
it, you can remove it and try again.
EOF
    exit 1
fi
set -x

# Build all Ekiden binaries and resources.
cargo install --force --path tools
(cd contracts/token && cargo ekiden build-contract --release)
(cd compute && cargo build --release)
(cd node/dummy && cargo build --release)

# Package all binaries and resources.
mkdir -p target/docker-deployment/context/bin target/docker-deployment/context/lib target/docker-deployment/context/res
ln target/contract/token.so target/docker-deployment/context/lib
ln target/release/ekiden-compute target/docker-deployment/context/bin
ln target/release/ekiden-node-dummy target/docker-deployment/context/bin
ln target/release/ekiden-node-dummy-controller target/docker-deployment/context/bin
ln docker/deployment/Dockerfile.runtime target/docker-deployment/context/Dockerfile
tar cvzhf target/docker-deployment/context.tar.gz -C target/docker-deployment/context .
rm -rf target/docker-deployment/context
