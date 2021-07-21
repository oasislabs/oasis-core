#!/bin/bash

cd /root
git clone https://github.com/oasisprotocol/oasis-core.git
cd oasis-core
make
CARGO_TARGET_DIR=target/default cargo build -p oasis-core-runtime-loader --release

# Reproducible tarballing:
SOURCE_DATE_EPOCH=`git log -1 --format=%ai $GIT_TAG`
tar --sort=name \
      --mtime="${SOURCE_DATE_EPOCH}" \
      --owner=0 --group=0 --numeric-owner \
      --pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime \
      -cvf oasis_core_reproducible_${GIT_TAG}_linux_amd64.tar.gz \
      go/oasis-node/oasis-node \
      go/oasis-net-runner/oasis-net-runner \
      target/default/release/oasis-core-runtime-loader \
      target/sgx/x86_64-fortanix-unknown-sgx/debug/simple-keyvalue.sgxs

