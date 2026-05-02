#!/bin/sh
exec `dirname $0`/run.sh /bin/sh -s <<EOF
rustup target add wasm32-unknown-unknown
RUSTFLAGS=-Clink-arg=-zstack-size=131072 cargo b -p toa-hash-wasm --target wasm32-unknown-unknown --profile web-release
EOF
