#!/bin/sh

set -xe

cd
apk add gcc g++ curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
mv config.toml .cargo
