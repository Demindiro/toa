#!/bin/sh

set -xe

cd "`dirname "$0"`"

test -d work || ./init.sh

mkdir -p work/target

exec env -i HOME=/root TERM="$TERM" PATH=/bin:/usr/bin:/root/.cargo/bin \
	RUSTFLAGS="$waddle_rustflags" \
	./work/bin/waddle \
	--base work/alpine \
	--net \
	--mount-proc \
	--bind /dev /dev \
	--bind /rust .. \
	--bind /target work/target \
	--cwd /rust \
	-- "$@"
