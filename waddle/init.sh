#!/bin/sh

ARCH=${ARCH:-x86_64}
CC=${CC:-cc}

WADDLE_SRC=https://codeberg.org/Demindiro/waddle/raw/commit/9af6172beaf98a8510b8290b0a69861939d83c74
ALPINE_SRC=https://dl-cdn.alpinelinux.org/alpine/v3.23/releases/$ARCH

set -xe

cd "`dirname "$0"`"

mkdir work
cd work

mkdir bin waddle alpine
curl "$WADDLE_SRC"/waddle/waddle.c -o waddle/waddle.c
curl "$WADDLE_SRC"/sys.h -O
curl "$WADDLE_SRC"/util.h -O
curl "$ALPINE_SRC"/alpine-minirootfs-3.23.3-$ARCH.tar.gz -O
sha256sum -c < ../check.sha256

"$CC" -Wall -Wextra -Os waddle/waddle.c -o bin/waddle

cd alpine
tar xvf ../alpine-minirootfs-3.23.3-$ARCH.tar.gz
cp /etc/resolv.conf etc
cp ../../_alpine-setup.sh root
cp ../../config.toml root
chmod 0500 root/_alpine-setup.sh

cd ..
exec env -i HOME=/root TERM="$TERM" ./bin/waddle \
	--base alpine \
	--net \
	--mount-proc \
	--bind /dev /dev \
	--cwd /root \
	-- ./_alpine-setup.sh
