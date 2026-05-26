#!/bin/sh
export waddle_rustflags="-C target-feature=-crt-static"
exec `dirname $0`/run.sh /root/.cargo/bin/cargo fuzz run -s none "$@"
