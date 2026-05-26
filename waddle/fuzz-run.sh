#!/bin/sh
exec `dirname $0`/run.sh /root/.cargo/bin/cargo fuzz run -s none "$@"
