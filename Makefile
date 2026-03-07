build:
	cargo b --release --bin toa-cli --bin toa-fuse

fuzz:
	cargo fuzz run fuzz_target_1 -s none

.PHONY: build fuzz
