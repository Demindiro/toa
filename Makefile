build:
	cargo b --release --bin toa-cli --bin toa-fuse

fuzz:
	cargo fuzz run fuzz_target_1 -s none

fuzz-toa-blob:
	cargo fuzz run toa-blob-fuzz -s none -- -max_len=256

.PHONY: build fuzz
