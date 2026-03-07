build:
	cargo b --release --bin toa-cli --bin toa-fuse

fuzz:
	# TODO -max_len is inefficient. Consider "repeat" of sorts.
	cargo fuzz run fuzz_target_1 -s none

.PHONY: build fuzz
