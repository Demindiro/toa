build:
	./waddle/build.sh

fuzz:
	./waddle/fuzz.sh fuzz_target_1

fuzz-toa-blob:
	./waddle/fuzz.sh toa-blob-fuzz -- -max_len=256

fuzz-toa-blob-compress:
	./waddle/fuzz.sh toa-blob-compress-fuzz -- -max_len=256 -len_control=1000
