# Appender

Appender is an append-only object-/filesystem for immutable storage.

## Reasoning

TODO (the previous one was bad and rambly).

## Format

Currently [sled](https://github.com/spacejam/sled) is used as backing store.
It will eventually be replaced with a custom store with GC,
compression and encryption support.
