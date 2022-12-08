# Pravega C client.

This project provides a way to interact with [Pravega](http://pravega.io) using C client.

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for
the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream
with strict ordering and consistency.

## Build
```
# build dynamic library & generate header
cd cc
cargo build --release
```

## How to use pravega c client

1. build rust dynamic library
cd golang
cargo build --release
cd .. # cd to rust project root directory
mv ./target/release/libpravega_client_c.so /usr/lib
