# NeoKV

> **⚠️ Disclaimer — Learning Project Only**
>
> This project is built primarily for learning and practicing **Multi Braft** concepts. It is **NOT intended for production use**.
>
> Much of the underlying code is derived from studying the following excellent open-source projects: BaikalDB,Kvrocks,Pika; Any code originating from these projects remains subject to their respective original open-source licenses.

NeoKV is a strongly consistent distributed KV store built on **Braft + RocksDB**, with a **Redis-compatible protocol layer (RESP)** on top. You can connect directly using `redis-cli` or any Redis SDK.

For more detailed design documentation, see:
- `doc/`

## Components

- `neoMeta` — Cluster metadata management (region allocation, scheduling, etc.)
- `neoStore` — Data node (internal brpc + external Redis/RESP interface)

## Building

```bash
mkdir -p build && cd build
cmake -DWITH_TESTS=ON ..
make -j"$(nproc)"
```

Build artifacts are placed in `build/output/bin/` by default.

Common CMake options:
- `-DWITH_TESTS=ON` — Build tests
- `-DDEBUG=ON -DWITH_DEBUG_SYMBOLS=ON` — Debug build
- `-DWITH_SYSTEM_LIBS=ON` — Prefer system libraries (otherwise dependencies are fetched and built automatically)

## Quick Start (Standalone Redis Server)

Start the server (default Redis port 16379):

```bash
./build/output/bin/neo_redis_standalone --redis_port=16379 --data_dir=/tmp/neokv_standalone
```

Verify:

```bash
redis-cli -p 16379 PING
redis-cli -p 16379 SET k v
redis-cli -p 16379 GET k
```

## Testing

### C++ Unit Tests

> Requires building with `-DWITH_TESTS=ON`.

```bash
cd build
make test
```

Or run an individual test binary:

```bash
./output/bin/test_redis_slot
```

### Redis Protocol Integration Tests (Go, `tests/gocase/`)

```bash
cd tests/gocase && mkdir -p workspace
REPO_ROOT="$(cd ../.. && pwd)"
go test -count=1 ./unit/... \
  -args \
  -binPath="${REPO_ROOT}/build/output/bin/neo_redis_standalone" \
  -workspace="${REPO_ROOT}/tests/gocase/workspace"
```

If `go` is not in your `PATH`, you can invoke the toolchain binary directly.

## Code Style

```bash
./scripts/style.sh format
./scripts/style.sh check
./scripts/style.sh tidy --build-dir build
```

## Packaging

```bash
./scripts/make_dist.sh --out dist
```

## License

Apache License 2.0 — see `LICENSE`.
