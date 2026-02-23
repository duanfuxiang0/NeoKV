# NeoKV

NeoKV 是一个基于 **Braft + RocksDB** 的强一致分布式 KV 存储，并在其上提供 **Redis 协议兼容层（RESP）**，
可以直接使用 `redis-cli` 或 Redis SDK 访问。

更详细的设计说明见：
- `doc/neokv设计.md`
- `doc/Redis协议实现.md`

## 主要组件 / 二进制

- `neoMeta`：集群元数据管理（Region 分配/调度等）
- `neoStore`：数据节点（内部 brpc + 对外 Redis/RESP）
- `neo_redis_standalone`：单进程测试模式（嵌入 RocksDB + 单副本 Raft Region，无需 MetaServer，适合本地开发与 `tests/gocase`）

## 构建

```bash
mkdir -p build && cd build
cmake -DWITH_TESTS=ON ..
make -j"$(nproc)"
```

构建产物默认位于 `build/output/bin/`。

常用 CMake 选项：
- `-DWITH_TESTS=ON`：构建测试
- `-DDEBUG=ON -DWITH_DEBUG_SYMBOLS=ON`：调试构建
- `-DWITH_SYSTEM_LIBS=ON`：优先使用系统库（否则会自动拉取/构建依赖）

## 快速开始（单进程 Redis 服务器）

启动（默认 Redis 端口 16379）：

```bash
./build/output/bin/neo_redis_standalone --redis_port=16379 --data_dir=/tmp/neokv_standalone
```

验证：

```bash
redis-cli -p 16379 PING
redis-cli -p 16379 SET k v
redis-cli -p 16379 GET k
```

## 测试

### C++ 单测

> 需要先用 `-DWITH_TESTS=ON` 构建。

```bash
cd build
make test
```

或运行单个测试二进制（示例）：

```bash
./output/bin/test_redis_slot
```

### Redis 协议集成测试（Go，`tests/gocase/`）

```bash
cd tests/gocase && mkdir -p workspace
REPO_ROOT="$(cd ../.. && pwd)"
go test -count=1 ./unit/... \
  -args \
  -binPath="${REPO_ROOT}/build/output/bin/neo_redis_standalone" \
  -workspace="${REPO_ROOT}/tests/gocase/workspace"
```

如果 `go` 不在 `PATH`，可使用 toolchain 直接运行。

## 代码风格

```bash
./scripts/style.sh format
./scripts/style.sh check
./scripts/style.sh tidy --build-dir build
```

## 打包发布

```bash
./scripts/make_dist.sh --out dist
```

## License

Apache License 2.0，见 `LICENSE`。
