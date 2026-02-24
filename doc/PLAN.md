# NeoKV 文档编写计划

## 文档定位

NeoKV 是一个面向教学的项目，核心目标是帮助读者理解 **Multi-Raft 分布式架构** 如何与 **RocksDB 存储引擎** 结合，构建一个支持 Redis 协议的强一致分布式 KV 存储。

文档体系采用渐进式结构，从分布式共识到存储引擎，再到上层数据结构和协议实现，层层递进。

## 文风规范

参考 [mini-lsm](https://github.com/skyzh/mini-lsm) 的教学风格：

- **语言**：中文为主，技术术语保留英文（Raft、RocksDB、Region、Slot、Column Family 等）
- **人称**：使用"我们"营造协作感（"在这一章中，我们将深入分析..."）
- **结构**：每篇开头有"本章概览"列出学习目标，末尾有"检验你的理解"思考题
- **动机先行**：先讲 Why（为什么需要这个设计），再讲 How（具体如何实现）
- **代码导读**：关键代码片段内嵌讲解，标注文件路径，不大段堆砌
- **对比分析**：适当对比其他系统（Redis Cluster、Kvrocks、TiKV）帮助理解设计取舍
- **图表辅助**：用 ASCII 图和表格说明编码格式、数据流、架构关系

## 目录结构

```
doc/
├── PLAN.md                              # 本文件：文档编写计划
├── 00-概述.md                            # 项目介绍、架构总览、学习路线
│
├── part1-MultiRaft分布式架构/
│   ├── 01-Raft共识算法.md                 # Raft 基础、为什么选择 Raft、从单 Raft 到 Multi-Raft
│   ├── 02-braft实践.md                   # braft 框架、直接使用的挑战、NeoKV 的定制方案
│   ├── 03-Multi-Raft与Region.md          # Region 状态机、读写路径、Store 管理多 Region
│   └── 04-MetaServer与集群管理.md         # MetaServer HA、Region 调度、Split/Merge、心跳
│
├── part2-存储引擎/
│   ├── 05-RocksDB基础与特性.md            # LSM-Tree、Column Family、Compaction、Bloom Filter
│   ├── 06-存储架构设计.md                 # RocksWrapper、8 个 CF 设计、Key 编码、Compaction Filter
│   └── 07-Redis存储编码.md               # Metadata CF、Subkey CF、Version-based Lazy Deletion
│
├── part3-Redis数据结构/
│   ├── 08-String类型.md                  # 内联编码、数值操作、迁移策略
│   ├── 09-Hash类型.md                    # 子键编码、field 操作、HSCAN
│   ├── 10-Set类型.md                     # 成员编码、集合运算（SINTER/SUNION/SDIFF）
│   ├── 11-List类型.md                    # 双指针设计、O(1) push/pop、LINSERT/LREM
│   └── 12-ZSet类型.md                    # 双 CF 索引、Score 编码、范围查询
│
├── part4-Redis协议层/
│   ├── 13-RESP协议与命令框架.md           # brpc::RedisService、CommandHandler 模式
│   ├── 14-路由与集群.md                   # CRC16 Slot、SlotTable、CROSSSLOT、MOVED
│   └── 15-过期与TTL.md                   # 被动过期、主动清理、TTL Cleaner
│
├── part5-测试与运维/
│   └── 16-测试体系.md                    # C++ 单元测试、Go 集成测试、Standalone 模式
│
└── appendix-命令参考.md                   # 98 个已实现命令的完整列表
```

## 推荐阅读顺序

```
00-概述 ──→ Part 1 (01→02→03→04) ──→ Part 2 (05→06→07) ──→ Part 3 (08→12) ──→ Part 4 (13→15) ──→ Part 5
  │              │                         │                      │                   │
  │         分布式架构                   存储引擎              数据结构实现          协议与路由
  │         理解 Raft                  理解 RocksDB           理解编码设计          理解命令处理
  └─ 快速了解全貌
```

- **想了解分布式架构**：00 → Part 1 全部
- **想了解存储设计**：00 → Part 2 全部（建议先读 Part 1 的 03）
- **想了解 Redis 实现**：00 → 07 → Part 3 任意章节 → Part 4
- **想贡献代码**：全部阅读，重点关注 Part 3 和 Part 4

## 各篇状态

| 编号 | 标题 | 状态 |
|------|------|------|
| 00 | 概述 | ✅ 已完成 |
| 01 | Raft 共识算法 | ✅ 已完成 |
| 02 | braft 实践 | ✅ 已完成 |
| 03 | Multi-Raft 与 Region | ✅ 已完成 |
| 04 | MetaServer 与集群管理 | ✅ 已完成 |
| 05 | RocksDB 基础与特性 | ✅ 已完成 |
| 06 | 存储架构设计 | ✅ 已完成 |
| 07 | Redis 存储编码 | ✅ 已完成 |
| 08 | String 类型 | ✅ 已完成 |
| 09 | Hash 类型 | ✅ 已完成 |
| 10 | Set 类型 | ✅ 已完成 |
| 11 | List 类型 | ✅ 已完成 |
| 12 | ZSet 类型 | ✅ 已完成 |
| 13 | RESP 协议与命令框架 | ✅ 已完成 |
| 14 | 路由与集群 | ✅ 已完成 |
| 15 | 过期与 TTL | ✅ 已完成 |
| 16 | 测试体系 | ✅ 已完成 |
| A | 命令参考 | ✅ 已完成 |
