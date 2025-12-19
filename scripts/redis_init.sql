-- Redis system table initialization for BaikalDB
-- 
-- NOTE: 这个 SQL 文件现在是 **可选的**！
-- 
-- BaikalDB Redis 模块已经使用硬编码的 REDIS_INDEX_ID，不再需要创建 SQL 表。
-- SplitCompactionFilter 会在启动时自动注册 Redis 索引信息。
--
-- 如果你仍然想创建这个表（例如为了兼容性或未来扩展），可以执行以下 SQL：

-- Create the __redis__ database if not exists
CREATE DATABASE IF NOT EXISTS `__redis__`;

-- Create the kv table for Redis String type
-- This table structure is used internally by BaikalDB's Redis module
-- The actual data is stored in RocksDB with custom encoding, not in regular SQL rows
CREATE TABLE IF NOT EXISTS `__redis__`.`kv` (
    `slot` SMALLINT UNSIGNED NOT NULL COMMENT 'Redis hash slot (0-16383)',
    `key` VARBINARY(1024) NOT NULL COMMENT 'Redis key',
    `value` BLOB COMMENT 'Redis value',
    `expire_at` BIGINT DEFAULT 0 COMMENT 'Expiration timestamp in milliseconds',
    PRIMARY KEY (`slot`, `key`)
) ENGINE=Rocksdb DEFAULT CHARSET=binary
COMMENT='Redis String type storage - internal system table'
PARTITION BY RANGE (`slot`) (
    PARTITION p0 VALUES LESS THAN (4096),
    PARTITION p1 VALUES LESS THAN (8192),
    PARTITION p2 VALUES LESS THAN (12288),
    PARTITION p3 VALUES LESS THAN (16384)
);

-- Note: The partition scheme can be adjusted based on cluster size
-- Each partition will map to one or more BaikalDB regions
