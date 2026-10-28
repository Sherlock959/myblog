---
title: "Flink CDC 同步达梦到 Doris：从归档日志到分桶主键"
date: 2026-09-12T09:20:00+08:00
draft: false
author: "Rock Star"
tags: ["Flink CDC", "达梦", "Doris", "数据同步"]
categories: ["实时同步"]
description: "用 dameng-cdc 把达梦的增量数据实时写入 Doris：源端日志配置、Flink SQL 双表定义、Doris 主键模型与并行度调优。"
summary: "达梦库做 CDC 的关键不在 Flink 侧，而在源端的归档日志与补充日志；Doris 侧的关键不在建表语法，而在主键模型与字段长度。这篇把两端的配置和踩过的坑一次写全。"
showToc: true
TocOpen: false
hidemeta: false
comments: false
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
---

## 场景与约束

需求很直接：业务库是达梦 DM8，分析侧是 Apache Doris，要求分钟级延迟、支持物理删除同步、不侵入源库业务逻辑。

结论是先走 **Flink CDC + dameng-cdc 连接器**。整条链路里真正让人卡住的，是下面三件事：

1. 达梦没有打开归档日志和补充日志，CDC 直接读不到变更；
2. Doris 表字段长度按源端原样定义，中文被截断；
3. 作业重跑时 `label-prefix` 冲突，Stream Load 报事务重复。

## 源端准备

达梦需要先确认两件事，缺一不可。

```sql
-- 1. 确认归档已开启（MOUNT 状态下执行）
ALTER DATABASE MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- 2. 补充日志：必须为 2（全部列），否则 UPDATE 只带主键
SELECT PARA_NAME, PARA_VALUE
FROM V$DM_INI
WHERE PARA_NAME = 'RLOG_APPEND_LOGIC';
```

`RLOG_APPEND_LOGIC` 不为 2 时，Flink 拿到的变更记录里只有主键和变更列，宽表下游会出现大量 NULL。这个参数需要重启实例生效。

另外给同步账号最小权限即可：

```sql
CREATE USER "CDC_USER" IDENTIFIED BY "******";
GRANT SELECT ON "订单库".* TO "CDC_USER";
GRANT SELECT ON V$DM_INI TO "CDC_USER";
```

## Flink SQL：源表与目标表

源表和目标表的字段定义必须**完全一致**，包括顺序和类型。这是最容易出错的一条。

```sql
-- 源端：达梦
CREATE TABLE dm_order_src (
  order_id      BIGINT,
  order_no      VARCHAR(64),
  cust_name     VARCHAR(128),
  amount        DECIMAL(18, 2),
  status        INT,
  create_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'dameng-cdc',
  'url' = 'jdbc:dm://10.0.0.11:5236',
  'username' = 'CDC_USER',
  'password' = '******',
  'table-name' = '"订单库"."T_ORDER"',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

-- 目标端：Doris
CREATE TABLE doris_order_sink (
  order_id      BIGINT,
  order_no      VARCHAR(64),
  cust_name     VARCHAR(128),
  amount        DECIMAL(18, 2),
  status        INT,
  create_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes' = '10.0.0.21:8030',
  'table.identifier' = 'ods.t_order',
  'username' = 'root',
  'password' = '******',
  'sink.label-prefix' = 'dm_order_20260912_v1',
  'sink.enable-delete' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true'
);
```

两个参数值得单独说：

- `scan.incremental.snapshot.chunk.size = 8096`：千万级表用这个分片大小，全量阶段并行度比较均衡，再大容易把单个 chunk 拉长成瓶颈。
- `sink.label-prefix`：**全局唯一**，重跑必须换后缀。作业失败后直接重启原 label，Doris 会判定重复导入。

DDL 只有一句：

```sql
INSERT INTO doris_order_sink
SELECT * FROM dm_order_src;
```

## Doris 侧建表

```sql
CREATE TABLE ods.t_order (
  order_id      BIGINT       NOT NULL COMMENT '订单ID',
  order_no      VARCHAR(192) NOT NULL COMMENT '订单号',
  cust_name     VARCHAR(384) NULL     COMMENT '客户名称',
  amount        DECIMAL(18, 2) NULL   COMMENT '金额',
  status        INT          NULL     COMMENT '状态',
  create_time   DATETIME     NULL     COMMENT '创建时间',
  update_time   DATETIME     NULL     COMMENT '更新时间'
) ENGINE = OLAP
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 16
PROPERTIES (
  'enable_unique_key_merge_on_write' = 'true',
  'replication_num' = '3'
);
```

几条硬规则：

| 规则 | 原因 |
| --- | --- |
| VARCHAR 长度按源端 **3 倍** 定义 | Doris 按字节计长，一个中文字符占 3 字节 |
| 主键模型 + `merge_on_write` | 支持 Upsert 与 Delete，查询侧只读最新版本 |
| `DECIMAL` 而不是 `DOUBLE` | 金额类字段避免精度漂移 |
| 时间字段用 `DATETIME` | 达梦 `DATETIMEV2` → Flink `TIMESTAMP` → Doris `DATETIME` |

## 踩坑清单

1. **报错 `can't find any matched data scanning range`**：源端没有归档日志，或者连接用户没有 `V$DM_INI` 查询权限，CDC 无法定位日志起点。
2. **更新后字段变成 NULL**：`RLOG_APPEND_LOGIC` 不是 2，只有变更列被写入。
3. **中文被截断成乱码**：Doris VARCHAR 长度按字节算，`VARCHAR(128)` 实际只能存 42 个中文。
4. **重跑作业报事务重复**：`label-prefix` 没换后缀，这是 Stream Load 幂等机制在起作用，不是 bug。
5. **删除同步不生效**：忘了 `sink.enable-delete = 'true'`，Doris 默认忽略 DELETE 事件。

## 小结

达梦 CDC 的配置其实不难，难的是把源端日志、Flink 表结构、Doris 表结构这三份定义"对齐"。我的习惯是先在源库 `SELECT *` 拿一份字段和长度的快照，直接按它生成 Doris DDL，能省掉 80% 的类型问题。
