---
title: "Doris 建表：主键模型还是明细模型？"
date: 2026-10-08T10:30:00+08:00
draft: false
author: "Rock Star"
tags: ["Doris", "数据建模", "数仓"]
categories: ["数仓建模"]
description: "同一批同步任务里，有的表用 UNIQUE KEY，有的只能用 DUPLICATE KEY。判断标准只有一条：这张表有没有业务主键。"
summary: "有 id 就用主键模型并打开 merge-on-write；没有 id 就用明细模型，只取第一个字段做 key。附字段类型、分桶、副本的常用取值。"
showToc: true
TocOpen: false
hidemeta: false
comments: false
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
---

## 判断标准

同步 Hive 表到 Doris 时，建表模型只看一个问题：**这张表有没有业务主键（通常是 `id`）？**

| 情况 | 模型 | Key |
| --- | --- | --- |
| 有 `id` 字段 | 主键模型 `UNIQUE KEY(id)` | `id` |
| 无 `id` 字段 | 明细模型 `DUPLICATE KEY(第一列)` | 仅第一个字段，不是前四列 |
| 需要保留全量变更历史 | 明细模型 | 第一列 |

明细模型的 key 只需要一列，多写几列只会让排序键变宽，不会带来查询收益。

## 主键模型

```sql
CREATE TABLE ods.t_customer (
  id            BIGINT       NOT NULL COMMENT '主键',
  cust_code     VARCHAR(96)  NOT NULL COMMENT '客户编码',
  cust_name     VARCHAR(384) NULL     COMMENT '客户名称',
  org_id        BIGINT       NULL     COMMENT '所属机构',
  create_time   DATETIME     NULL     COMMENT '创建时间'
) ENGINE = OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
  'enable_unique_key_merge_on_write' = 'true',
  'replication_num' = '3'
);
```

`enable_unique_key_merge_on_write = 'true'` 是必选项。不开 MOW，查询需要合并多版本，读放大会非常明显；开了之后写入时完成合并，查询侧只读最新版本。

主键模型的代价是写入放大，适合维表、状态表、需要 Upsert 的 ODS 层。

## 明细模型

```sql
CREATE TABLE dws.province_report (
  stat_date     DATE         NOT NULL COMMENT '统计日期',
  province      VARCHAR(32)  NOT NULL COMMENT '省份编码',
  province_name VARCHAR(96)  NULL     COMMENT '省份名称',
  city          VARCHAR(64)  NULL     COMMENT '地市',
  order_cnt     BIGINT       NULL     COMMENT '订单量',
  amount        DECIMAL(18, 2) NULL   COMMENT '金额'
) ENGINE = OLAP
DUPLICATE KEY(stat_date)
DISTRIBUTED BY HASH(province) BUCKETS 12
PROPERTIES (
  'replication_num' = '3'
);
```

这张报表没有唯一主键，同一天同省份可能有多条口径不同的记录，强行建主键模型会导致数据被覆盖。明细模型保留所有行，聚合交给查询侧或物化视图。

## 字段类型的三个约定

1. **VARCHAR 按源端 3 倍长度**：Doris 的 VARCHAR 按字节计，中文占 3 字节。`VARCHAR(128)` 实际只能存 42 个汉字。
2. **数值统一 DECIMAL**：金额、比例一律 `DECIMAL`，不用 `DOUBLE`。`DOUBLE` 在跨表汇总时会飘。
3. **Datetime 用 DATETIME**：Flink 的 `TIMESTAMP(3)` 落到 Doris 就是 `DATETIME`，不需要额外转换。

Hive 侧如果源字段是 `string`，Doris 侧映射成 `VARCHAR(65533)`；如果源端有明确的 `varchar(n)`，按 3 倍长度算并封顶 65533。

## 分桶与副本

- 分桶数：单表数据量 1 亿以内，`BUCKETS` 取 8~16 足够；分桶过多会让小文件问题变严重。
- 副本数：生产环境 `replication_num = 3`；单机测试环境必须显式写 `1`，否则建表直接失败。

## 表名大小写

这一条不是 Doris 的限制，是团队踩出来的规矩：**Doris 表名必须和源端表名完全一致**，禁止统一转大写或小写。

原因是同步任务、数据模型、调度流程里表名会出现在多个位置（`targetTable`、`dtModelName`、`dsModelName`、流程定义名、工作流名）。只要有一处大小写不同，就会在排查时找不到对应表，尤其是在 Linux 上的 Hive 与 Doris 混用环境里。

## 小结

选型的判断成本其实很低：

- 有主键 → `UNIQUE KEY(id)` + MOW，接受写入放大，换取 Upsert 能力；
- 无主键 → `DUPLICATE KEY(第一列)`，接受重复行，换取全量保留；
- 字段长度按 3 倍，金额用 DECIMAL，副本按环境给。

把这三条写进团队的建表规范，比事后修数据省事得多。
