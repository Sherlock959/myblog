---
title: "ODS 到 ADS：一条实时链路的编排方式"
date: 2026-10-15T20:10:00+08:00
draft: false
author: "Rock Star"
tags: ["数仓分层", "DolphinScheduler", "调度", "实时数仓"]
categories: ["架构实践"]
description: "把 CDC 同步、建表、清洗、汇总拆成可重跑的节点：依赖 → 建表 → 同步，失败不阻断，幂等靠 truncate。"
summary: "一条能长期维护的同步链路，不是一个大任务，而是三个职责清晰的节点。分层、编排、幂等、校验，缺一块都会在三个月后变成负债。"
showToc: true
TocOpen: false
hidemeta: false
comments: false
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
---

## 分层的现实版本

教科书上的分层是 ODS → DWD → DWS → ADS。落到实际项目里，判断标准只有两个：**谁消费、谁负责口径**。

| 层 | 数据形态 | 谁消费 | 关键约束 |
| --- | --- | --- | --- |
| ODS | 与源库同构，含删除标记 | 只给 DWD 用 | 表名、字段名与源端一致 |
| DWD | 明细宽表，已清洗 | 分析、取数 | 一行一业务事件，可回溯 |
| DWS | 按主题/维度的轻度汇总 | 报表、看板 | 口径集中在这一层 |
| ADS | 面向具体报表的窄表 | 前端、BI | 可以直接查，不再二次加工 |

ODS 层最常见的问题是"顺手加工一下"：在同步阶段做去重、改字段名、补默认值。一旦加工，就再也分不清是源端问题还是链路问题。所以 ODS 只做一件事：**原样落库**。

## 一条链路的节点拆分

把一次同步拆成三个节点，是我目前最满意的结构：

```
DEPENDENT(表名)  →  SQL(建表)  →  DATAX(同步)
```

- **DEPENDENT**：前置依赖节点，节点名直接用纯表名，方便在调度图里一眼定位；
- **SQL**：`CREATE TABLE IF NOT EXISTS`，保证表存在，不负责数据；
- **DATAX**：负责数据搬运，写入前 `truncate`，保证幂等。

这种拆分的好处是：重跑只重跑坏掉的那一段。建表语句改了就只重跑 SQL 节点，数据源延迟就只重跑 DATAX 节点。

## 幂等：truncate 而不是 delete

全量同步任务最容易出的问题是"重跑一次，数据翻倍"。解法是在 writer 的 `preSql` 里做 truncation：

```json
"writer": {
  "name": "dorisexporter",
  "parameter": {
    "preSql": ["truncate table dws.province_report"],
    "postSql": []
  }
}
```

多省合表的场景要注意：**只 truncate 一次**。31 个省份分表合并到一张 Doris 合表时，如果每个省份节点都带 `truncate`，最后只剩一个省的数据。

正确做法是：

```
SQL(建表 + truncate)  →  DATAX(省A)  →  DATAX(省B)  →  ... →  DATAX(省Z)
```

第一个 DATAX 节点之前清空一次，后续所有节点直接追加。串行执行，避免并发写入同一张表造成版本冲突。

## 调度参数

离线同步任务通常固定在凌晨跑，参数这样给：

```json
"schedule": {
  "crontab": "0 35 3 * * ? *",
  "timezoneId": "Asia/Hong_Kong",
  "warningGroupId": 2,
  "workerGroup": "default",
  "environmentCode": -1,
  "failureStrategy": "CONTINUE"
}
```

`failureStrategy: CONTINUE` 是有意的：一个省的源数据延迟不应该阻断另外 30 个省。真正需要阻断的场景（比如建表失败）单独配置告警，而不是靠整条链路的失败扩散。

`timezoneId` 必须显式写。默认时区如果和业务时区差 8 小时，拉出来的分区永远是错的，而且错得很安静。

## 校验：三步走

同步完成后至少跑这三条：

```sql
-- 1. 总行数
SELECT COUNT(*) FROM dws.province_report WHERE stat_date = '${stat_date}';

-- 2. 主键/维度唯一性
SELECT province, COUNT(*) c FROM dws.province_report
WHERE stat_date = '${stat_date}' GROUP BY province HAVING c > 1;

-- 3. 关键指标汇总
SELECT SUM(order_cnt), SUM(amount) FROM dws.province_report
WHERE stat_date = '${stat_date}';
```

第 2 条最容易抓出问题。省分层出现重复行的经典原因，是关联的省级维度表里同一个省分有多行数据（维表没去重）。解法是在汇总时按省分编码 `GROUP BY`，用 `MAX` 或 `SUM` 收敛，而不是直接 JOIN。

## 小结

一条链路能不能长期维护，取决于三件事：

1. 分层是否守规矩（ODS 不加工）；
2. 是否幂等（truncate 位置对不对）；
3. 是否能局部重跑（节点职责是否单一）。

这三件事在项目前期看起来"没必要"，在项目后期就是救命的东西。
