---
title: "Hive 到 Doris 的 DataX 同步：分区列那个坑"
date: 2026-09-20T14:05:00+08:00
draft: false
author: "Rock Star"
tags: ["DataX", "Hive", "Doris", "离线同步"]
categories: ["离线同步"]
description: "Hive 分区表同步到 Doris 时，preColumns 少写一列就全表判脏数据，报 Framework-14。原因和正确写法都在这里。"
summary: "reader 的 preColumns 是 ORC 文件里的列，分区列不在文件里而在路径里。把分区列写进 preColumns 会导致列数不匹配，全部行被判脏数据。"
showToc: true
TocOpen: false
hidemeta: false
comments: false
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
---

## 现象

一个再普通不过的 Hive → Doris 同步任务，跑起来 30 秒就退出，日志里是这样的：

```
Framework-14: 您配置的preColumns与hive表的列数量不匹配,
共 12 个字段, 但preColumns配置了 13 个字段
```

同步的数据量是 0，任务状态却是"成功"——因为 DataX 把每一行都判定成了脏数据，而脏数据默认不阻断任务。

## 原因

Hive 表是这样的结构：

```sql
CREATE TABLE dws.order_summary (
  order_id     BIGINT,
  province     STRING,
  city         STRING,
  amount       DECIMAL(18,2),
  order_cnt    BIGINT
)
PARTITIONED BY (sum_month STRING)
STORED AS ORC;
```

物理文件里只有 5 个字段，`sum_month` 不在文件里，它体现在路径上：`/dws/order_summary/sum_month=202609/part-00000.orc`。

所以 DataX 的 reader 和 writer 列数是不一样的：

- `preColumns`：ORC 文件真实包含的列，**不含分区列**；
- `postColumns`：Doris 表的完整列，**含分区列**；
- `partitionColumns`：告诉 DataX 从路径里把 `sum_month` 补回来。

正确的配置长这样：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "path": "/dws/order_summary/sum_month=${partitionkey_day_400_cc}",
            "defaultFS": "hdfs://nameservice1",
            "fileType": "orc",
            "column": [
              {"index": 0, "type": "long",     "value": "order_id"},
              {"index": 1, "type": "string",   "value": "province"},
              {"index": 2, "type": "string",   "value": "city"},
              {"index": 3, "type": "string",   "value": "amount"},
              {"index": 4, "type": "long",     "value": "order_cnt"}
            ],
            "fieldDelimiter": "\u0001"
          }
        },
        "writer": {
          "name": "dorisexporter",
          "parameter": {
            "fenodes": "10.0.0.21:8030",
            "database": "dws",
            "table": "order_summary",
            "username": "root",
            "password": "******",
            "column": [
              "order_id", "province", "city", "amount", "order_cnt", "sum_month"
            ],
            "preSql": ["truncate table order_summary"],
            "postSql": []
          }
        }
      }
    ]
  }
}
```

关键点只有一句：**`preColumns` 比 `postColumns` 少一个分区列，分区列由 `partitionColumns` 从路径自动补。**

补全之后应该是：

```json
"reader": {
  "parameter": {
    "partitionColumns": ["sum_month"]
  }
}
```

## 另一类问题：分区变量

调度里习惯把日期做成变量，例如 `${partitionkey_day_400_cc}`。这里踩过两个坑：

1. 变量没替换，路径变成字面量 `/sum_month=${partitionkey_day_400_cc}`，HDFS 直接报路径不存在；
2. 变量替换成了带引号的值（`"202609"`），路径里多了一对双引号。

排查方法：把 DataX 的 `-Dloglevel=debug` 打开，看它实际拼出来的 path 是什么，比猜快得多。

## 数据校验

同步"成功"不代表数据对，至少要核对三件事：

```sql
-- 1. 行数对齐
SELECT COUNT(*) FROM dws.order_summary WHERE sum_month = '202609';

-- 2. 主键唯一（Doris 侧）
SELECT order_id, COUNT(*) c
FROM dws.order_summary
WHERE sum_month = '202609'
GROUP BY order_id
HAVING c > 1
LIMIT 10;

-- 3. 金额汇总对齐（最容易发现截断/精度问题）
SELECT SUM(amount) FROM dws.order_summary WHERE sum_month = '202609';
```

金额汇总比对是我最推荐的校验方式：一旦 VARCHAR 截断、DECIMAL 精度丢失、脏数据过滤，汇总值一定对不上，比逐行比对便宜得多。

## 小结

DataX 的错配基本都是"列"的问题：reader 列、writer 列、分区列三者的关系没理顺。记住一条就够了——**ORC 文件里有什么，preColumns 就写什么**。
