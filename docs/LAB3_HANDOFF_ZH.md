# 给 Lab3 组员的交接说明

## 从哪里继续

从本项目的 Lab2 分支继续。正式输入仍为 `dataset/` 下六个课程文件，
正式集成结果仍是 `data/gold/integrated_taxi_trips` Delta 表。
运行命令见根目录 README。无需维护第二套天气时区或集成结果。

## 保持一致的三项约定

1. 存储时间统一 UTC；月、日期、星期和高峰小时使用纽约当地时间。
2. 天气 CSV 没有注明时区。小组已确认沿用 Lab1 的 UTC 解释，并在报告中
   披露它是未验证的来源假设。本次没有敏感性检查或第二套正式数据。
   只要没有新的来源证据，沿用此约定即可。
3. 分析范围是纽约当地 2024-01-01（含）至 2024-04-01（不含）。
   月度物理分区仍基于 UTC，因此当地月份末尾的夜间记录可能在下一个
   UTC 月分区。不要只读取同名 UTC 月就声称覆盖完整当地月份。

## Lab1 到 Lab2 的必要修复

- 空气质量先按纽约州编码 36 和 NYC 县编码筛选。不能只按县名连接，
  因为 California 也有 Kings 县。
- 多个 POC 先在站点小时内平均，再在 borough 小时内按独立站点等权平均。
  `aq_station_count` 现在是真实独立站点数。
- trip_id 改为对排序后的命名 JSON 字段计算 SHA-256，显式保留 NULL。
  ID 生成方式与旧 Lab1 不同，因此已经重建 Bronze/Gold。不要将旧表和
  新表的 trip_id 混合作为增量键；从这个版本生成的基线开始。
- 剔除结束时间早于开始时间的行程；当前有效行程共 9,417,864 条。
- 天气小表不分区；schema 版本为 2。所有摄取依然使用 overwrite，
  不应把它解释为已经实现增量写入。

## Lab2 新增的数据产品

四张表位于 `data/products/`：

- `daily_mobility_summary`：每日和 borough 的行程数，以及距离、时长、车费的总和/有效样本数。
- `taxi_zone_statistics`：每月、上车分区的统计。
- `weather_impact_summary`：各分区、天气代码的观测小时、需求及距离统计。
- `air_quality_impact_summary`：每 borough、每小时的需求和空气质量，带覆盖标记。

产品由正式集成表的指定 Delta 版本生成。注册信息位于
`data/lab2_metadata/product_registry`，包含 source_delta_version、创建/刷新时间、
schema、配置、行数和存储大小。重建产品会覆盖产品表，并追加一条注册记录。
benchmark 会拒绝来源版本或分析配置不一致的旧产品，要求先重建。

## 刷新或扩展时需要注意

- 当前产品是批量重建；如果 Lab3 要求增量处理，再针对它实现 append/merge
  和依赖刷新。这里没有预先替 Lab3 选择增量方案。
- 平均数需要保留分子和有效样本数，不能把每日均值直接平均成月均值。
- 天气需求按每个天气代码的观测小时归一化，含零行程的分区小时，
  每个代码至少 24 小时；比较至少两类天气的分区。
- 空气质量相关性每个 borough 小时只算一条观测，不按每趟行程重复加权。
  无空气质量的小时保持缺失，不能把 PM2.5 缺失值改成 0。
- 天气代码保留源文件的数字编号，不自行加入来源未说明的标签。

## 交接时快速核验

```sh
python scripts/check_environment.py
python scripts/validate_platform.py
python -m pytest tests -q
```

只修改报告不需要重建数据。更改摄取、主键、时间解释或地理逻辑后，应重建
相关 Delta 表并重跑验证；更改产品逻辑或分析范围后，重建产品再跑 benchmark。
