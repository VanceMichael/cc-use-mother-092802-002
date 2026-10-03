# 假期跨方式客流校正与发布

归集铁路、公路、水路、民航四个来源的假期客流，统一统计口径与单位，
处理迟到修订、人工调整、勘误发布，并能按任一历史版本解释总量构成。

## 它解决什么问题

- 公路**营业性客运**与**非营业性小客车**口径不同：系统显式建模分项与合计，
  发布前逐日校验 `公路合计 = 营业性 + 小客车`（容差 0.05 万人次）。
- 各来源单位不一：万人次 / 人次 / 亿人次在入口统一归一化为**万人次**。
- 重复报送：同来源、同批次编号、同语义内容只记一次，绝不重复累计。
- 编号相同、内容不同：先**隔离**，不计入指标，由人工裁决接受或丢弃。
- 迟到修订：以「修订批次 + 修订原因」接替旧批次，旧批次留痕不删除。
- 某方式补报：只重算受影响的口径/方式/全国指标（输入指纹驱动）。
- 已对外发布：永不原地覆盖，通过**勘误**衔接新版本。
- 人工调整：必须由**另一名人员**复核后才生效，提交人/复核人/原因全留痕。
- 服务重启：事件日志回放 + 原子快照恢复，截止期提醒与待发布任务继续。
- 历史解释：任一版本都能回答「用了哪些批次、哪些待确认、为何与旧公报不同」。

## 架构

```
src/mobility/
  models.py       领域模型：来源、口径、批次、调整、待发布任务
  metrics.py      纯计算：单位归一化、包含关系、日均/总量/可比增幅、指纹
  errors.py       校验/包含关系/隔离/流程/未找到 五类领域异常
  eventstore.py   追加式事件日志 + 原子快照（fsync + rename）
  state.py        事件归并器：回放重建状态、快照编解码
  service.py      AggregationService：归集、裁决、复核、发布、勘误、提醒、恢复
```

所有状态变更都是一条追加事件；重启时先读快照，再回放其后事件，
因此任何历史版本与审计轨迹都可重建。

## 典型流程

```python
from src.mobility import AggregationService, Caliber, Mode

svc = AggregationService("./data/mid-autumn-2026")
svc.open_holiday("2026中秋", "2026-09-25", "2026-09-27")
svc.register_source("rail-src", "国家铁路局")
svc.register_source("road-src", "公路数据提供方")
svc.define_caliber(Caliber("rail", Mode.RAIL, "铁路客运量", "rail-src",
                           prior_year_total=5000, prior_year_days=3))
svc.define_caliber(Caliber("road-biz", Mode.ROAD, "公路营业性客运量", "road-src"))
svc.define_caliber(Caliber("road-car", Mode.ROAD, "非营业性小客车出行量", "road-src"))
svc.define_caliber(Caliber("road-total", Mode.ROAD, "公路人员流动量", "road-src",
                           kind="aggregate", children=("road-biz", "road-car")))

# 报送（万人次；也支持 unit="人次"/"亿人次"，自动归一化）
svc.submit_batch("R-01", "rail-src", "rail",
                 {"2026-09-25": 1800, "2026-09-26": 1700, "2026-09-27": 1900})
# 重复报送 → duplicate，不重复累计
svc.submit_batch("R-01", "rail-src", "rail",
                 {"2026-09-25": 1800, "2026-09-26": 1700, "2026-09-27": 1900})
# 迟到修订 → 旧批次 superseded，修订原因留痕
svc.submit_batch("R-01", "rail-src", "rail",
                 {"2026-09-25": 1810, "2026-09-26": 1700, "2026-09-27": 1905},
                 revision_of="R-01", revision_reason="终报口径修正")

# 人工调整：先提议，再由另一人复核
svc.propose_adjustment("ADJ-1", "caliber", 12.5, "站点漏报补录", "分析员甲",
                       caliber_id="rail", day="2026-09-27")
svc.review_adjustment("ADJ-1", "分析员乙", True, "凭证齐全")

# 发布；之后修改只能勘误
v1 = svc.publish("v1", "发布审核人员甲", "中秋假期出行快报")
v2 = svc.issue_corrigendum("v2", "发布审核人员乙", "铁路终报替换快报数")

# 任一历史版本的解释
svc.explain_version("v2")
# { batches_used: [...每批来源/口径/修订原因...],
#   pending: [...仍待确认的口径与缺失日期...],
#   diff_against: { metric_changes: [...], batch_changes: [...] } }

# 服务恢复后继续
svc2 = AggregationService("./data/mid-autumn-2026")
svc2.resume()   # open_tasks / quarantine / pending_adjustments / reminders_due
```

编号相同但内容不同的批次会抛 `QuarantineError`，批次进入隔离区，
用 `resolve_quarantine(qid, "accept"|"reject", 处理人, 说明)` 裁决。

## 数据目录

- `events.logl`：追加事件（每行一个 JSON，含单调 `seq`）。
- `snapshot.json`：原子快照，加速重启；缺失时从事件全量回放。

## 开发命令

运行测试：

```bash
python3 -m unittest discover -s tests -v
```

编译检查：

```bash
python3 -m compileall -q src tests
```

两条命令只读写仓库与临时目录，不连接外部业务系统。
领域规则详见 [`docs/domain-rules.md`](docs/domain-rules.md)。
