# 网约车订单事件流计算服务

面向网约车网关乱序、重复、迟到事件的事件时间流计算引擎,纯 TypeScript / Node.js 实现,
零运行时依赖。包含确定性事件生成器、流处理拓扑、WAL+快照恢复、倍速回放与五页看板。

## 需求映射

| # | 需求 | 实现 |
|---|------|------|
| 1 | 滚动/滑动/会话窗口 | `src/engine/window.ts`:Tumble / Sliding / Session 三类 Assigner + 通用 WindowOperator |
| 2 | 按城市独立推进水位线,卡住不拖累其他城市 | `src/engine/watermark.ts`:每城市 maxEventTime,事件时间口径空闲检测,全局取活跃城市 min |
| 3 | 迟到事件修正已发结果并留痕 | 窗口在 `allowedLateness` 内合并迟到事件并按 version 递增重发;超期丢弃。全部进 `LateRecord` |
| 4 | 重复投递只算一次、下游精确一次 | 入口 `DedupOperator` 按 eventId 去重;结果带确定性 `resultId`,`ResultStore` 幂等 upsert |
| 5 | 重启从快照恢复且结果不变 | 批次边界对齐检查点(原子 rename 落盘)+ WAL 重放;`test/e2e.test.ts` 崩溃恢复逐行一致 |
| 6 | 历史事件倍速回放结果一致 | 全程纯事件时间、种子化 PRNG,墙钟不参与计算口径;回放后逐 resultId 对比 |
| 7 | 司机上下线拼会话、跨天切开 | `src/engine/driver-session.ts`:乱序 OFFLINE 缓存、东八区日边界切分、时长摊分钟窗 |
| — | 5 万/秒、p99<2s | `npm run bench` 三段门禁:吞吐、延迟、恢复一致性 |

## 架构

```
EventGenerator ──► WAL(预写) ──► Topology
                                   │
   DedupOperator(eventId 去重)     │
        │                          │
   WatermarkAssigner(分城市) ── watermark ──┐
        │                                   │
        ├─► Tumble 1min   (接单率)          │
        ├─► Sliding 1min/10s (接单率)       ├─ processWatermark 触发/修正
        ├─► Session 5min   (接单率)         │
        └─► DriverSession  (在线时长分钟窗) ─┘
                     │
              ResultStore(resultId 幂等 + changelog)
                     │
              HTTP REST + SSE ──► 看板五页
```

关键设计:

- **纯事件时间确定性**:水位线、窗口触发、修正版本号、PRNG 全部由事件时间驱动,
  不调用 `Math.random` / `Date.now`(仅实时作业显式把事件时间基准对齐墙钟,且只用于交付节流)。
  因此同种子任意次运行、任意回放倍速,结果逐行一致。
- **检查点与批次边界对齐**:检查点不在批次中途触发,而在 `endBatch()` 落盘,
  保证快照里的生成器游标、WAL 位点、拓扑状态对应同一条事件边界。
- **WAL + 快照两段恢复**:快照周期之间的事件在 WAL 中;恢复时先还原快照,再重放
  `deliveryId > 检查点位点` 的 WAL 段。去重算子保证重放幂等。
- **精确一次两层**:入口 eventId 去重(同一交付不重复计算)+ 出口 resultId 幂等
  (同一结果不重复落表),SSE 用 `Last-Event-ID` 断点续传。
- **大体量状态外置 sidecar**:去重 ID 集合与生成器在途堆不进周期性主检查点,
  各自写独立 sidecar 文件并按检查点对齐,主快照保持在数百 KB,检查点不阻塞热路径。
- **热路径无全量扫描**:分城市水位线用最小堆维护活跃城市最小值;
  去重用 Set + 环形缓冲批量淘汰,避免大 Map 逐次删除的 V8 退化。

## 性能

引擎在 30 城、默认乱序/重复/迟到参数下的实测(本机,Node 24):

- 全管线持续吞吐 **约 7.5 万~12 万事件/秒**,高于 5 万/秒的设计目标
- 端到端处理延迟 p99 约 50ms,远低于 2 秒门禁
- 单次检查点耗时为毫秒级,主检查点文件数百 KB

运行 `npm run bench` 可复现吞吐、p99、重启一致性三段门禁。

## 目录

```
src/
  config.ts              全局配置与默认值
  types.ts               事件/结果/留痕模型
  rng.ts                 种子化 PRNG
  util/time.ts           窗口对齐、本地日边界、resultId 派生
  engine/                水位线、去重、窗口、司机会话、结果存储、指标、拓扑
  state/                 检查点(原子落盘)、WAL(同步追加)
  generator/generator.ts 确定性事件生成器(乱序/重复/迟到/倾斜/卡住可调)
  job/                   JobCoordinator(生命周期+恢复)、JobService(看板后端)
  server/http-server.ts  REST + SSE
  web/dashboard.html     五页看板
  cli/                   bench / generate / serve / replay / verify
test/                    node:test 单元与端到端测试
```

## 使用

需要 Node.js ≥ 22.18(已在 24 验证,直接运行 .ts,无需构建)。

```bash
npm install
npm test                 # 24 项单元 + 端到端测试
npm run typecheck        # 严格类型检查

npm run bench            # 压测:吞吐 / p99 / 恢复一致性三段门禁
npm run verify           # 确定性、崩溃恢复、语义覆盖验证
npm run generate -- --duration=60 --out=data/events.jsonl
npm run replay -- --replay-speed=60
npm run serve            # 启动看板,默认 http://127.0.0.1:8080
```

常用压测旋钮(均为 `--key=value`):
`--rate --duration --cities --out-of-order-ms --duplicate-rate --late-rate
--lateness-lag-ms --skew --stall-rate --allowed-lateness-ms --replay-speed`。

## 看板五页

1. **数据流拓扑**:算子链、各算子状态规模、分城市水位线与空闲/卡住标记
2. **窗口结果**:四类算子结果,含版本号与首帧/修正标记,可按算子过滤
3. **迟到数据**:合并修正与超期丢弃留痕、落后水位线毫秒数、归属窗口
4. **状态快照**:各算子状态规模、检查点文件、最近原始事件流
5. **回放**:倍速回放的基准 vs 回放逐行对比,以及 SSE 实时修正流

## 精确一次与恢复的边界

- 快照默认保留 3 个,检查点间隔 10s 事件时间;WAL 在每个检查点后截断。
- WAL 用同步批次 IO 保证"先写日志再改状态";崩溃残留半行在读取时跳过。
- 重复事件与原事件共享业务 `seq` 但各有独立 `deliveryId`,恢复位点按 deliveryId 跟踪,
  重复投递不会在恢复时被漏掉。
- 去重保留容量取"乱序/重复窗口"与"两个检查点周期"的最大值。后者是恢复正确性的硬约束:
  检查点游标早于崩溃点,续跑生成器会重发检查点之后的事件,这些重叠事件必须仍在去重
  集合内才能被幂等拦截。去重用 Set + 环形缓冲批量淘汰,大容量下也不会退化。
- 生成器在流末补发在途接单事件后置 `exhausted`,下一次取数返回空批;
  驱动循环只能以空批作为终止条件,避免提前退出丢事件。
