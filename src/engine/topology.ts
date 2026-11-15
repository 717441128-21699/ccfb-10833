/**
 * 流处理拓扑:把去重 -> 水位线 -> 窗口算子们 -> 结果存储 串成一条有向管线。
 *
 * 数据流(单线程顺序处理,保证确定性):
 *
 *   Source --data--> Dedup --data--> [Tumble/Sliding/Session 窗口算子] --result--> ResultStore
 *                     |                                                  ↑
 *                     +--> WatermarkAssigner --watermark--> 各算子 processWatermark
 *                     +--> DriverSessionOperator ───────── result ───────┘
 *
 * barrier 按事件时间间隔插入;到达时同步收集全算子状态落盘。
 */
import { readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import type { EngineConfig } from '../config.ts';
import type {
  LateRecord,
  StreamEvent,
  WindowResult,
} from '../types.ts';
import { CheckpointManager } from '../state/checkpoint.ts';
import { OrderCountersAggregate } from './aggregates.ts';
import { DedupOperator, type DedupSnapshot } from './dedup.ts';
import { DriverSessionOperator, type DriverOperatorSnapshot } from './driver-session.ts';
import { MetricsCollector } from './metrics.ts';
import { ResultStore, type ResultStoreSnapshot } from './result-store.ts';
import {
  SessionAssigner,
  SlidingAssigner,
  TumbleAssigner,
  WindowOperator,
  type WindowOperatorSnapshot,
} from './window.ts';
import {
  WatermarkAssigner,
  type WatermarkAssignerSnapshot,
} from './watermark.ts';

export interface TopologySnapshot {
  checkpointId: number;
  nextCheckpointWatermark: number;
  watermarkAssigner: WatermarkAssignerSnapshot;
  dedup: DedupSnapshot;
  windowOps: Record<string, WindowOperatorSnapshot>;
  driverSession: DriverOperatorSnapshot;
  resultStore: ResultStoreSnapshot;
}

export class Topology {
  readonly watermarkAssigner: WatermarkAssigner;
  readonly dedup: DedupOperator;
  readonly windowOps: WindowOperator<unknown>[] = [];
  readonly driverSession: DriverSessionOperator;
  readonly resultStore = new ResultStore();
  readonly metrics = new MetricsCollector();
  readonly checkpointManager: CheckpointManager;

  private checkpointId = 0;
  private nextCheckpointWatermark: number;
  private lastWatermark = Number.NEGATIVE_INFINITY;
  /** 批次内登记、批次边界落盘的待完成检查点水位线 */
  private pendingCheckpointWatermark: number | null = null;
  /** 快照触发回调:协调器收到后把生成器游标一并落盘 */
  onCheckpoint:
    | ((info: { checkpointId: number; watermark: number; snapshot: TopologySnapshot }) => void)
    | null = null;
  /** 最近一次完成的快照摘要(看板"状态快照"页) */
  lastCheckpointInfo: {
    id: number;
    watermark: number;
    wallMs: number;
    path: string;
    size: number;
  } | null = null;

  readonly cfg: EngineConfig;

  constructor(cfg: EngineConfig) {
    this.cfg = cfg;
    this.watermarkAssigner = new WatermarkAssigner(cfg);
    this.dedup = new DedupOperator(cfg, cfg.checkpoint.dir);
    this.checkpointManager = new CheckpointManager(cfg);
    this.nextCheckpointWatermark = cfg.checkpoint.intervalMs;

    const cb = {
      emit: (r: WindowResult<string, Record<string, number>>) => this.onResult(r),
      late: (r: LateRecord) => this.onLate(r),
    };

    const orderAgg = () => new OrderCountersAggregate();
    this.windowOps.push(
      new WindowOperator(
        'order-tumble-1min',
        new TumbleAssigner(cfg.windows.tumbleMs),
        orderAgg(),
        cfg,
        cb,
      ) as WindowOperator<unknown>,
    );
    this.windowOps.push(
      new WindowOperator(
        'order-sliding-1min-10s',
        new SlidingAssigner(cfg.windows.slideMs, cfg.windows.slideSizeMs),
        orderAgg(),
        cfg,
        cb,
      ) as WindowOperator<unknown>,
    );
    this.windowOps.push(
      new WindowOperator(
        'order-session',
        new SessionAssigner(cfg.windows.sessionGapMs),
        orderAgg(),
        cfg,
        cb,
      ) as WindowOperator<unknown>,
    );

    this.driverSession = new DriverSessionOperator(cfg, cb);
  }

  private onResult(r: WindowResult<string, Record<string, number>>): void {
    const changed = this.resultStore.upsert(r);
    if (!changed) return;
    this.metrics.emitted++;
    if (r.correction) this.metrics.corrections++;
  }

  private onLate(r: LateRecord): void {
    this.resultStore.recordLate(r);
    if (r.reason === 'merged-with-correction') this.metrics.lateMerged++;
    else this.metrics.lateDropped++;
  }

  /** 处理一个数据事件 */
  handleEvent(event: StreamEvent): void {
    this.metrics.received++;
    const wmBefore = this.watermarkAssigner.currentGlobalWatermark;

    // 1. 入口去重(重复投递只算一次)
    if (!this.dedup.process(event, wmBefore)) {
      this.metrics.duplicates++;
      return;
    }

    // 2. 分城市推进水位线(可能产生一条 watermark 元素)
    const wmElement = this.watermarkAssigner.processEvent(event);

    // 3. 数据事件分发给业务算子
    for (const op of this.windowOps) op.processEvent(event);
    this.driverSession.processEvent(event);

    // 4. 水位线元素按序在数据事件之后广播
    if (wmElement) this.broadcastWatermark(wmElement.globalWatermark);

    // 5. 延迟度量(仅墙钟口径,不影响业务语义)
    if (event.ingestWallMs !== undefined) {
      const wall = performance.now();
      this.metrics.observeLatency(wall - event.ingestWallMs);
      this.metrics.observeThroughput(wall);
    }
  }

  private broadcastWatermark(wm: number): void {
    if (wm <= this.lastWatermark) return;
    this.lastWatermark = wm;
    this.metrics.watermarksEmitted++;
    for (const op of this.windowOps) op.processWatermark(wm);
    this.driverSession.processWatermark(wm);

    // 只登记检查点请求,真正落盘由驱动层在事件批次边界调用 completePendingCheckpoint 完成。
    // 这样生成器游标与拓扑状态对应同一条事件边界,恢复后不会跳过批次内事件。
    if (wm >= this.nextCheckpointWatermark && this.pendingCheckpointWatermark === null) {
      this.pendingCheckpointWatermark = wm;
    }
  }

  /**
   * 批次边界:先落盘去重 sidecar 缓冲,再完成批次内登记的检查点(若有)。
   * 保证检查点的计数与 sidecar 中的 ID 始终对应同一事件边界。
   */
  completePendingCheckpoint(): boolean {
    this.dedup.flushBatch();
    const wm = this.pendingCheckpointWatermark;
    if (wm === null) return false;
    this.pendingCheckpointWatermark = null;
    this.takeCheckpoint(wm);
    this.nextCheckpointWatermark =
      Math.floor(wm / this.cfg.checkpoint.intervalMs) *
        this.cfg.checkpoint.intervalMs +
      this.cfg.checkpoint.intervalMs;
    return true;
  }

  private takeCheckpoint(wm: number): void {
    this.checkpointId++;
    const snapshot: TopologySnapshot = {
      checkpointId: this.checkpointId,
      nextCheckpointWatermark: this.nextCheckpointWatermark,
      watermarkAssigner: this.watermarkAssigner.snapshot(),
      dedup: this.dedup.snapshot(this.checkpointId),
      windowOps: Object.fromEntries(
        this.windowOps.map((op) => [op.name, op.snapshot()]),
      ),
      driverSession: this.driverSession.snapshot(),
      resultStore: this.resultStore.snapshot(),
    };
    this.gcDedupSidecars();
    this.metrics.checkpointsCompleted++;
    this.onCheckpoint?.({ checkpointId: this.checkpointId, watermark: wm, snapshot });
  }

  /** 检查点保留最近 retain 个,清理被淘汰检查点的去重 sidecar 副本 */
  private gcDedupSidecars(): void {
    const keep = new Set<number>();
    for (let i = 0; i < this.cfg.checkpoint.retain; i++) keep.add(this.checkpointId - i);
    for (const f of readdirSync(this.cfg.checkpoint.dir)) {
      if (!f.startsWith('dedup-cp-') || !f.endsWith('.log')) continue;
      // 文件名形如 dedup-cp-00000004.log,取固定宽度的编号段
      const id = Number(f.slice('dedup-cp-'.length, 'dedup-cp-'.length + 8));
      if (!Number.isFinite(id) || !keep.has(id)) {
        rmSync(join(this.cfg.checkpoint.dir, f), { force: true });
      }
    }
  }

  /** 流结束:闭合所有残留窗口与开放会话 */
  flush(): void {
    for (const op of this.windowOps) op.flush();
    this.driverSession.flush();
  }

  get currentWatermark(): number {
    return this.lastWatermark;
  }

  /** 从快照恢复(信封由协调器读取后传入) */
  restore(snapshot: TopologySnapshot, watermark: number): void {
    this.checkpointId = snapshot.checkpointId;
    this.nextCheckpointWatermark = snapshot.nextCheckpointWatermark;
    this.lastWatermark = watermark;

    this.watermarkAssigner.restore(snapshot.watermarkAssigner);
    this.dedup.restore(snapshot.dedup);
    for (const op of this.windowOps) {
      const snap = snapshot.windowOps[op.name];
      if (snap) op.restore(snap, new OrderCountersAggregate() as never);
    }
    this.driverSession.restore(snapshot.driverSession);
    this.resultStore.restore(snapshot.resultStore);
  }

  /** 看板"状态快照"页:各算子状态规模 */
  stateProfile(): Record<string, number | string> {
    return {
      dedupRetainedIds: this.dedup.retainedIds,
      dedupBuckets: this.dedup.bucketCount,
      tumblePanes: this.windowOps[0]?.activePaneCount() ?? 0,
      slidingPanes: this.windowOps[1]?.activePaneCount() ?? 0,
      sessionPanes: this.windowOps[2]?.activePaneCount() ?? 0,
      driverPanes: this.driverSession.activePaneCount(),
      openDriverSessions: this.driverSession.openSessions().length,
      resultRows: this.resultStore.resultCount,
      currentWatermark: this.currentWatermark,
    };
  }
}
