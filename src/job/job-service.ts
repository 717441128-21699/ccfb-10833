/**
 * 作业服务:看板背后的长驻作业管理器。
 *
 * 职责:
 *  - 启动/停止实时流作业(live,墙钟节流)
 *  - 启动确定性回放作业(replay,可指定倍速;Infinity 全速)
 *  - 维护最近事件流样本(拓扑页实时展示)
 *  - 暴露拓扑、结果、迟到、快照、指标等只读视图给 HTTP 层
 *
 * 同一时刻只允许一个运行中作业,避免多个作业争抢快照目录。
 */
import type { EngineConfig } from '../config.ts';
import type { StreamEvent } from '../types.ts';
import { JobCoordinator } from './coordinator.ts';
import { EventGenerator } from '../generator/generator.ts';
import { compareResults, runOnce, type EngineRunOutcome } from '../verify/harness.ts';

export type JobPhase = 'idle' | 'running' | 'finished' | 'stopped' | 'failed';

export interface JobStatus {
  phase: JobPhase;
  mode: 'live' | 'replay' | null;
  startedAt: number | null;
  finishedAt: number | null;
  processed: number;
  progress: number;
  error: string | null;
  replay: {
    speed: number;
    baselineRows: number;
    resultRows: number;
    identical: number;
    mismatch: number;
    onlyInBaseline: number;
    onlyInReplay: number;
    consistent: boolean;
  } | null;
}

export interface BusEvent {
  id: number;
  kind: 'result' | 'late' | 'job';
  payload: unknown;
}

const EVENT_SAMPLE_CAP = 300;
const BUS_CAP = 5_000;

export class JobService {
  private coordinator: JobCoordinator | null = null;
  private generator: EventGenerator | null = null;
  private stopRequested = false;
  private status: JobStatus = {
    phase: 'idle',
    mode: null,
    startedAt: null,
    finishedAt: null,
    processed: 0,
    progress: 0,
    error: null,
    replay: null,
  };
  private eventSample: StreamEvent[] = [];
  private runPromise: Promise<void> | null = null;
  /** 跨作业事件总线:SSE 订阅它,作业切换不影响推送 */
  private bus: BusEvent[] = [];
  private busNextId = 1;
  private subscribers = new Set<(e: BusEvent) => void>();

  private readonly baseCfg: EngineConfig;

  constructor(baseCfg: EngineConfig) {
    this.baseCfg = baseCfg;
  }

  /** 订阅总线事件,返回取消函数;backfillId > 0 时先补发缺失事件 */
  subscribe(fn: (e: BusEvent) => void, backfillId = 0): () => void {
    for (const e of this.bus) if (e.id > backfillId) fn(e);
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  private publish(kind: BusEvent['kind'], payload: unknown): void {
    const entry: BusEvent = { id: this.busNextId++, kind, payload };
    this.bus.push(entry);
    if (this.bus.length > BUS_CAP) this.bus.splice(0, this.bus.length - BUS_CAP);
    for (const sub of this.subscribers) {
      try {
        sub(entry);
      } catch {
        /* 订阅者异常不影响作业 */
      }
    }
  }

  getStatus(): JobStatus {
    return { ...this.status, processed: this.status.processed };
  }

  private ensureIdle(): void {
    if (this.status.phase === 'running') {
      throw new Error('已有运行中的作业,请先停止');
    }
  }

  /** 启动实时作业 */
  startLive(overrides?: Partial<EngineConfig['generator']>): JobStatus {
    this.ensureIdle();
    const cfg = structuredClone(this.baseCfg);
    // 实时作业的事件时间对齐当前墙钟;回放保持确定性固定基准
    cfg.generator.baseEventTime = Date.now();
    if (overrides) Object.assign(cfg.generator, overrides);
    this.beginRun(cfg, 'live');
    return this.getStatus();
  }

  /**
   * 启动回放作业。先全速计算同种子基准,再以指定倍速驱动同一事件流,
   * 结束后自动对比两份结果(需求 6 的服务端验证)。
   */
  startReplay(speed: number, overrides?: Partial<EngineConfig['generator']>): JobStatus {
    this.ensureIdle();
    const cfg = structuredClone(this.baseCfg);
    if (overrides) Object.assign(cfg.generator, overrides);
    this.beginRun(cfg, 'replay', speed);
    return this.getStatus();
  }

  private beginRun(cfg: EngineConfig, mode: 'live' | 'replay', speed = 1): void {
    this.coordinator = new JobCoordinator(cfg);
    this.generator = new EventGenerator(cfg);
    this.coordinator.attachGenerator(this.generator);
    // 结果存储的 changelog 桥接到跨作业总线
    this.coordinator.topology.resultStore.subscribe((entry) => {
      this.publish(entry.kind, entry.payload);
    });
    this.eventSample = [];
    this.stopRequested = false;
    this.status = {
      phase: 'running',
      mode,
      startedAt: Date.now(),
      finishedAt: null,
      processed: 0,
      progress: 0,
      error: null,
      replay:
        mode === 'replay'
          ? {
              speed,
              baselineRows: 0,
              resultRows: 0,
              identical: 0,
              mismatch: 0,
              onlyInBaseline: 0,
              onlyInReplay: 0,
              consistent: false,
            }
          : null,
    };
    this.publish('job', { phase: 'running', mode });
    this.runPromise = mode === 'live' ? this.runLive(cfg) : this.runReplay(cfg, speed);
  }

  async stop(): Promise<JobStatus> {
    if (this.status.phase !== 'running') return this.getStatus();
    this.stopRequested = true;
    if (this.runPromise) await this.runPromise;
    return this.getStatus();
  }

  private async runLive(cfg: EngineConfig): Promise<void> {
    try {
      const generator = this.generator!;
      const startWall = Date.now();
      const base = generator.baseEventTime;
      while (!this.stopRequested) {
        const batch = generator.nextBatch(2_000);
        // 只能以"生成器返回空批"作为结束标志:到达终点后生成器仍会补发
        // 在途接单事件(非空批),用 progress>=1 提前退出会丢事件
        if (batch.length === 0) break;
        for (const event of batch) {
          const target = startWall + (event.eventTime - base);
          const wait = target - Date.now();
          if (wait > 0) await new Promise((r) => setTimeout(r, wait));
          if (this.stopRequested) break;
          event.ingestWallMs = performance.now();
          this.coordinator!.feed(event);
          this.captureSample(event);
          this.status.processed++;
        }
        this.status.progress = generator.progress;
        // 批次边界完成检查点(生成器游标/拓扑/WAL 对齐)
        this.coordinator!.endBatch();
        // 定期让出事件循环,保证看板 API 与 SSE 可响应
        await new Promise((r) => setImmediate(r));
      }
      if (!this.stopRequested) this.coordinator!.flush();
      this.finish(this.stopRequested ? 'stopped' : 'finished');
    } catch (err) {
      this.fail(err);
    }
  }

  private async runReplay(cfg: EngineConfig, speed: number): Promise<void> {
    try {
      // 1. 基准(全速,确定性)
      const baseline: EngineRunOutcome = runOnce(structuredClone(cfg));
      this.status.replay!.baselineRows = baseline.results.size;

      // 2. 倍速回放(走 JobCoordinator,带快照;与实时完全相同的处理路径)
      const generator = this.generator!;
      const startWall = Date.now();
      const base = generator.baseEventTime;
      const infinite = !Number.isFinite(speed);

      while (!this.stopRequested) {
        const batch = generator.nextBatch(2_000);
        if (batch.length === 0) break;
        for (const event of batch) {
          if (!infinite) {
            const target = startWall + (event.eventTime - base) / speed;
            const wait = target - Date.now();
            if (wait > 0) await new Promise((r) => setTimeout(r, wait));
          }
          if (this.stopRequested) break;
          event.ingestWallMs = performance.now();
          this.coordinator!.feed(event);
          this.captureSample(event);
          this.status.processed++;
        }
        this.status.progress = generator.progress;
        this.coordinator!.endBatch();
        await new Promise((r) => setImmediate(r));
      }
      if (!this.stopRequested) this.coordinator!.flush();

      // 3. 对比
      const replayResults = new Map(
        this.coordinator!.topology.resultStore.allResults().map((r) => [r.resultId, r]),
      );
      const diff = compareResults(baseline.results, replayResults);
      const replay = this.status.replay!;
      replay.resultRows = replayResults.size;
      replay.identical = diff.identical;
      replay.mismatch = diff.valueMismatch.length;
      replay.onlyInBaseline = diff.onlyInA.length;
      replay.onlyInReplay = diff.onlyInB.length;
      replay.consistent =
        diff.valueMismatch.length === 0 &&
        diff.onlyInA.length === 0 &&
        diff.onlyInB.length === 0;

      this.finish(this.stopRequested ? 'stopped' : 'finished');
    } catch (err) {
      this.fail(err);
    }
  }

  private captureSample(event: StreamEvent): void {
    this.eventSample.push(event);
    if (this.eventSample.length > EVENT_SAMPLE_CAP) {
      this.eventSample.splice(0, this.eventSample.length - EVENT_SAMPLE_CAP);
    }
  }

  private finish(phase: JobPhase): void {
    this.status.phase = phase;
    this.status.finishedAt = Date.now();
    this.publish('job', { phase, status: { ...this.status } });
  }

  private fail(err: unknown): void {
    this.status.phase = 'failed';
    this.status.error = err instanceof Error ? err.message : String(err);
    this.status.finishedAt = Date.now();
    this.publish('job', { phase: 'failed', error: this.status.error });
  }

  // ---------- 看板只读视图 ----------

  get coordinatorOrThrow(): JobCoordinator {
    if (!this.coordinator) throw new Error('作业尚未启动');
    return this.coordinator;
  }

  get hasRun(): boolean {
    return this.coordinator !== null;
  }

  recentEvents(): StreamEvent[] {
    return [...this.eventSample].reverse();
  }

  topologyView(): Record<string, unknown> {
    if (!this.coordinator) return { operators: [], status: this.status };
    const t = this.coordinator.topology;
    return {
      status: this.status,
      operators: [
        {
          name: 'dedup',
          type: 'DedupOperator',
          retainedIds: t.dedup.retainedIds,
          duplicatesDropped: t.dedup.duplicatesDropped,
        },
        {
          name: 'watermark',
          type: 'WatermarkAssigner',
          globalWatermark: t.currentWatermark,
          emitted: t.metrics.watermarksEmitted,
          cities: t.watermarkAssigner.cityStates(),
        },
        ...t.windowOps.map((op, i) => ({
          name: op.name,
          type: 'WindowOperator',
          windowType: ['tumble', 'sliding', 'session'][i],
          activePanes: op.activePaneCount(),
          stats: op.stats,
        })),
        {
          name: t.driverSession.name,
          type: 'DriverSessionOperator',
          activePanes: t.driverSession.activePaneCount(),
          openSessions: t.driverSession.openSessions().length,
          stats: t.driverSession.stats,
        },
        {
          name: 'result-store',
          type: 'ResultStore',
          rows: t.resultStore.resultCount,
        },
      ],
    };
  }
}
