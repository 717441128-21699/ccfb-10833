/**
 * 入口去重算子:同一 eventId 的重复投递只放行第一次。
 *
 * 设计(按到达顺序的有界保留):
 *  - 网关重投的重复副本与原始事件在相近处理时刻到达(乱序边界内);
 *    迟到事件与其副本"同时到达",因此按到达顺序保留可同时正确处理乱序与迟到。
 *
 * 高性能结构(避免大 Map 逐次删除导致的 V8 退化):
 *  - seen: Set<string> 负责 O(1) 判重
 *  - ring: 定长环形数组按到达顺序记录 eventId;写满一圈时,把最老一圈对应的 ID
 *    批量从 Set 删除。每个 eventId 平均只触发一次 Set.delete,而非每事件删除
 *
 * 持久化:
 *  - 首次见到的 ID 追加到只增 sidecar(dedup-sidecar.log),批次边界 flush
 *  - 检查点时刻把 sidecar 原子复制为 dedup-cp-<id>.log,严格止于检查点边界
 *  - 主检查点只保存计数,不保存 ID 数组
 *
 * 正确性边界:窗口算子会拒绝超出 allowedLateness 的过期事件,
 * 即便极端情况下重复副本晚于去重容量到达,它也无法再改变任何窗口结果。
 */
import {
  appendFileSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import type { EngineConfig } from '../config.ts';
import type { StreamEvent } from '../types.ts';

export interface DedupSnapshot {
  maxArrivalSeq: number;
  retainedCount: number;
  sidecarFile: string | null;
  duplicatesDropped: number;
  passed: number;
}

export class DedupOperator {
  /** 判重集合 */
  private seen = new Set<string>();
  /** 按到达顺序的环形缓冲,长度 = 保留容量 */
  private ring: string[];
  private ringPos = 0;
  private arrivalSeq = 0;
  private readonly retainCapacity: number;
  private readonly sidecarPath: string | null;
  private readonly sidecarDir: string | null;
  private pendingLines: string[] = [];
  duplicatesDropped = 0;
  passed = 0;
  recentDuplicates: StreamEvent[] = [];

  constructor(cfg: EngineConfig, sidecarDir?: string) {
    // 容量必须同时满足两个职责,取最大值:
    //  1. 传输层去重:覆盖乱序边界 + 重复保留窗口内的事件
    //  2. 崩溃恢复幂等:检查点游标早于崩溃点,续跑生成器会重发检查点之后的事件,
    //     这些重叠事件必须仍能被 eventId 去重识别,因此至少覆盖两个检查点周期
    const ratePerMs = cfg.generator.ratePerSecond / 1000;
    const dedupWindowMs = cfg.watermark.outOfOrderBoundMs + cfg.dedupTtlMs;
    const recoveryWindowMs = cfg.checkpoint.intervalMs * 2;
    const retainMs = Math.max(dedupWindowMs, recoveryWindowMs);
    this.retainCapacity = Math.max(
      10_000,
      Math.ceil(ratePerMs * retainMs * 1.5),
    );
    this.ring = new Array<string>(this.retainCapacity);
    if (sidecarDir) {
      mkdirSync(sidecarDir, { recursive: true });
      this.sidecarDir = sidecarDir;
      this.sidecarPath = join(sidecarDir, 'dedup-sidecar.log');
    } else {
      this.sidecarDir = null;
      this.sidecarPath = null;
    }
  }

  /** 返回 true 表示首次出现、放行;false 表示重复、丢弃 */
  process(event: StreamEvent, _currentWatermark: number): boolean {
    this.arrivalSeq++;
    if (this.seen.has(event.eventId)) {
      this.duplicatesDropped++;
      if (this.recentDuplicates.length >= 200) this.recentDuplicates.shift();
      this.recentDuplicates.push(event);
      return false;
    }

    this.seen.add(event.eventId);
    if (this.sidecarPath) {
      this.pendingLines.push(`${this.arrivalSeq}\t${event.eventId}`);
    }
    this.passed++;

    // 环形淘汰:槽位已有更老的 ID,先把它从判重集合移除,再覆盖
    const slot = this.ring[this.ringPos];
    if (slot !== undefined) this.seen.delete(slot);
    this.ring[this.ringPos] = event.eventId;
    this.ringPos++;
    if (this.ringPos >= this.retainCapacity) this.ringPos = 0;
    return true;
  }

  /** 批次边界:把新增 ID 一次性同步追加到只增 sidecar */
  flushBatch(): void {
    if (!this.sidecarPath || this.pendingLines.length === 0) return;
    appendFileSync(this.sidecarPath, this.pendingLines.join('\n') + '\n');
    this.pendingLines = [];
  }

  /** 检查点对齐:flush 后把 sidecar 复制为带检查点编号的副本,并压缩只增日志 */
  alignToCheckpoint(checkpointId: number): string | null {
    if (!this.sidecarPath || !this.sidecarDir) return null;
    this.flushBatch();
    const file = `dedup-cp-${String(checkpointId).padStart(8, '0')}.log`;
    const target = join(this.sidecarDir, file);
    if (existsSync(this.sidecarPath)) copyFileSync(this.sidecarPath, target);
    // 用当前存活集合压缩只增日志,防止长跑中无限增长
    this.compactLiveSidecar();
    return file;
  }

  /** 把只增 sidecar 重写为当前判重集合内容(临时文件 + 原子 rename) */
  private compactLiveSidecar(): void {
    if (!this.sidecarPath) return;
    const lines: string[] = [];
    let seq = 0;
    for (const id of this.seen) lines.push(`${++seq}\t${id}`);
    const tmp = this.sidecarPath + '.tmp';
    writeFileSync(tmp, lines.join('\n') + (lines.length ? '\n' : ''));
    renameSync(tmp, this.sidecarPath);
  }

  static removeCheckpointSidecar(dir: string, file: string | null): void {
    if (!file) return;
    rmSync(join(dir, file), { force: true });
  }

  /** 从检查点 sidecar 副本重建判重集合与环形缓冲 */
  private loadCheckpointSidecar(file: string | null): void {
    if (!file || !this.sidecarDir) return;
    const p = join(this.sidecarDir, file);
    if (!existsSync(p)) return;
    const ids: string[] = [];
    for (const line of readFileSync(p, 'utf8').split('\n')) {
      if (!line) continue;
      const tab = line.indexOf('\t');
      if (tab <= 0) continue;
      const id = line.slice(tab + 1);
      if (id) ids.push(id);
    }
    // 只保留最近 retainCapacity 个,重建 Set 与 ring
    const kept = ids.length > this.retainCapacity ? ids.slice(-this.retainCapacity) : ids;
    this.seen = new Set(kept);
    this.ring = new Array<string>(this.retainCapacity);
    for (let i = 0; i < kept.length; i++) this.ring[i] = kept[i]!;
    this.ringPos = kept.length >= this.retainCapacity ? 0 : kept.length;
  }

  get bucketCount(): number {
    return this.seen.size;
  }

  get retainedIds(): number {
    return this.seen.size;
  }

  get capacity(): number {
    return this.retainCapacity;
  }

  snapshotEntries(): IterableIterator<string> {
    return this.seen.keys();
  }

  snapshot(checkpointId: number): DedupSnapshot {
    const sidecarFile = this.alignToCheckpoint(checkpointId);
    return {
      maxArrivalSeq: this.arrivalSeq,
      retainedCount: this.seen.size,
      sidecarFile,
      duplicatesDropped: this.duplicatesDropped,
      passed: this.passed,
    };
  }

  restore(s: DedupSnapshot): void {
    this.loadCheckpointSidecar(s.sidecarFile);
    this.arrivalSeq = s.maxArrivalSeq;
    this.duplicatesDropped = s.duplicatesDropped;
    this.passed = s.passed;
  }
}
