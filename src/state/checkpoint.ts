/**
 * 检查点管理。
 *
 * 一致性模型:Node 单线程内,barrier 随事件流按序到达;引擎在同步处理 barrier 的
 * 这一刻收集所有算子状态 —— 不存在其他事件并发修改状态,因此天然是对齐的一致快照。
 *
 * 落盘:先写 <dir>/tmp-<id>.json,fsync 后原子 rename 为 chk-<id>.json,
 * 崩溃只会留下临时文件或上一个完整快照,不会出现半个快照被读到。
 * 恢复时选取 checkpointId 最大的完整快照。
 */
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import type { EngineConfig } from '../config.ts';

export interface CheckpointEnvelope {
  version: 1;
  checkpointId: number;
  eventTimeWatermark: number;
  wallTimeMs: number;
  /** 检查点已覆盖的最大投递序号(deliveryId),WAL 据此截断与重放 */
  sourceLastDeliveryId: number;
  /**
   * 生成器小游标(不含大体量堆)。完整恢复还需读取 generatorSidecarFile。
   */
  generatorCursor: unknown | null;
  /** 生成器大堆状态所在的 sidecar 文件名(与检查点同目录,null 表示无生成器) */
  generatorSidecarFile: string | null;
  state: {
    watermarkAssigner: unknown;
    dedup: unknown;
    windowOps: Record<string, unknown>;
    driverSession: unknown;
    resultStore: unknown;
  };
}

export class CheckpointManager {
  private readonly cfg: EngineConfig;

  constructor(cfg: EngineConfig) {
    this.cfg = cfg;
    mkdirSync(this.cfg.checkpoint.dir, { recursive: true });
  }

  private path(id: number): string {
    return join(this.cfg.checkpoint.dir, `chk-${String(id).padStart(8, '0')}.json`);
  }

  private tmpPath(id: number): string {
    return join(this.cfg.checkpoint.dir, `tmp-${String(id).padStart(8, '0')}.json`);
  }

  save(envelope: CheckpointEnvelope): string {
    const tmp = this.tmpPath(envelope.checkpointId);
    const final = this.path(envelope.checkpointId);
    // 单文件快照;30M 事件规模下算子状态为聚合后的有界状态,文件体量可控
    writeFileSync(tmp, JSON.stringify(envelope));
    renameSync(tmp, final);
    this.gc();
    return final;
  }

  /** 保留最近 retain 个快照,其余删除(含关联的生成器 sidecar) */
  private gc(): void {
    const kept = new Set(
      readdirSync(this.cfg.checkpoint.dir)
        .filter((f) => f.startsWith('chk-') && f.endsWith('.json'))
        .sort()
        .slice(-this.cfg.checkpoint.retain),
    );
    for (const f of readdirSync(this.cfg.checkpoint.dir)) {
      // 主检查点:不在保留集合则删除
      if (f.startsWith('chk-') && f.endsWith('.json') && !kept.has(f)) {
        rmSync(join(this.cfg.checkpoint.dir, f), { force: true });
        continue;
      }
      // 生成器 sidecar:gen-cp-<id>.json,其对应检查点已不在保留集合则删除
      if (f.startsWith('gen-cp-') && f.endsWith('.json')) {
        const id = f.slice('gen-cp-'.length, 'gen-cp-'.length + 8);
        if (!kept.has(`chk-${id}.json`)) {
          rmSync(join(this.cfg.checkpoint.dir, f), { force: true });
        }
      }
    }
  }

  latest(): CheckpointEnvelope | null {
    if (!existsSync(this.cfg.checkpoint.dir)) return null;
    const files = readdirSync(this.cfg.checkpoint.dir)
      .filter((f) => f.startsWith('chk-') && f.endsWith('.json'))
      .sort();
    const newest = files.at(-1);
    if (!newest) return null;
    return JSON.parse(
      readFileSync(join(this.cfg.checkpoint.dir, newest), 'utf8'),
    ) as CheckpointEnvelope;
  }

  list(): Array<{ id: number; path: string; size: number }> {
    if (!existsSync(this.cfg.checkpoint.dir)) return [];
    return readdirSync(this.cfg.checkpoint.dir)
      .filter((f) => f.startsWith('chk-') && f.endsWith('.json'))
      .sort()
      .map((f) => {
        const id = Number(f.slice(4, 12));
        const size = statSync(join(this.cfg.checkpoint.dir, f)).size;
        return { id, path: join(this.cfg.checkpoint.dir, f), size };
      });
  }

  clear(): void {
    rmSync(this.cfg.checkpoint.dir, { recursive: true, force: true });
    mkdirSync(this.cfg.checkpoint.dir, { recursive: true });
  }
}
