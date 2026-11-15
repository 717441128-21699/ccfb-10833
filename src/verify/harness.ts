/**
 * 确定性验证与压测测试台。
 *
 * 提供三种引擎驱动方式,全部在进程内全速运行(不做墙钟节流):
 *  - runOnce            :一次性跑完整个生成器,收集最终结果表
 *  - runWithRestart     :跑到指定事件时间进度时模拟崩溃,从最近快照恢复后续跑
 *  - runFromJsonl       :消费文件事件(验证外部历史数据回放)
 *
 * 结果对比按 resultId 对齐,逐字段比较 value/version,任何差异都视为口径错误。
 */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import type { EngineConfig } from '../config.ts';
import type { StreamEvent, WindowResult } from '../types.ts';
import { JobCoordinator } from '../job/coordinator.ts';
import { EventGenerator } from '../generator/generator.ts';

export interface EngineRunOutcome {
  results: Map<string, WindowResult<string, Record<string, number>>>;
  lateRecords: number;
  processed: number;
  duplicates: number;
  corrections: number;
  checkpoints: number;
  endWatermark: number;
  wallMs: number;
}

function collectOutcome(
  coordinator: JobCoordinator,
  processed: number,
  wallMs: number,
): EngineRunOutcome {
  const results = new Map(
    coordinator.topology.resultStore
      .allResults()
      .map((r) => [r.resultId, structuredClone(r)]),
  );
  return {
    results,
    lateRecords: coordinator.topology.resultStore.latestLate.length,
    processed,
    duplicates: coordinator.topology.metrics.duplicates,
    corrections: coordinator.topology.metrics.corrections,
    checkpoints: coordinator.topology.metrics.checkpointsCompleted,
    endWatermark: coordinator.topology.currentWatermark,
    wallMs,
  };
}

/** 全速跑一遍生成器 */
export function runOnce(cfg: EngineConfig, instanceId?: string): EngineRunOutcome {
  const coordinator = new JobCoordinator(cfg, instanceId);
  const generator = new EventGenerator(cfg);
  coordinator.attachGenerator(generator);

  const start = performance.now();
  let processed = 0;
  for (;;) {
    const batch = generator.nextBatch(2_000);
    if (batch.length === 0) break;
    for (const event of batch) {
      event.ingestWallMs = performance.now();
      coordinator.feed(event);
      processed++;
    }
    // 批次边界完成检查点,保证生成器游标与拓扑状态对齐
    coordinator.endBatch();
    if (generator.progress >= 1) break;
  }
  coordinator.flush();
  return collectOutcome(coordinator, processed, performance.now() - start);
}

export interface RestartRunOutcome extends EngineRunOutcome {
  restartAtProgress: number;
  restartCheckpointId: number;
}

/**
 * 模拟崩溃恢复:跑到 restartAtProgress 时停止喂入,从最近检查点恢复,
 * 新生成器从快照游标续跑,最终结果必须与连续运行一致。
 */
export function runWithRestart(
  cfg: EngineConfig,
  restartAtProgress = 0.5,
): RestartRunOutcome {
  const instanceId = `restart-${cfg.generator.seed}-${cfg.generator.durationSeconds}`;
  const coordinator = new JobCoordinator(cfg, instanceId);
  const generator = new EventGenerator(cfg);
  coordinator.attachGenerator(generator);

  const start = performance.now();
  let processed = 0;
  let restarted = false;
  let restartCheckpointId = 0;

  for (;;) {
    const batch = generator.nextBatch(2_000);
    if (batch.length === 0) break;
    for (const event of batch) {
      event.ingestWallMs = performance.now();
      coordinator.feed(event);
      processed++;
    }
    // 批次边界先完成检查点,再判断是否在此边界模拟崩溃,
    // 保证崩溃点与一个已落盘的检查点对齐
    coordinator.endBatch();
    if (!restarted && generator.progress >= restartAtProgress) {
      restarted = true;
      // 模拟崩溃:丢弃内存中的生成器与管线,仅靠磁盘快照 + WAL 重建
      const cursor = coordinator.restoreLatest();
      if (!cursor) throw new Error('重启模拟失败:没有可用快照');
      restartCheckpointId = coordinator.topology.metrics.checkpointsCompleted;
      // WAL 重放已在 restoreLatest 内完成(检查点与崩溃点之间的事件不丢)
      const resumedGenerator = new EventGenerator(cfg);
      resumedGenerator.restoreCursor(cursor);
      coordinator.attachGenerator(resumedGenerator);
      // 用新生成器替换循环驱动对象
      driveResumed(coordinator, resumedGenerator, (n) => (processed += n));
      break;
    }
    if (generator.progress >= 1) break;
  }
  coordinator.flush();
  return {
    ...collectOutcome(coordinator, processed, performance.now() - start),
    restartAtProgress,
    restartCheckpointId,
  };
}

function driveResumed(
  coordinator: JobCoordinator,
  generator: EventGenerator,
  addProcessed: (n: number) => void,
): void {
  for (;;) {
    const batch = generator.nextBatch(2_000);
    if (batch.length === 0) break;
    for (const event of batch) {
      event.ingestWallMs = performance.now();
      coordinator.feed(event);
      addProcessed(1);
    }
    coordinator.endBatch();
    if (generator.progress >= 1) break;
  }
}

/** 从 JSONL 文件回放(文件由 generate CLI 产出) */
export function runFromJsonl(cfg: EngineConfig, path: string): EngineRunOutcome {
  const coordinator = new JobCoordinator(cfg);
  const lines = readFileSync(path, 'utf8').split('\n').filter(Boolean);
  const start = performance.now();
  let processed = 0;
  for (const line of lines) {
    const event = JSON.parse(line) as StreamEvent;
    event.ingestWallMs = performance.now();
    coordinator.feed(event);
    processed++;
  }
  coordinator.flush();
  return collectOutcome(coordinator, processed, performance.now() - start);
}

export interface ResultDiff {
  onlyInA: string[];
  onlyInB: string[];
  valueMismatch: Array<{
    resultId: string;
    a: WindowResult<string, Record<string, number>>;
    b: WindowResult<string, Record<string, number>>;
  }>;
  identical: number;
}

/** 对比两份结果表(精确一次口径下应当完全一致) */
export function compareResults(
  a: Map<string, WindowResult<string, Record<string, number>>>,
  b: Map<string, WindowResult<string, Record<string, number>>>,
): ResultDiff {
  const onlyInA: string[] = [];
  const onlyInB: string[] = [];
  const valueMismatch: ResultDiff['valueMismatch'] = [];
  let identical = 0;

  for (const [id, ra] of a) {
    const rb = b.get(id);
    if (!rb) {
      onlyInA.push(id);
      continue;
    }
    const sameValue =
      JSON.stringify(ra.value) === JSON.stringify(rb.value) &&
      ra.version === rb.version &&
      ra.windowStart === rb.windowStart &&
      ra.windowEnd === rb.windowEnd;
    if (sameValue) identical++;
    else valueMismatch.push({ resultId: id, a: ra, b: rb });
  }
  for (const id of b.keys()) {
    if (!a.has(id)) onlyInB.push(id);
  }
  return { onlyInA, onlyInB, valueMismatch, identical };
}

/** 把结果表导出为 JSONL,作为回放基准 */
export function exportBaseline(outcome: EngineRunOutcome, path: string): void {
  mkdirSync(path.slice(0, path.lastIndexOf('/')) || '.', { recursive: true });
  const lines = [...outcome.results.values()]
    .sort((x, y) => x.resultId.localeCompare(y.resultId))
    .map((r) => JSON.stringify(r));
  writeFileSync(path, lines.join('\n') + '\n');
}

export function importBaseline(
  path: string,
): Map<string, WindowResult<string, Record<string, number>>> {
  const lines = readFileSync(path, 'utf8').split('\n').filter(Boolean);
  const map = new Map<string, WindowResult<string, Record<string, number>>>();
  for (const line of lines) {
    const r = JSON.parse(line) as WindowResult<string, Record<string, number>>;
    map.set(r.resultId, r);
  }
  return map;
}
