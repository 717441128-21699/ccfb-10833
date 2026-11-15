/**
 * 作业协调器:拥有 WAL + EventGenerator + Topology 的完整生命周期。
 *
 * 精确一次恢复模型(需求 5):
 *  1. 每个事件 feed 前先 append 到 WAL(预写)
 *  2. 水位线推进到检查点间隔时,Topology 同步产出算子状态;协调器把生成器游标、
 *     已处理位点一并原子落盘,然后截断 WAL 中位点之前的段
 *  3. 崩溃恢复:
 *     a. Topology 从最近检查点还原
 *     b. 重放 WAL 中 seq > 检查点位点的事件(入口去重保证幂等)
 *     c. EventGenerator 从检查点游标继续 —— 后续事件与"没重启过"完全一致
 */
import { readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { EngineConfig } from '../config.ts';
import type { StreamEvent } from '../types.ts';
import {
  CheckpointManager,
  type CheckpointEnvelope,
} from '../state/checkpoint.ts';
import { WriteAheadLog } from '../state/wal.ts';
import { Topology, type TopologySnapshot } from '../engine/topology.ts';
import {
  EventGenerator,
  type GeneratorCursor,
  type GeneratorHeapState,
} from '../generator/generator.ts';

export interface RestorePoint {
  envelope: CheckpointEnvelope;
  generatorCursor: GeneratorCursor | null;
}

let coordinatorSeq = 0;

export class JobCoordinator {
  readonly cfg: EngineConfig;
  readonly topology: Topology;
  readonly checkpointManager: CheckpointManager;
  readonly wal: WriteAheadLog;
  generator: EventGenerator | null = null;
  /** 已进入拓扑的最大投递序号 */
  private processedMaxDeliveryId = -1;
  /** 最近一次恢复实际重放的 WAL 事件数(看板诊断) */
  private lastWalReplayed = 0;
  private readonly instanceDir: string;
  private readonly workDir: string;

  constructor(cfg: EngineConfig, instanceId?: string) {
    this.cfg = cfg;
    // 每个协调器使用独立快照子目录,避免并行作业/测试的 GC 互相删除文件;
    // 重启模拟传入相同 instanceId 以复用同一目录读取历史快照与 WAL
    this.instanceDir =
      instanceId ?? `job-${process.pid}-${++coordinatorSeq}`;
    const isolatedCfg = structuredClone(cfg);
    isolatedCfg.checkpoint.dir = join(cfg.checkpoint.dir, this.instanceDir);
    this.workDir = isolatedCfg.checkpoint.dir;
    this.topology = new Topology(isolatedCfg);
    this.checkpointManager = new CheckpointManager(isolatedCfg);
    this.wal = new WriteAheadLog(isolatedCfg.checkpoint.dir);
    // 拓扑触发检查点时,由协调器补上生成器游标与位点后落盘
    this.topology.onCheckpoint = ({ checkpointId, watermark, snapshot }) => {
      this.persist(checkpointId, watermark, snapshot);
    };
  }

  attachGenerator(generator: EventGenerator): void {
    this.generator = generator;
  }

  private persist(
    checkpointId: number,
    watermark: number,
    snapshot: TopologySnapshot,
  ): void {
    let generatorSidecarFile: string | null = null;
    let compactCursor: unknown = null;

    if (this.generator) {
      // 大堆状态写独立 sidecar(临时文件 + 原子 rename),主检查点只保留小游标
      generatorSidecarFile = `gen-cp-${String(checkpointId).padStart(8, '0')}.json`;
      const heaps: GeneratorHeapState = this.generator.heapState();
      const tmp = join(this.workDir, generatorSidecarFile + '.tmp');
      writeFileSync(tmp, JSON.stringify(heaps));
      renameSync(tmp, join(this.workDir, generatorSidecarFile));
      compactCursor = this.generator.compactCursor();
    }

    const envelope: CheckpointEnvelope = {
      version: 1,
      checkpointId,
      eventTimeWatermark: watermark,
      wallTimeMs: Date.now(),
      sourceLastDeliveryId: this.processedMaxDeliveryId,
      generatorCursor: compactCursor,
      generatorSidecarFile,
      state: snapshot as unknown as CheckpointEnvelope['state'],
    };
    const path = this.checkpointManager.save(envelope);
    // 检查点已包含该位点之前的状态,WAL 只需要保留之后的事件
    this.wal.truncateBefore(this.processedMaxDeliveryId);
    this.topology.lastCheckpointInfo = {
      id: checkpointId,
      watermark,
      wallMs: envelope.wallTimeMs,
      path,
      size: statSync(path).size,
    };
  }

  /** 事件入口:先写 WAL,再进拓扑(WAL 先于状态变更,崩溃不丢事件) */
  feed(event: StreamEvent): void {
    this.wal.append(event);
    this.topology.handleEvent(event);
    if (event.deliveryId > this.processedMaxDeliveryId) {
      this.processedMaxDeliveryId = event.deliveryId;
    }
  }

  /**
   * 批次边界:先把 WAL 缓冲批量落盘,再完成批次内登记的检查点。
   * 此刻生成器游标、WAL、拓扑状态三者对齐到同一条事件边界。
   */
  endBatch(): void {
    this.wal.flushBatch();
    this.topology.completePendingCheckpoint();
  }

  /** 读取最近快照(不修改当前状态);生成器游标由小游标 + sidecar 堆组装 */
  latestRestorePoint(): RestorePoint | null {
    const envelope = this.checkpointManager.latest();
    if (!envelope) return null;
    return {
      envelope,
      generatorCursor: this.loadGeneratorCursor(envelope),
    };
  }

  /** 从主检查点小游标 + 生成器 sidecar 组装完整游标 */
  private loadGeneratorCursor(
    envelope: CheckpointEnvelope,
  ): GeneratorCursor | null {
    const compact = envelope.generatorCursor as GeneratorCursor | null;
    if (!compact) return null;
    let heaps: GeneratorHeapState = { acceptHeap: [], driverHeap: [] };
    if (envelope.generatorSidecarFile) {
      const p = join(this.workDir, envelope.generatorSidecarFile);
      heaps = JSON.parse(readFileSync(p, 'utf8')) as GeneratorHeapState;
    }
    return { ...compact, ...heaps };
  }

  /**
   * 从最近快照恢复拓扑,并重放检查点之后的 WAL 事件。
   * 返回生成器游标(无快照返回 null)。
   */
  restoreLatest(): GeneratorCursor | null {
    const point = this.latestRestorePoint();
    if (!point) return null;
    const snapshot = point.envelope.state as unknown as TopologySnapshot;
    this.topology.restore(snapshot, point.envelope.eventTimeWatermark);
    this.processedMaxDeliveryId = point.envelope.sourceLastDeliveryId;

    // 重放检查点之后写入 WAL 的事件;去重算子已含检查点内的 eventId,天然幂等
    const checkpointDeliveryId = point.envelope.sourceLastDeliveryId;
    this.lastWalReplayed = 0;
    for (const event of this.wal.readAll()) {
      if (event.deliveryId > checkpointDeliveryId) {
        this.topology.handleEvent(event);
        this.lastWalReplayed++;
        if (event.deliveryId > this.processedMaxDeliveryId) {
          this.processedMaxDeliveryId = event.deliveryId;
        }
      }
    }
    return point.generatorCursor;
  }

  /** 最近一次恢复实际重放的 WAL 事件数(看板诊断) */
  walReplayCount(): number {
    return this.lastWalReplayed;
  }

  flush(): void {
    this.topology.flush();
  }

  get walEntries(): number {
    return this.wal.entryCount();
  }
}
