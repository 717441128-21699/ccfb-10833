/**
 * 事件预写日志(WAL)。
 *
 * 解决的问题:检查点是周期性的,崩溃可能发生在两个检查点之间。
 * 事件在进入计算拓扑前先进入 WAL;恢复时:
 *   1. 拓扑从最近检查点还原状态
 *   2. 重放 WAL 中 deliveryId > 检查点位点的所有事件(入口去重保证幂等,重放安全)
 *   3. 新检查点完成后截断已覆盖的 WAL 前缀
 *
 * 写入策略(吞吐与正确性的平衡):
 *  - append() 只入内存缓冲,不产生系统调用
 *  - flushBatch() 在每个事件批次边界做一次 appendFileSync 批量落盘
 *  - 检查点在批次边界触发,落盘前必然先 flushBatch —— 因此快照永远不领先于 WAL
 *  - 崩溃只可能丢失"当前未 flush 批次"的事件,而该批次本就未进入任何检查点,
 *    恢复后由生成器从检查点游标重新产出,不影响精确一次
 *
 * 截断写临时文件后原子 rename;崩溃最多留下未截断的旧 WAL,重放由去重兜底。
 */
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import type { StreamEvent } from '../types.ts';

export class WriteAheadLog {
  private readonly path: string;
  private buffer: string[] = [];
  private maxDeliveryId = -1;

  constructor(dir: string, name = 'wal.jsonl') {
    mkdirSync(dir, { recursive: true });
    this.path = join(dir, name);
    if (existsSync(this.path)) {
      const lines = readFileSync(this.path, 'utf8').split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const e = JSON.parse(line) as StreamEvent;
          if (e.deliveryId > this.maxDeliveryId) this.maxDeliveryId = e.deliveryId;
        } catch {
          /* 末尾半行:崩溃时写了一半,跳过 */
        }
      }
    }
  }

  /** 入内存缓冲(无系统调用);批次边界由 flushBatch 统一落盘 */
  append(event: StreamEvent): void {
    this.buffer.push(JSON.stringify(event));
    if (event.deliveryId > this.maxDeliveryId) this.maxDeliveryId = event.deliveryId;
  }

  /** 批次边界:把缓冲一次性同步追加到 WAL 文件 */
  flushBatch(): void {
    if (this.buffer.length === 0) return;
    appendFileSync(this.path, this.buffer.join('\n') + '\n');
    this.buffer = [];
  }

  /** 读取全部已落盘 WAL 事件(自动先 flush,保证缓冲事件可见) */
  readAll(): StreamEvent[] {
    this.flushBatch();
    if (!existsSync(this.path)) return [];
    const out: StreamEvent[] = [];
    for (const line of readFileSync(this.path, 'utf8').split('\n')) {
      if (!line) continue;
      try {
        out.push(JSON.parse(line) as StreamEvent);
      } catch {
        /* 半行:写入进程崩溃时可能残留,忽略 */
      }
    }
    return out;
  }

  /**
   * 检查点完成后截断:只保留 deliveryId > checkpointDeliveryId 的事件。
   */
  truncateBefore(checkpointDeliveryId: number): void {
    const remaining = this
      .readAll()
      .filter((e) => e.deliveryId > checkpointDeliveryId);
    const tmp = this.path + '.tmp';
    writeFileSync(
      tmp,
      remaining.map((e) => JSON.stringify(e)).join('') + (remaining.length ? '\n' : ''),
    );
    renameSync(tmp, this.path);
  }

  get currentMaxDeliveryId(): number {
    return this.maxDeliveryId;
  }

  get filePath(): string {
    return this.path;
  }

  /** 缓冲中尚未落盘的事件数(看板诊断) */
  get buffered(): number {
    return this.buffer.length;
  }

  close(): void {
    this.flushBatch();
  }

  delete(): void {
    this.buffer = [];
    rmSync(this.path, { force: true });
  }

  /** 已落盘行数(看板诊断用) */
  entryCount(): number {
    this.flushBatch();
    if (!existsSync(this.path)) return 0;
    return readFileSync(this.path, 'utf8').split('\n').filter(Boolean).length;
  }
}
