/**
 * 结果存储:精确一次投递的落点。
 *
 * 精确一次的两层保证:
 *  1. 入口去重让重复事件不产生重复计算
 *  2. 每个窗口结果带确定性 resultId,这里按 resultId 幂等 upsert ——
 *     即使重放/重试导致算子重发,下游表中每个结果只有一行,版本号单调更新
 *
 * 同时维护:
 *  - changelog:所有发出/修正的追加日志(看板 SSE 用 Last-Event-ID 断点续传)
 *  - lateRecords:迟到事件留痕(合并修正 + 超期丢弃)
 *  - 容量有界:changelog 与 lateRecords 保留最近 N 条,但当前值表完整保留
 */
import type { LateRecord, WindowResult } from '../types.ts';

interface ChangelogEntry {
  id: number;
  kind: 'result' | 'late';
  payload: unknown;
}

export interface ResultStoreSnapshot {
  current: Record<string, WindowResult<string, Record<string, number>>>;
  changelogSeq: number;
  nextChangelogId: number;
  lateSeq: number;
}

const CHANGELOG_CAP = 5_000;
const LATE_CAP = 2_000;

export class ResultStore {
  /** resultId -> 最新版本结果 */
  private current = new Map<string, WindowResult<string, Record<string, number>>>();
  private changelog: ChangelogEntry[] = [];
  private nextChangelogId = 1;
  private lateRecords: Array<{ id: number; record: LateRecord }> = [];
  private nextLateId = 1;

  /** SSE 订阅者 */
  private subscribers = new Set<(entry: ChangelogEntry) => void>();

  /** 幂等写入;返回 true 表示状态发生了实际变化 */
  upsert(result: WindowResult<string, Record<string, number>>): boolean {
    const prev = this.current.get(result.resultId);
    if (prev && prev.version >= result.version) return false;
    this.current.set(result.resultId, result);
    const entry: ChangelogEntry = {
      id: this.nextChangelogId++,
      kind: 'result',
      payload: result,
    };
    this.appendChangelog(entry);
    return true;
  }

  recordLate(rec: LateRecord): void {
    const entry: ChangelogEntry = {
      id: this.nextChangelogId++,
      kind: 'late',
      payload: rec,
    };
    this.appendChangelog(entry);
    this.lateRecords.push({ id: this.nextLateId++, record: rec });
    if (this.lateRecords.length > LATE_CAP) {
      this.lateRecords.splice(0, this.lateRecords.length - LATE_CAP);
    }
  }

  private appendChangelog(entry: ChangelogEntry): void {
    this.changelog.push(entry);
    if (this.changelog.length > CHANGELOG_CAP) {
      this.changelog.splice(0, this.changelog.length - CHANGELOG_CAP);
    }
    for (const sub of this.subscribers) {
      try {
        sub(entry);
      } catch {
        /* 订阅者异常不影响主管线 */
      }
    }
  }

  subscribe(fn: (entry: ChangelogEntry) => void): () => void {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  /** SSE 断线重连:返回 id 大于 lastId 的 changelog 条目 */
  since(lastId: number): ChangelogEntry[] {
    return this.changelog.filter((e) => e.id > lastId);
  }

  get changelogTail(): ChangelogEntry[] {
    return this.changelog;
  }

  get latestLate(): Array<{ id: number; record: LateRecord }> {
    return this.lateRecords;
  }

  allResults(): WindowResult<string, Record<string, number>>[] {
    return [...this.current.values()];
  }

  /** 看板窗口结果页:按算子过滤 */
  resultsByOperator(operator: string): WindowResult<string, Record<string, number>>[] {
    return this.allResults()
      .filter((r) => r.operator === operator)
      .sort((a, b) => a.windowStart - b.windowStart || a.regionId.localeCompare(b.regionId));
  }

  get resultCount(): number {
    return this.current.size;
  }

  snapshot(): ResultStoreSnapshot {
    return {
      current: Object.fromEntries(this.current),
      changelogSeq: this.changelog.length,
      nextChangelogId: this.nextChangelogId,
      lateSeq: this.lateRecords.length,
    };
  }

  restore(s: ResultStoreSnapshot): void {
    this.current = new Map(Object.entries(s.current));
    this.nextChangelogId = s.nextChangelogId;
    // changelog 是瞬态展示数据,恢复后从空继续(当前值表才是权威状态)
    this.changelog = [];
    this.lateRecords = [];
    this.nextLateId = 1;
  }
}
