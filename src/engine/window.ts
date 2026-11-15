/**
 * 通用事件时间窗口算子,支持滚动 / 滑动 / 会话三种窗口。
 *
 * 关键语义:
 *  - 窗口在 watermark >= windowEnd 时首次触发(version=1)
 *  - allowedLateness 内到达的迟到事件合并进已触发窗口,以递增 version 再次发出修正结果
 *  - 超过 allowedLateness 的事件直接丢弃并记录 LateRecord(reason=dropped-expired)
 *  - 会话窗口按 key 维护不重叠区间,新事件触发相邻会话合并,旧 pane 的累加器并入新 pane
 *  - 所有触发只由 watermark 驱动(纯事件时间),回放与实时输出序列一致
 */
import type { EngineConfig } from '../config.ts';
import type {
  LateRecord,
  StreamEvent,
  WindowResult,
} from '../types.ts';
import { buildResultId } from '../util/time.ts';

export interface Window {
  start: number;
  end: number;
}

export interface AggregateFunction<ACC> {
  create(): ACC;
  add(acc: ACC, event: StreamEvent): void;
  merge(a: ACC, b: ACC): ACC;
  /** 结果指标名与值(值为对象,可同时输出计数和派生率) */
  metric: string;
  value(acc: ACC): Record<string, number>;
}

export interface WindowAssigner {
  readonly windowType: 'tumble' | 'sliding' | 'session';
  /**
   * 返回目标窗口列表。
   * 对滚动/滑动:一个事件命中的所有窗口,absorbed 恒为空。
   * 对会话:只有一个目标窗口(合并后的会话),absorbed 为被它吸收的旧会话区间。
   */
  assign(event: StreamEvent): { windows: Window[]; absorbed: Window[] };
  snapshot(): unknown;
  restore(state: unknown): void;
}

/** 滚动窗口:无状态,纯对齐计算 */
export class TumbleAssigner implements WindowAssigner {
  readonly windowType = 'tumble' as const;
  private readonly sizeMs: number;

  constructor(sizeMs: number) {
    this.sizeMs = sizeMs;
  }
  assign(event: StreamEvent): { windows: Window[]; absorbed: Window[] } {
    const start = Math.floor(event.eventTime / this.sizeMs) * this.sizeMs;
    return { windows: [{ start, end: start + this.sizeMs }], absorbed: [] };
  }
  snapshot(): unknown {
    return null;
  }
  restore(): void {}
}

/** 滑动窗口:一个事件可同时落入多个重叠窗口 */
export class SlidingAssigner implements WindowAssigner {
  readonly windowType = 'sliding' as const;
  private readonly sizeMs: number;
  private readonly slideMs: number;

  constructor(sizeMs: number, slideMs: number) {
    this.sizeMs = sizeMs;
    this.slideMs = slideMs;
  }
  assign(event: StreamEvent): { windows: Window[]; absorbed: Window[] } {
    const windows: Window[] = [];
    const lastStart = Math.floor(event.eventTime / this.slideMs) * this.slideMs;
    for (let s = lastStart; s > event.eventTime - this.sizeMs; s -= this.slideMs) {
      if (s < 0) break;
      windows.push({ start: s, end: s + this.sizeMs });
    }
    return { windows, absorbed: [] };
  }
  snapshot(): unknown {
    return null;
  }
  restore(): void {}
}

/** 会话窗口:按 key 维护会话区间,支持事件时间乱序下的动态合并 */
export class SessionAssigner implements WindowAssigner {
  readonly windowType = 'session' as const;
  private sessions = new Map<string, Window[]>();
  private readonly gapMs: number;

  constructor(gapMs: number) {
    this.gapMs = gapMs;
  }

  assignForKey(key: string, t: number): { windows: Window[]; absorbed: Window[] } {
    const existing = this.sessions.get(key) ?? [];
    // 找到所有与 [t-gap, t+gap] 相邻/重叠的会话
    const touching: Window[] = [];
    const remain: Window[] = [];
    for (const w of existing) {
      if (t >= w.start - this.gapMs && t <= w.end + this.gapMs) touching.push(w);
      else remain.push(w);
    }
    let merged: Window;
    if (touching.length === 0) {
      merged = { start: t, end: t + 1 };
    } else {
      let start = t;
      let end = t + 1;
      for (const w of touching) {
        start = Math.min(start, w.start);
        end = Math.max(end, w.end);
      }
      merged = { start, end };
    }
    remain.push(merged);
    remain.sort((a, b) => a.start - b.start);
    this.sessions.set(key, remain);
    return { windows: [merged], absorbed: touching };
  }

  assign(event: StreamEvent): { windows: Window[]; absorbed: Window[] } {
    return this.assignForKey(event.regionId, event.eventTime);
  }

  snapshot(): unknown {
    return Object.fromEntries(this.sessions);
  }

  restore(state: unknown): void {
    this.sessions = new Map(Object.entries(state as Record<string, Window[]>));
  }
}

interface Pane<ACC> {
  window: Window;
  acc: ACC;
  fired: boolean;
  version: number;
  emittedWatermark: number;
}

interface PaneIndex<ACC> {
  byId: Map<string, Pane<ACC>>;
}

export interface OperatorCallbacks {
  emit(result: WindowResult<string, Record<string, number>>): void;
  late(record: LateRecord): void;
}

export interface WindowOperatorSnapshot {
  name: string;
  watermark: number;
  assigner: unknown;
  panes: Record<
    string,
    Array<{
      id: string;
      window: Window;
      acc: unknown;
      fired: boolean;
      version: number;
      emittedWatermark: number;
    }>
  >;
  stats: { fired: number; corrected: number; lateMerged: number; lateDropped: number };
}

export class WindowOperator<ACC> {
  private watermark = Number.NEGATIVE_INFINITY;
  private panes = new Map<string, PaneIndex<ACC>>();
  readonly stats = { fired: 0, corrected: 0, lateMerged: 0, lateDropped: 0 };

  readonly name: string;
  private readonly assigner: WindowAssigner;
  private readonly agg: AggregateFunction<ACC>;
  private readonly cfg: EngineConfig;
  private readonly cb: OperatorCallbacks;

  constructor(
    name: string,
    assigner: WindowAssigner,
    agg: AggregateFunction<ACC>,
    cfg: EngineConfig,
    cb: OperatorCallbacks,
  ) {
    this.name = name;
    this.assigner = assigner;
    this.agg = agg;
    this.cfg = cfg;
    this.cb = cb;
  }

  private keyIndex(regionId: string): PaneIndex<ACC> {
    let idx = this.panes.get(regionId);
    if (!idx) {
      idx = { byId: new Map() };
      this.panes.set(regionId, idx);
    }
    return idx;
  }

  private paneId(w: Window): string {
    return `${w.start}..${w.end}`;
  }

  processEvent(event: StreamEvent): void {
    const { windows, absorbed } = this.assigner.assign(event);
    const idx = this.keyIndex(event.regionId);

    // 会话窗口:windows 只有一个合并后的目标窗口;absorbed 是被吸收的旧会话
    if (this.assigner.windowType === 'session') {
      const target = windows[0]!;
      const targetId = this.paneId(target);
      let pane = idx.byId.get(targetId);
      if (!pane) {
        pane = {
          window: target,
          acc: this.agg.create(),
          fired: false,
          version: 0,
          emittedWatermark: Number.NEGATIVE_INFINITY,
        };
        idx.byId.set(targetId, pane);
      } else {
        pane.window = target;
      }
      let absorbedFired = false;
      for (const w of absorbed) {
        const old = idx.byId.get(this.paneId(w));
        if (old) {
          pane.acc = this.agg.merge(pane.acc, old.acc);
          if (old.fired) absorbedFired = true;
          idx.byId.delete(this.paneId(w));
        }
      }
      this.addToPane(pane, event);
      // 合并了已经发出过结果的旧会话:目标窗口以修正帧重发,下游用 resultId 替换旧会话结果
      if (absorbedFired && !pane.fired) {
        pane.fired = true;
        pane.version = 1;
        pane.emittedWatermark = this.watermark;
        this.emitPane(pane, event.regionId, true);
        this.stats.corrected++;
      }
      return;
    }

    for (const w of windows) {
      const id = this.paneId(w);
      let pane = idx.byId.get(id);
      const expired =
        Number.isFinite(this.watermark) &&
        this.watermark > w.end + this.cfg.windows.allowedLatenessMs;
      if (expired) {
        this.stats.lateDropped++;
        this.cb.late({
          eventId: event.eventId,
          eventTime: event.eventTime,
          cityId: event.cityId,
          regionId: event.regionId,
          watermarkAtArrival: this.watermark,
          behindMs: this.watermark - event.eventTime,
          reason: 'dropped-expired',
          windowStart: w.start,
          windowEnd: w.end,
          operator: this.name,
          seq: event.seq,
        });
        continue;
      }
      if (!pane) {
        pane = {
          window: w,
          acc: this.agg.create(),
          fired: false,
          version: 0,
          emittedWatermark: Number.NEGATIVE_INFINITY,
        };
        idx.byId.set(id, pane);
      }
      const wasLate = pane.fired;
      this.addToPane(pane, event);
      if (wasLate) {
        this.stats.lateMerged++;
        this.cb.late({
          eventId: event.eventId,
          eventTime: event.eventTime,
          cityId: event.cityId,
          regionId: event.regionId,
          watermarkAtArrival: this.watermark,
          behindMs: this.watermark - event.eventTime,
          reason: 'merged-with-correction',
          windowStart: w.start,
          windowEnd: w.end,
          operator: this.name,
          seq: event.seq,
        });
      }
    }
  }

  private addToPane(pane: Pane<ACC>, event: StreamEvent): void {
    this.agg.add(pane.acc, event);
    // 已触发窗口收到迟到事件:立刻发出修正(不等下一个 watermark)
    if (pane.fired) {
      pane.version++;
      this.emitPane(pane, event.regionId, true);
      this.stats.corrected++;
    }
  }

  processWatermark(wm: number): void {
    if (wm <= this.watermark) return;
    this.watermark = wm;
    for (const [regionId, idx] of this.panes) {
      for (const [id, pane] of [...idx.byId]) {
        if (!pane.fired && wm >= pane.window.end) {
          pane.fired = true;
          pane.version = 1;
          pane.emittedWatermark = wm;
          this.emitPane(pane, regionId, false);
          this.stats.fired++;
        }
        // 窗口迟到宽限到期:回收状态
        if (pane.fired && wm > pane.window.end + this.cfg.windows.allowedLatenessMs) {
          idx.byId.delete(id);
        }
      }
    }
  }

  private emitPane(
    pane: Pane<ACC>,
    regionId: string,
    correction: boolean,
  ): void {
    const result: WindowResult<string, Record<string, number>> = {
      resultId: buildResultId([
        this.name,
        regionId,
        this.assigner.windowType,
        pane.window.start,
        pane.window.end,
        this.agg.metric,
      ]),
      operator: this.name,
      windowType: this.assigner.windowType,
      regionId,
      key: regionId,
      windowStart: pane.window.start,
      windowEnd: pane.window.end,
      metric: this.agg.metric,
      value: this.agg.value(pane.acc),
      version: pane.version,
      emittedAtWatermark: pane.emittedWatermark === Number.NEGATIVE_INFINITY
        ? this.watermark
        : pane.emittedWatermark,
      correction,
    };
    this.cb.emit(result);
  }

  /** EOS:把所有残留窗口(含未触发)闭合发出 */
  flush(): void {
    for (const [regionId, idx] of this.panes) {
      for (const pane of idx.byId.values()) {
        if (!pane.fired) {
          pane.fired = true;
          pane.version = 1;
          pane.emittedWatermark = this.watermark;
          this.emitPane(pane, regionId, false);
          this.stats.fired++;
        }
      }
    }
  }

  /** 看板:当前各区域存活 pane 数量与状态 */
  activePaneCount(): number {
    let n = 0;
    for (const idx of this.panes.values()) n += idx.byId.size;
    return n;
  }

  snapshot(): WindowOperatorSnapshot {
    const panes: WindowOperatorSnapshot['panes'] = {};
    for (const [regionId, idx] of this.panes) {
      panes[regionId] = [...idx.byId.entries()].map(([id, p]) => ({
        id,
        window: p.window,
        acc: p.acc as unknown,
        fired: p.fired,
        version: p.version,
        emittedWatermark: p.emittedWatermark,
      }));
    }
    return {
      name: this.name,
      watermark: this.watermark,
      assigner: this.assigner.snapshot(),
      panes,
      stats: { ...this.stats },
    };
  }

  restore(s: WindowOperatorSnapshot, agg: AggregateFunction<ACC>): void {
    this.watermark = s.watermark;
    this.assigner.restore(s.assigner);
    this.panes = new Map();
    for (const [regionId, entries] of Object.entries(s.panes)) {
      const idx: PaneIndex<ACC> = { byId: new Map() };
      for (const e of entries) {
        idx.byId.set(e.id, {
          window: e.window,
          acc: e.acc as ACC,
          fired: e.fired,
          version: e.version,
          emittedWatermark: e.emittedWatermark,
        });
      }
      this.panes.set(regionId, idx);
    }
    Object.assign(this.stats, s.stats);
    void agg;
  }
}
