/**
 * 分城市水位线推进器(热路径 O(log 城市数),不做全城市扫描)。
 *
 * 语义:
 *  - 每个城市各自维护 maxEventTime,城市水位线 = maxEventTime - outOfOrderBound
 *  - 全局水位线 = 所有"活跃"城市水位线的最小值(取 min 保证不会有城市的事件被过早判迟到)
 *  - 空闲判定基于事件时间:某城市 maxEventTime 落后全局 maxEventTime 超过 idleTimeout
 *    即视为空闲,从全局 min 集合中剔除 —— 一个城市卡住不会拖累其他城市
 *  - 活跃城市水位线用最小堆维护,事件只更新所属城市;空闲状态在看板查询时惰性计算,
 *    不在每个事件上扫描全部城市
 *  - 仅当全局水位线前进至少 intervalMs 时才向下游发一条 watermark 元素
 *
 * 不使用任何墙钟,因此实时与回放(任意倍速)产生完全一致的水位线序列。
 */
import type { EngineConfig } from '../config.ts';
import type { StreamEvent, WatermarkElement } from '../types.ts';
import { MinHeap } from '../util/heap.ts';

interface CityState {
  maxEventTime: number;
  watermark: number;
  events: number;
}

interface HeapEntry {
  cityId: string;
  watermark: number;
  /** 城市状态版本:水位线每次推进自增,用于丢弃堆中过期条目(惰性删除) */
  version: number;
}

export interface WatermarkAssignerSnapshot {
  cities: Record<string, CityState>;
  idle: Record<string, boolean>;
  globalMaxEventTime: number;
  lastEmittedWatermark: number;
  emittedCount: number;
}

export class WatermarkAssigner {
  private cities = new Map<string, CityState>();
  private versions = new Map<string, number>();
  /** 活跃城市水位线最小堆,可能含过期版本,取顶时惰性清理 */
  private heap: MinHeap<HeapEntry>;
  private globalMaxEventTime = Number.NEGATIVE_INFINITY;
  private lastEmittedWatermark = Number.NEGATIVE_INFINITY;
  emittedCount = 0;
  private readonly cfg: EngineConfig;

  constructor(cfg: EngineConfig) {
    this.cfg = cfg;
    this.heap = new MinHeap<HeapEntry>((x, y) =>
      x.watermark < y.watermark || (x.watermark === y.watermark && x.cityId < y.cityId),
    );
  }

  processEvent(event: StreamEvent): WatermarkElement | null {
    const { outOfOrderBoundMs, intervalMs, idleTimeoutMs } = this.cfg.watermark;
    let st = this.cities.get(event.cityId);
    if (!st) {
      st = { maxEventTime: Number.NEGATIVE_INFINITY, watermark: Number.NEGATIVE_INFINITY, events: 0 };
      this.cities.set(event.cityId, st);
      this.versions.set(event.cityId, 0);
    }
    st.events++;

    if (event.eventTime > st.maxEventTime) st.maxEventTime = event.eventTime;
    if (event.eventTime > this.globalMaxEventTime) this.globalMaxEventTime = event.eventTime;

    const newWm = st.maxEventTime - outOfOrderBoundMs;
    if (newWm > st.watermark) {
      st.watermark = newWm;
      const v = (this.versions.get(event.cityId) ?? 0) + 1;
      this.versions.set(event.cityId, v);
      // 只在城市仍活跃时入堆;卡住城市不再贡献全局 min
      const isIdle = this.globalMaxEventTime - st.maxEventTime > idleTimeoutMs;
      if (!isIdle) this.heap.push({ cityId: event.cityId, watermark: newWm, version: v });
    }

    const globalWm = this.activeMinWatermark();
    if (globalWm === Number.POSITIVE_INFINITY) return null;
    const effectiveWm = Math.max(globalWm, this.lastEmittedWatermark);

    if (effectiveWm - this.lastEmittedWatermark >= intervalMs) {
      this.lastEmittedWatermark = effectiveWm;
      this.emittedCount++;
      return {
        kind: 'watermark',
        globalWatermark: effectiveWm,
        cityWatermarks: this.cityWatermarkSnapshot(),
      };
    }
    return null;
  }

  /** 堆顶惰性清理过期/已空闲条目,返回当前活跃城市最小水位线 */
  private activeMinWatermark(): number {
    const { idleTimeoutMs } = this.cfg.watermark;
    for (;;) {
      const top = this.heap.peek();
      if (!top) return Number.POSITIVE_INFINITY;
      const st = this.cities.get(top.cityId);
      const ver = this.versions.get(top.cityId) ?? 0;
      const stale =
        !st ||
        top.version !== ver ||
        top.watermark !== st.watermark ||
        this.globalMaxEventTime - st.maxEventTime > idleTimeoutMs;
      if (!stale) return top.watermark;
      this.heap.pop();
    }
  }

  private isCityIdle(id: string, st: CityState): boolean {
    if (st.maxEventTime === Number.NEGATIVE_INFINITY) return true;
    return this.globalMaxEventTime - st.maxEventTime > this.cfg.watermark.idleTimeoutMs;
  }

  cityWatermarkSnapshot(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [id, c] of this.cities) {
      out[id] = this.isCityIdle(id, c) ? Number.NEGATIVE_INFINITY : c.watermark;
    }
    return out;
  }

  /** 当前各城市状态(看板拓扑页展示卡顿/空闲),空闲标记惰性计算 */
  cityStates(): Record<string, CityState & { idle: boolean }> {
    const out: Record<string, CityState & { idle: boolean }> = {};
    for (const [id, c] of this.cities) {
      out[id] = { ...c, idle: this.isCityIdle(id, c) };
    }
    return out;
  }

  get currentGlobalWatermark(): number {
    return this.lastEmittedWatermark === Number.NEGATIVE_INFINITY
      ? Number.NEGATIVE_INFINITY
      : this.lastEmittedWatermark;
  }

  snapshot(): WatermarkAssignerSnapshot {
    const cities: Record<string, CityState> = {};
    const idle: Record<string, boolean> = {};
    for (const [id, c] of this.cities) {
      cities[id] = { ...c };
      idle[id] = this.isCityIdle(id, c);
    }
    return {
      cities,
      idle,
      globalMaxEventTime: this.globalMaxEventTime,
      lastEmittedWatermark: this.lastEmittedWatermark,
      emittedCount: this.emittedCount,
    };
  }

  restore(s: WatermarkAssignerSnapshot): void {
    this.cities = new Map(Object.entries(s.cities).map(([id, c]) => [id, { ...c }]));
    this.globalMaxEventTime = s.globalMaxEventTime;
    this.lastEmittedWatermark = s.lastEmittedWatermark;
    this.emittedCount = s.emittedCount;
    // 重建堆与版本号
    this.heap = new MinHeap<HeapEntry>((x, y) =>
      x.watermark < y.watermark || (x.watermark === y.watermark && x.cityId < y.cityId),
    );
    this.versions = new Map();
    for (const [id, c] of this.cities) {
      this.versions.set(id, 1);
      const isIdle = s.idle[id] ?? this.isCityIdle(id, c);
      if (!isIdle && Number.isFinite(c.watermark)) {
        this.heap.push({ cityId: id, watermark: c.watermark, version: 1 });
      }
    }
  }
}
