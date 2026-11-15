/**
 * 司机在线会话算子(需求 7)。
 *
 * 职责:
 *  1. 按 driverId 把 DRIVER_ONLINE / DRIVER_OFFLINE 拼成会话 [online, offline)
 *     - 乱序:先到 OFFLINE 再到 ONLINE 时缓存 OFFLINE,ONLINE 到达后闭合
 *     - 重复 ONLINE:以最早的事件时间为准(去重算子已挡掉同 eventId,这里再防同类型重放)
 *     - 未闭合的 ONLINE 作为开放会话保留,水位线推进时按 watermark 估算在线时长
 *  2. 每个闭合/更新的会话按本地日边界切开(默认东八区),跨天部分各自归属当天
 *  3. 把在线时长摊到会话覆盖的每一分钟滚动窗口,输出 regionId 粒度的 online_ms 指标,
 *     与接单率窗口对齐,看板可以同屏对比
 *  4. EOS 时把开放会话按最后水位线闭合
 *
 * 迟到修正:与窗口算子一致,已发出的分钟窗收到新的会话贡献时按 version 递增重发。
 */
import type { EngineConfig } from '../config.ts';
import type {
  LateRecord,
  StreamEvent,
  WindowResult,
} from '../types.ts';
import { buildResultId, splitByLocalDay } from '../util/time.ts';
import type { OperatorCallbacks } from './window.ts';

interface DriverSession {
  driverId: string;
  regionId: string;
  cityId: string;
  onlineAt: number | null;
  offlineAt: number | null;
  /** 乱序到达的 OFFLINE 缓存 */
  pendingOffline: number | null;
}

interface DurationPane {
  windowStart: number;
  windowEnd: number;
  onlineMs: number;
  fired: boolean;
  version: number;
  /** 已计入该窗的会话片段 key -> 时长,防止同一片段重复累加(迟到重建场景) */
  contributions: Array<[string, number]>;
}

export interface DriverOperatorSnapshot {
  sessions: Record<string, DriverSession>;
  panes: Record<string, DurationPane>;
  watermark: number;
  stats: { sessionsOpened: number; sessionsClosed: number; corrected: number; lateDropped: number };
}

const MINUTE = 60_000;

export class DriverSessionOperator {
  readonly name = 'driver-online-duration';
  private sessions = new Map<string, DriverSession>();
  /** key = regionId + '@' + windowStart */
  private panes = new Map<string, DurationPane>();
  private watermark = Number.NEGATIVE_INFINITY;
  readonly stats = {
    sessionsOpened: 0,
    sessionsClosed: 0,
    corrected: 0,
    lateDropped: 0,
  };

  private readonly cfg: EngineConfig;
  private readonly cb: OperatorCallbacks;

  constructor(cfg: EngineConfig, cb: OperatorCallbacks) {
    this.cfg = cfg;
    this.cb = cb;
  }

  processEvent(event: StreamEvent): void {
    if (event.type !== 'DRIVER_ONLINE' && event.type !== 'DRIVER_OFFLINE') return;

    // 超过宽限的司机事件无法再影响已结账窗口,留痕丢弃
    const windowEnd = Math.floor(event.eventTime / MINUTE) * MINUTE + MINUTE;
    if (
      Number.isFinite(this.watermark) &&
      this.watermark > windowEnd + this.cfg.windows.allowedLatenessMs
    ) {
      this.stats.lateDropped++;
      const rec: LateRecord = {
        eventId: event.eventId,
        eventTime: event.eventTime,
        cityId: event.cityId,
        regionId: event.regionId,
        watermarkAtArrival: this.watermark,
        behindMs: this.watermark - event.eventTime,
        reason: 'dropped-expired',
        windowStart: windowEnd - MINUTE,
        windowEnd,
        operator: this.name,
        seq: event.seq,
      };
      this.cb.late(rec);
      return;
    }

    let s = this.sessions.get(event.subjectId);
    if (!s) {
      s = {
        driverId: event.subjectId,
        regionId: event.regionId,
        cityId: event.cityId,
        onlineAt: null,
        offlineAt: null,
        pendingOffline: null,
      };
      this.sessions.set(event.subjectId, s);
    }

    if (event.type === 'DRIVER_ONLINE') {
      if (s.onlineAt === null || event.eventTime < s.onlineAt) {
        if (s.onlineAt === null) {
          s.onlineAt = event.eventTime;
          this.stats.sessionsOpened++;
        } else {
          s.onlineAt = Math.min(s.onlineAt, event.eventTime);
        }
        // 之前缓存了乱序 OFFLINE,现在可以闭合
        const onlineAt = s.onlineAt;
        const pending = s.pendingOffline;
        if (pending !== null && pending > onlineAt) {
          s.offlineAt = pending;
          s.pendingOffline = null;
          this.closeSession(s);
        }
      }
    } else {
      // DRIVER_OFFLINE
      if (s.onlineAt === null) {
        // 乱序:ONLINE 还没到,缓存(取最晚的离线时间)
        s.pendingOffline =
          s.pendingOffline === null
            ? event.eventTime
            : Math.max(s.pendingOffline, event.eventTime);
      } else if (s.offlineAt === null || event.eventTime > s.offlineAt) {
        s.offlineAt = event.eventTime;
        this.closeSession(s);
      }
    }
  }

  /** 把一个闭合会话按天切开,再摊到每分钟窗口 */
  private closeSession(s: DriverSession): void {
    if (s.onlineAt === null || s.offlineAt === null) return;
    if (s.offlineAt <= s.onlineAt) return;
    this.stats.sessionsClosed++;
    const daySegs = splitByLocalDay(
      s.onlineAt,
      s.offlineAt,
      this.cfg.dayBoundaryOffsetMinutes,
    );
    for (const seg of daySegs) {
      this.distribute(s, seg.start, seg.end, seg.dayStart);
    }
    // 会话已结账,删除司机状态(后续同司机的 ONLINE 会开启新会话)
    this.sessions.delete(s.driverId);
  }

  /** 把 [start,end) 的在线时长摊到覆盖的每分钟窗,片段 key 保证幂等累加 */
  private distribute(
    s: DriverSession,
    start: number,
    end: number,
    dayStart: number,
  ): void {
    const fragmentKey = `${s.driverId}|${start}|${end}|${dayStart}`;
    let cursor = start;
    while (cursor < end) {
      const wStart = Math.floor(cursor / MINUTE) * MINUTE;
      const wEnd = wStart + MINUTE;
      const segEnd = Math.min(end, wEnd);
      const delta = segEnd - cursor;
      const key = `${s.regionId}@${wStart}`;
      let pane = this.panes.get(key);
      if (!pane) {
        pane = {
          windowStart: wStart,
          windowEnd: wEnd,
          onlineMs: 0,
          fired: false,
          version: 0,
          contributions: [],
        };
        this.panes.set(key, pane);
      }
      const prev = pane.contributions.find(([k]) => k === fragmentKey)?.[1] ?? 0;
      pane.onlineMs += delta - prev;
      const existing = pane.contributions.find(([k]) => k === fragmentKey);
      if (existing) existing[1] = delta;
      else pane.contributions.push([fragmentKey, delta]);
      if (pane.fired) {
        pane.version++;
        this.emit(pane, s.regionId, true);
        this.stats.corrected++;
      }
      cursor = segEnd;
    }
  }

  processWatermark(wm: number): void {
    if (wm <= this.watermark) return;
    this.watermark = wm;
    for (const [key, pane] of [...this.panes]) {
      if (!pane.fired && wm >= pane.windowEnd) {
        pane.fired = true;
        pane.version = 1;
        this.emit(pane, key.slice(0, key.indexOf('@')), false);
      }
      if (pane.fired && wm > pane.windowEnd + this.cfg.windows.allowedLatenessMs) {
        this.panes.delete(key);
      }
    }
  }

  private emit(pane: DurationPane, regionId: string, correction: boolean): void {
    const result: WindowResult<string, Record<string, number>> = {
      resultId: buildResultId([
        this.name,
        regionId,
        'tumble',
        pane.windowStart,
        pane.windowEnd,
        'online_ms',
      ]),
      operator: this.name,
      windowType: 'tumble',
      regionId,
      key: regionId,
      windowStart: pane.windowStart,
      windowEnd: pane.windowEnd,
      metric: 'online_ms',
      value: {
        onlineMs: pane.onlineMs,
        onlineSeconds: Math.round(pane.onlineMs / 100) / 10,
      },
      version: pane.version,
      emittedAtWatermark: this.watermark,
      correction,
    };
    this.cb.emit(result);
  }

  /** 看板:开放会话(在线但尚未收到 OFFLINE) */
  openSessions(): DriverSession[] {
    return [...this.sessions.values()].filter((s) => s.onlineAt !== null);
  }

  activePaneCount(): number {
    return this.panes.size;
  }

  flush(): void {
    // EOS:开放会话用最后水位线闭合
    for (const s of [...this.sessions.values()]) {
      if (s.onlineAt !== null) {
        s.offlineAt = Math.max(
          this.watermark === Number.NEGATIVE_INFINITY ? s.onlineAt : this.watermark,
          s.onlineAt + 1,
        );
        this.closeSession(s);
      }
    }
    for (const [key, pane] of [...this.panes]) {
      if (!pane.fired) {
        pane.fired = true;
        pane.version = 1;
        this.emit(pane, key.slice(0, key.indexOf('@')), false);
      }
    }
  }

  snapshot(): DriverOperatorSnapshot {
    const panes: Record<string, DurationPane> = {};
    for (const [k, p] of this.panes) panes[k] = { ...p };
    return {
      sessions: Object.fromEntries(this.sessions),
      panes,
      watermark: this.watermark,
      stats: { ...this.stats },
    };
  }

  restore(s: DriverOperatorSnapshot): void {
    this.sessions = new Map(Object.entries(s.sessions).map(([k, v]) => [k, { ...v }]));
    this.panes = new Map();
    for (const [k, p] of Object.entries(s.panes)) {
      this.panes.set(k, { ...p, contributions: [...p.contributions] });
    }
    this.watermark = s.watermark;
    Object.assign(this.stats, s.stats);
  }
}
