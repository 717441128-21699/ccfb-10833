/** 时间与窗口的纯函数工具 */

/** 滚动窗口起点(对齐到 epoch) */
export function tumbleStart(t: number, sizeMs: number): number {
  return Math.floor(t / sizeMs) * sizeMs;
}

/** 滑动窗口:返回事件时间 t 命中的所有窗口起点 */
export function slidingStarts(t: number, sizeMs: number, slideMs: number): number[] {
  const out: number[] = [];
  // 最近一个不晚于 t 的滑动起点
  const lastStart = Math.floor(t / slideMs) * slideMs;
  for (let s = lastStart; s > t - sizeMs; s -= slideMs) {
    if (s < 0) break;
    out.push(s);
  }
  return out;
}

/**
 * 本地日历日的 UTC 毫秒边界。
 * offsetMinutes:本地时区相对 UTC 的偏移(中国 +480)。
 * 例如东八区 2026-11-15 00:00 对应 UTC 2026-11-14 16:00。
 */
export function localDayStart(t: number, offsetMinutes: number): number {
  const offsetMs = offsetMinutes * 60_000;
  const local = t + offsetMs;
  const dayMs = 86_400_000;
  const dayStartLocal = Math.floor(local / dayMs) * dayMs;
  return dayStartLocal - offsetMs;
}

/** 把 [start,end) 区间按本地日边界切成若干段 */
export function splitByLocalDay(
  start: number,
  end: number,
  offsetMinutes: number,
): Array<{ start: number; end: number; dayStart: number }> {
  const out: Array<{ start: number; end: number; dayStart: number }> = [];
  let cursor = start;
  while (cursor < end) {
    const dayStart = localDayStart(cursor, offsetMinutes);
    const nextDay = dayStart + 86_400_000;
    const segEnd = Math.min(end, nextDay);
    out.push({ start: cursor, end: segEnd, dayStart });
    cursor = segEnd;
  }
  return out;
}

/** 确定性结果 ID:同一窗口同一指标永远映射到同一个 resultId */
export function buildResultId(parts: Array<string | number>): string {
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;
  const s = parts.join('|');
  for (let i = 0; i < s.length; i++) {
    const ch = s.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  const h = (4294967296 * (2097151 & h2) + (h1 >>> 0)) >>> 0;
  return 'r_' + h.toString(36).padStart(7, '0');
}

export function formatClock(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return '-';
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${String(
    d.getMilliseconds(),
  ).padStart(3, '0')}`;
}
