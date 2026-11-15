/**
 * 窗口 assigner 与时间工具测试。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  TumbleAssigner,
  SlidingAssigner,
  SessionAssigner,
} from '../src/engine/window.ts';
import {
  localDayStart,
  slidingStarts,
  splitByLocalDay,
  tumbleStart,
} from '../src/util/time.ts';
import type { StreamEvent } from '../src/types.ts';

function ev(over: Partial<StreamEvent>): StreamEvent {
  return {
    eventId: 'e',
    type: 'ORDER_CREATED',
    eventTime: 0,
    cityId: 'c',
    regionId: 'r',
    subjectId: 's',
    seq: 1,
    deliveryId: 1,
    ...over,
  };
}

test('滚动窗口对齐 epoch 边界', () => {
  const a = new TumbleAssigner(60_000);
  assert.deepEqual(a.assign(ev({ eventTime: 90_000 })).windows, [
    { start: 60_000, end: 120_000 },
  ]);
  assert.equal(tumbleStart(123_456, 60_000), 120_000);
});

test('滑动窗口:一个事件落入多个重叠窗', () => {
  const starts = slidingStarts(65_000, 60_000, 10_000);
  assert.deepEqual(starts, [60_000, 50_000, 40_000, 30_000, 20_000, 10_000]);
  const a = new SlidingAssigner(60_000, 10_000);
  const wins = a.assign(ev({ eventTime: 65_000 })).windows;
  assert.equal(wins.length, 6);
  assert.deepEqual(wins[0], { start: 60_000, end: 120_000 });
});

test('会话窗口:间隔内的事件合并为同一会话', () => {
  const a = new SessionAssigner(300_000);
  const r1 = a.assign(ev({ eventTime: 0, regionId: 'r' }));
  const r2 = a.assign(ev({ eventTime: 100_000, regionId: 'r' }));
  assert.equal(r1.windows[0]!.start, 0);
  assert.equal(r2.windows[0]!.start, 0);
  assert.equal(r2.windows[0]!.end, 100_001);
  assert.equal(r2.absorbed.length, 1, '旧会话被吸收并出现在 absorbed 中');
});

test('会话窗口:超过 gap 的事件开启新会话', () => {
  const a = new SessionAssigner(300_000);
  a.assign(ev({ eventTime: 0, regionId: 'r' }));
  const r2 = a.assign(ev({ eventTime: 400_000, regionId: 'r' }));
  assert.equal(r2.windows.length, 1);
  assert.equal(r2.windows[0]!.start, 400_000);
  assert.equal(r2.absorbed.length, 0);
});

test('会话窗口:乱序事件桥接两个会话后三者合并', () => {
  const a = new SessionAssigner(300_000);
  a.assign(ev({ eventTime: 0, regionId: 'r' }));
  a.assign(ev({ eventTime: 400_000, regionId: 'r' }));
  // 中间事件把两个会话桥接起来
  const r3 = a.assign(ev({ eventTime: 200_000, regionId: 'r' }));
  assert.equal(r3.windows[0]!.start, 0);
  assert.equal(r3.windows[0]!.end, 400_001);
  assert.equal(r3.absorbed.length, 2);
});

test('本地日边界:东八区跨天切开', () => {
  // 2026-11-15 23:30 到 2026-11-16 00:30(东八区)
  const start = Date.UTC(2026, 10, 15, 15, 30);
  const end = Date.UTC(2026, 10, 15, 16, 30);
  const dayStart = localDayStart(start, 480);
  assert.equal(dayStart, Date.UTC(2026, 10, 14, 16, 0)); // 东八区 11-15 00:00
  const segs = splitByLocalDay(start, end, 480);
  assert.equal(segs.length, 2);
  assert.equal(segs[0]!.end - segs[0]!.start, 30 * 60_000);
  assert.equal(segs[1]!.end - segs[1]!.start, 30 * 60_000);
});

test('跨天片段总时长等于原始区间', () => {
  const start = Date.UTC(2026, 10, 14, 20, 0);
  const end = Date.UTC(2026, 10, 17, 5, 0);
  const segs = splitByLocalDay(start, end, 480);
  const total = segs.reduce((acc, s) => acc + (s.end - s.start), 0);
  assert.equal(total, end - start);
});
