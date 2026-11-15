/**
 * 水位线、去重、窗口迟到修正的算子级测试。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_CONFIG, type EngineConfig } from '../src/config.ts';
import type { LateRecord, StreamEvent, WindowResult } from '../src/types.ts';
import { WatermarkAssigner } from '../src/engine/watermark.ts';
import { DedupOperator } from '../src/engine/dedup.ts';
import { TumbleAssigner, WindowOperator } from '../src/engine/window.ts';
import { OrderCountersAggregate } from '../src/engine/aggregates.ts';

function cfg(): EngineConfig {
  const c = structuredClone(DEFAULT_CONFIG);
  c.watermark.intervalMs = 1; // 测试中每个推进都广播
  c.checkpoint.dir = 'data/test-checkpoints';
  return c;
}

function ev(over: Partial<StreamEvent>): StreamEvent {
  return {
    eventId: `e${Math.round(over.eventTime ?? 0)}`,
    type: 'ORDER_CREATED',
    eventTime: 0,
    cityId: 'city_000',
    regionId: 'r',
    subjectId: 's',
    seq: 1,
    deliveryId: 1,
    ...over,
  };
}

test('分城市水位线:卡住城市不拖累其他城市', () => {
  const c = cfg();
  const wm = new WatermarkAssigner(c);
  wm.processEvent(ev({ cityId: 'A', eventTime: 10_000 }));
  wm.processEvent(ev({ cityId: 'B', eventTime: 10_000 }));
  // B 持续推进,A 卡住
  let last = -1;
  for (let t = 11_000; t < 60_000; t += 1_000) {
    const out = wm.processEvent(ev({ cityId: 'B', eventTime: t }));
    if (out) last = out.globalWatermark;
  }
  assert.ok(last > 30_000, `全局水位线应随 B 推进,实际 ${last}`);
  const states = wm.cityStates();
  assert.equal(states['A']!.idle, true);
  assert.equal(states['B']!.idle, false);
});

test('空闲城市恢复后水位线单调不减', () => {
  const c = cfg();
  const wm = new WatermarkAssigner(c);
  wm.processEvent(ev({ cityId: 'A', eventTime: 10_000 }));
  for (let t = 11_000; t < 60_000; t += 1_000) {
    wm.processEvent(ev({ cityId: 'A', eventTime: t }));
  }
  const before = wm.currentGlobalWatermark;
  // 新城市首次出现,不应把全局水位线拉低
  const out = wm.processEvent(ev({ cityId: 'NEW', eventTime: 12_000 }));
  if (out) assert.ok(out.globalWatermark >= before);
  assert.ok(wm.currentGlobalWatermark >= before);
});

test('入口去重:同 eventId 只放行一次,超容量后最老 ID 被回收', () => {
  const c = cfg();
  const dedup = new DedupOperator(c);
  const e = ev({ eventTime: 5_000, eventId: 'dup-1' });
  assert.equal(dedup.process(e, Number.NEGATIVE_INFINITY), true);
  assert.equal(dedup.process({ ...e }, Number.NEGATIVE_INFINITY), false);
  assert.equal(dedup.duplicatesDropped, 1);

  // 喂入超过保留容量的事件,触发最老项淘汰
  const cap = dedup.capacity;
  for (let i = 0; i < cap + 100; i++) {
    dedup.process(
      ev({ eventTime: 5_000 + i, eventId: `flow-${i}`, deliveryId: 100 + i }),
      6_000 + i,
    );
  }
  assert.equal(dedup.retainedIds, cap, '存活 ID 不超过保留容量');
  assert.ok(![...dedup.snapshotEntries()].includes('dup-1'), '最早的 ID 应被回收');
});

test('入口去重:迟到事件的同批重复副本仍被正确识别', () => {
  const c = cfg();
  const dedup = new DedupOperator(c);
  // 先建立正常流量与水位线
  for (let i = 0; i < 5_000; i++) {
    dedup.process(ev({ eventTime: 10_000 + i, eventId: `n-${i}`, deliveryId: i }), 10_000 + i);
  }
  // 一个事件时间很老的迟到事件
  const late = ev({ eventTime: 1_000, eventId: 'late-x', deliveryId: 90_000 });
  assert.equal(dedup.process(late, 60_000), true);
  // 它的重复副本紧接着到达,必须被拦截(按到达顺序保留,而非事件时间)
  assert.equal(dedup.process({ ...late, deliveryId: 90_001 }, 60_000), false);
  assert.equal(dedup.duplicatesDropped, 1);
});

function makeTumble(c: EngineConfig) {
  const emitted: WindowResult<string, Record<string, number>>[] = [];
  const late: LateRecord[] = [];
  const op = new WindowOperator(
    'test-tumble',
    new TumbleAssigner(60_000),
    new OrderCountersAggregate(),
    c,
    { emit: (r) => emitted.push(r), late: (r) => late.push(r) },
  );
  return { op, emitted, late };
}

test('窗口首帧在 watermark >= windowEnd 时触发,迟到事件产生版本递增修正', () => {
  const c = cfg();
  const { op, emitted, late } = makeTumble(c);
  // 窗口 [0,60000) 内两个事件
  op.processEvent(ev({ eventTime: 1_000 }));
  op.processEvent(ev({ eventTime: 2_000, eventId: 'e2', type: 'ORDER_ACCEPTED' }));
  op.processWatermark(59_000);
  assert.equal(emitted.length, 0, '未到窗口结束不触发');
  op.processWatermark(60_000);
  assert.equal(emitted.length, 1);
  assert.equal(emitted[0]!.version, 1);
  assert.equal(emitted[0]!.correction, false);
  assert.deepEqual(emitted[0]!.value.created, 1);

  // 迟到事件(allowedLateness=30s 内)→ 修正
  op.processEvent(ev({ eventTime: 3_000, eventId: 'late-1' }));
  assert.equal(emitted.length, 2);
  assert.equal(emitted[1]!.version, 2);
  assert.equal(emitted[1]!.correction, true);
  assert.equal(emitted[1]!.resultId, emitted[0]!.resultId, '修正复用同一 resultId');
  assert.equal(late[0]!.reason, 'merged-with-correction');

  // 超过宽限 → 丢弃留痕
  op.processWatermark(100_000);
  const dropped = emitted.length;
  op.processEvent(ev({ eventTime: 4_000, eventId: 'late-2' }));
  assert.equal(emitted.length, dropped, '超期事件不再产生结果');
  assert.ok(late.some((r) => r.reason === 'dropped-expired'));
});

test('窗口状态快照恢复后继续产生一致的修正', () => {
  const c = cfg();
  const a = makeTumble(c);
  a.op.processEvent(ev({ eventTime: 1_000 }));
  a.op.processWatermark(60_000);
  const snap = a.op.snapshot();

  const b = makeTumble(c);
  b.op.restore(snap, new OrderCountersAggregate());
  b.op.processEvent(ev({ eventTime: 2_000, eventId: 'late-x' }));
  assert.equal(b.emitted.length, 1);
  assert.equal(b.emitted[0]!.version, 2);
  assert.equal(b.emitted[0]!.resultId, a.emitted[0]!.resultId);
});
