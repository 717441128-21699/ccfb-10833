/**
 * 司机在线会话算子测试:拼会话、乱序 OFFLINE、跨天切开、分钟窗修正。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_CONFIG, type EngineConfig } from '../src/config.ts';
import type { StreamEvent } from '../src/types.ts';
import { DriverSessionOperator } from '../src/engine/driver-session.ts';

function cfg(): EngineConfig {
  const c = structuredClone(DEFAULT_CONFIG);
  c.checkpoint.dir = 'data/test-checkpoints';
  return c;
}

function driverEvent(
  type: 'DRIVER_ONLINE' | 'DRIVER_OFFLINE',
  eventTime: number,
  seq: number,
): StreamEvent {
  return {
    eventId: `d-${type}-${seq}`,
    type,
    eventTime,
    cityId: 'city_000',
    regionId: 'r',
    subjectId: 'driver-1',
    seq,
    deliveryId: seq,
  };
}

test('ONLINE+OFFLINE 拼成会话,时长摊到每分钟窗', () => {
  const c = cfg();
  const emitted: unknown[] = [];
  const op = new DriverSessionOperator(c, {
    emit: (r) => emitted.push(r),
    late: () => {},
  });
  op.processEvent(driverEvent('DRIVER_ONLINE', 0, 1));
  op.processEvent(driverEvent('DRIVER_OFFLINE', 90_000, 2));
  op.processWatermark(180_000);

  // 90 秒 = 60s 落在 [0,60000),30s 落在 [60000,120000)
  const results = emitted as Array<{
    value: Record<string, number>;
    windowStart: number;
  }>;
  const sum = results.reduce((a, r) => a + r.value.onlineMs!, 0);
  assert.equal(sum, 90_000);
  assert.equal(results.length, 2);
});

test('乱序:OFFLINE 先到被缓存,ONLINE 到达后闭合', () => {
  const c = cfg();
  const emitted: unknown[] = [];
  const op = new DriverSessionOperator(c, {
    emit: (r) => emitted.push(r),
    late: () => {},
  });
  op.processEvent(driverEvent('DRIVER_OFFLINE', 60_000, 2));
  assert.equal(op.openSessions().length, 0);
  op.processEvent(driverEvent('DRIVER_ONLINE', 0, 1));
  assert.equal(emitted.length, 0, '窗口未到水位线不触发,但会话应已闭合');
  assert.equal(op.openSessions().length, 0, '闭合后司机状态被清理');
  op.processWatermark(120_000);
  const results = emitted as Array<{ value: Record<string, number> }>;
  assert.equal(results.reduce((a, r) => a + r.value.onlineMs!, 0), 60_000);
});

test('跨天会话按东八区日边界切开,总时长不变', () => {
  const c = cfg();
  const emitted: Array<{ windowStart: number; value: Record<string, number> }> = [];
  const op = new DriverSessionOperator(c, {
    emit: (r) => emitted.push(r),
    late: () => {},
  });
  // 东八区 23:30 -> 次日 00:30
  const online = Date.UTC(2026, 10, 15, 15, 30);
  const offline = Date.UTC(2026, 10, 15, 16, 30);
  op.processEvent(driverEvent('DRIVER_ONLINE', online, 1));
  op.processEvent(driverEvent('DRIVER_OFFLINE', offline, 2));
  op.processWatermark(offline + 120_000);
  assert.equal(
    emitted.reduce((a, r) => a + r.value.onlineMs!, 0),
    60 * 60_000,
  );
  // 至少落在两个本地日的窗口里
  const days = new Set(
    emitted.map((r) => Math.floor((r.windowStart + 480 * 60_000) / 86_400_000)),
  );
  assert.ok(days.size >= 2);
});

test('迟到司机事件超宽限被丢弃并留痕', () => {
  const c = cfg();
  const late: unknown[] = [];
  const op = new DriverSessionOperator(c, { emit: () => {}, late: (r) => late.push(r) });
  op.processWatermark(200_000);
  op.processEvent(driverEvent('DRIVER_ONLINE', 10_000, 1));
  assert.equal(late.length, 1);
  assert.equal((late[0] as { reason: string }).reason, 'dropped-expired');
});

test('快照恢复后开放会话继续累计', () => {
  const c = cfg();
  const emitted1: Array<{ value: Record<string, number>; version: number }> = [];
  const op1 = new DriverSessionOperator(c, {
    emit: (r) => emitted1.push(r),
    late: () => {},
  });
  op1.processEvent(driverEvent('DRIVER_ONLINE', 1_000, 1));
  op1.processWatermark(120_000); // 触发 [0,60000) 与 [60000,120000) 的开放估算?不,开放会话不按 wm 估算
  const snap = op1.snapshot();

  const emitted2: Array<{ value: Record<string, number>; version: number }> = [];
  const op2 = new DriverSessionOperator(c, {
    emit: (r) => emitted2.push(r),
    late: () => {},
  });
  op2.restore(snap);
  assert.equal(op2.openSessions().length, 1);
  op2.processEvent(driverEvent('DRIVER_OFFLINE', 180_000, 2));
  op2.processWatermark(300_000);
  const total = emitted2.reduce((a, r) => a + r.value.onlineMs!, 0);
  assert.equal(total, 180_000 - 1_000);
});
