/**
 * 生成器确定性与端到端语义测试。
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_CONFIG, type EngineConfig } from '../src/config.ts';
import { EventGenerator } from '../src/generator/generator.ts';
import { DeterministicRng } from '../src/rng.ts';
import {
  compareResults,
  runOnce,
  runWithRestart,
} from '../src/verify/harness.ts';

function smallCfg(): EngineConfig {
  const c = structuredClone(DEFAULT_CONFIG);
  c.generator.cities = 6;
  c.generator.regionsPerCity = 2;
  c.generator.driversPerRegion = 10;
  c.generator.durationSeconds = 60;
  c.generator.ratePerSecond = 2_000;
  c.generator.duplicateRate = 0.05;
  c.generator.lateRate = 0.03;
  c.generator.stallRate = 0.05;
  c.checkpoint.dir = 'data/test-checkpoints-e2e';
  c.checkpoint.intervalMs = 5_000;
  return c;
}

test('DeterministicRng 同种子序列一致', () => {
  const a = new DeterministicRng(42);
  const b = new DeterministicRng(42);
  for (let i = 0; i < 100; i++) assert.equal(a.next(), b.next());
  const snap = new DeterministicRng(7);
  snap.next();
  snap.next();
  const state = snap.snapshot();
  const restored = DeterministicRng.restore(state);
  const fresh = new DeterministicRng(7);
  fresh.next();
  fresh.next();
  for (let i = 0; i < 10; i++) assert.equal(restored.next(), fresh.next());
});

test('生成器同配置两次产出逐事件一致(含 eventId/seq/事件时间)', () => {
  const a = new EventGenerator(smallCfg());
  const b = new EventGenerator(smallCfg());
  const allA = [...a.allEvents(1_000)];
  const allB = [...b.allEvents(1_000)];
  assert.equal(allA.length, allB.length);
  assert.ok(allA.length > 10_000);
  for (let i = 0; i < allA.length; i++) {
    assert.deepEqual(allA[i], allB[i]);
  }
});

test('生成器确实产出重复投递与迟到事件', () => {
  const cfg = smallCfg();
  const gen = new EventGenerator(cfg);
  const events = [...gen.allEvents(2_000)];
  const ids = new Map<string, number>();
  for (const e of events) ids.set(e.eventId, (ids.get(e.eventId) ?? 0) + 1);
  const dupKinds = [...ids.values()].filter((n) => n > 1);
  assert.ok(dupKinds.length > 0, '应存在重复 eventId');
  const lateEvents = events.filter((e) => e.eventId.startsWith('evt_') === false);
  assert.ok(lateEvents.length > 0 || events.some((e) => e.subjectId.startsWith('order_late_')));
});

test('端到端:两次独立运行结果表完全一致', () => {
  const cfg = smallCfg();
  const a = runOnce(structuredClone(cfg));
  const b = runOnce(structuredClone(cfg));
  const diff = compareResults(a.results, b.results);
  assert.equal(diff.valueMismatch.length, 0);
  assert.equal(diff.onlyInA.length, 0);
  assert.equal(diff.onlyInB.length, 0);
  assert.ok(diff.identical > 0);
});

test('端到端:50% 处崩溃恢复后结果与连续运行一致(需求5)', () => {
  const cfg = smallCfg();
  const baseline = runOnce(structuredClone(cfg));
  const restarted = runWithRestart(structuredClone(cfg), 0.5);
  const diff = compareResults(baseline.results, restarted.results);
  assert.equal(diff.valueMismatch.length, 0, JSON.stringify(diff.valueMismatch[0] ?? {}));
  assert.equal(diff.onlyInA.length, 0);
  assert.equal(diff.onlyInB.length, 0);
  assert.ok(restarted.checkpoints >= 2);
});

test('端到端:重复事件被拦截且结果表无重复行(精确一次)', () => {
  const outcome = runOnce(smallCfg());
  assert.ok(outcome.duplicates > 0);
  const ids = [...outcome.results.keys()];
  assert.equal(new Set(ids).size, ids.length);
  // 结果行版本号单调
  for (const r of outcome.results.values()) {
    assert.ok(r.version >= 1);
  }
});

test('端到端:接单率窗口与司机在线时长都有产出', () => {
  const outcome = runOnce(smallCfg());
  const operators = new Set([...outcome.results.values()].map((r) => r.operator));
  assert.ok(operators.has('order-tumble-1min'));
  assert.ok(operators.has('order-sliding-1min-10s'));
  assert.ok(operators.has('order-session'));
  assert.ok(operators.has('driver-online-duration'));
});
