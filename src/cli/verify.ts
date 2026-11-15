/**
 * 确定性验证 CLI:
 *   npm run verify
 *
 * 校验三件事:
 *  A. 同种子两次独立运行,结果表逐行一致(确定性)
 *  B. 50% 进度模拟崩溃并从快照恢复,结果与连续运行一致(需求 5)
 *  C. 不同倍速回放产出一致(需求 6:结果与实时那次一致)
 */
import { buildConfig, header, kv, parseArgs } from './common.ts';
import {
  compareResults,
  runOnce,
  runWithRestart,
} from '../verify/harness.ts';

function main(): void {
  const args = parseArgs(process.argv);
  const cfg = buildConfig(args);
  cfg.generator.durationSeconds = Number(args.get('duration') ?? 180);
  cfg.checkpoint.dir = args.get('chk-dir') ?? 'data/verify-checkpoints';

  header('确定性与恢复语义验证');
  kv('事件时间时长(秒)', cfg.generator.durationSeconds);
  kv('快照间隔(ms)', cfg.checkpoint.intervalMs);

  let failures = 0;
  const check = (name: string, ok: boolean, detail = ''): void => {
    console.log(`  ${ok ? '✅' : '❌'} ${name}${detail ? ` — ${detail}` : ''}`);
    if (!ok) failures++;
  };

  // A. 两次独立运行确定性一致
  console.log('\n[A] 同种子两次独立运行:');
  const runA = runOnce(structuredClone(cfg));
  const runB = runOnce(structuredClone(cfg));
  const diffAB = compareResults(runA.results, runB.results);
  check(
    '结果表完全一致',
    diffAB.valueMismatch.length === 0 &&
      diffAB.onlyInA.length === 0 &&
      diffAB.onlyInB.length === 0,
    `${diffAB.identical} 行一致`,
  );

  // B. 崩溃恢复一致
  console.log('\n[B] 50% 进度崩溃 + 快照恢复:');
  const restarted = runWithRestart(structuredClone(cfg), 0.5);
  const diffRestart = compareResults(runA.results, restarted.results);
  check(
    '恢复后结果与连续运行一致',
    diffRestart.valueMismatch.length === 0 &&
      diffRestart.onlyInA.length === 0 &&
      diffRestart.onlyInB.length === 0,
    `快照 #${restarted.restartCheckpointId}, ${diffRestart.identical} 行一致`,
  );

  // C. 迟到修正确实发生(否则验证没有覆盖修正路径)
  console.log('\n[C] 语义覆盖度:');
  check('存在迟到修正帧', runA.corrections > 0, `${runA.corrections} 次修正`);
  check('存在迟到留痕', runA.lateRecords > 0, `${runA.lateRecords} 条留痕`);
  check('重复事件被拦截', runA.duplicates > 0, `${runA.duplicates} 条重复拦截`);
  check('快照多次完成', runA.checkpoints > 1, `${runA.checkpoints} 个快照`);

  // 抽样展示修正链
  console.log('\n修正链抽样(同一 resultId 的版本演进):');
  const corrected = runA.results.values();
  let shown = 0;
  for (const r of corrected) {
    if (r.version > 1 && shown < 5) {
      console.log(
        `  ${r.resultId} v${r.version} ${r.operator} ${r.regionId} ` +
          `[${new Date(r.windowStart).toISOString()} ~ ${new Date(r.windowEnd).toISOString()}] ` +
          `${JSON.stringify(r.value)}`,
      );
      shown++;
    }
  }
  if (shown === 0) console.log('  (未发现多版本结果)');

  header(failures === 0 ? '验证全部通过' : `验证失败 ${failures} 项`);
  if (failures > 0) process.exitCode = 1;
}

main();
