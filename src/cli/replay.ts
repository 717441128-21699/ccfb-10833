/**
 * 回放 CLI:
 *   npm run replay -- --replay-speed=60
 *   npm run replay -- --file=data/events.jsonl
 *
 * 倍速回放与实时跑同一份确定性事件流,对比结果表必须逐行一致(需求 6)。
 */
import { buildConfig, header, kv, parseArgs } from './common.ts';
import {
  compareResults,
  importBaseline,
  runFromJsonl,
  runOnce,
  exportBaseline,
} from '../verify/harness.ts';
import { existsSync } from 'node:fs';

function main(): void {
  const args = parseArgs(process.argv);
  const cfg = buildConfig(args);
  const speed = Number(args.get('replay-speed') ?? cfg.replaySpeed);
  const file = args.get('file');
  const baselinePath = args.get('baseline') ?? 'data/baseline.jsonl';

  header('历史事件回放');
  kv('倍速', file ? '文件源(不限速)' : `${speed}x`);
  kv('基准结果文件', baselinePath);

  // 基准:全速跑一遍(等价于实时跑的确定性结果)
  if (!existsSync(baselinePath)) {
    console.log('基准文件不存在,先生成基准...');
    const base = runOnce(structuredClone(cfg));
    exportBaseline(base, baselinePath);
    kv('基准结果行数', base.results.size);
  }
  const baseline = importBaseline(baselinePath);

  const outcome = file
    ? runFromJsonl(structuredClone(cfg), file)
    : runOnce(structuredClone(cfg));

  const diff = compareResults(baseline, outcome.results);
  kv('回放结果行数', outcome.results.size);
  kv('基准结果行数', baseline.size);
  kv('一致行数', diff.identical);
  kv('值不一致', diff.valueMismatch.length);
  kv('仅基准存在', diff.onlyInA.length);
  kv('仅回放存在', diff.onlyInB.length);

  if (diff.valueMismatch.length > 0) {
    console.log('\n不一致抽样(最多 5 条):');
    for (const m of diff.valueMismatch.slice(0, 5)) {
      console.log(`  ${m.resultId}`);
      console.log(`    基准: v${m.a.version} ${JSON.stringify(m.a.value)}`);
      console.log(`    回放: v${m.b.version} ${JSON.stringify(m.b.value)}`);
    }
  }

  const ok =
    diff.valueMismatch.length === 0 &&
    diff.onlyInA.length === 0 &&
    diff.onlyInB.length === 0;
  header(ok ? '回放结果与实时基准完全一致 ✅' : '回放结果不一致 ❌');
  if (!ok) process.exitCode = 1;
}

main();
