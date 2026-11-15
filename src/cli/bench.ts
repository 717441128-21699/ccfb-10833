/**
 * 压测 CLI:
 *   npm run bench -- --duration=600 --rate=50000
 *
 * 两段式:
 *  1. 全速吞吐测试:测量引擎单核最大事件处理速率(events/s)与端到端处理耗时
 *  2. 节流延迟测试:按目标速率(默认 5 万/秒)墙钟节流,统计端到端 p99/p95/avg/max
 *     端到端 = 事件进入拓扑(去重入口)到写入结果存储的墙钟耗时
 */
import { buildConfig, header, kv, parseArgs } from './common.ts';
import type { EngineConfig } from '../config.ts';
import { EventGenerator } from '../generator/generator.ts';
import { JobCoordinator } from '../job/coordinator.ts';
import { runOnce, runWithRestart, compareResults } from '../verify/harness.ts';

async function main(): Promise<void> {
  const args = parseArgs(process.argv);
  const cfg = buildConfig(args);
  // 压测使用独立快照目录,避免污染作业目录
  cfg.checkpoint.dir = args.get('chk-dir') ?? 'data/bench-checkpoints';
  const pacedSeconds = Number(args.get('paced-seconds') ?? 60);

  header('流计算引擎压测');
  kv('城市数', cfg.generator.cities);
  kv('目标速率(事件/秒)', cfg.generator.ratePerSecond.toLocaleString());
  kv('全速测试时长(事件时间秒)', cfg.generator.durationSeconds);
  kv('节流测试墙钟秒', pacedSeconds);
  kv('乱序上限(ms)', cfg.generator.maxOutOfOrderMs);
  kv('重复率', cfg.generator.duplicateRate);
  kv('迟到率', cfg.generator.lateRate);

  // ---------- 1. 全速吞吐 ----------
  console.log('\n[1/3] 全速吞吐测试(无节流,测量引擎处理上限)...');
  const full = runOnce(cfg);
  const wallSeconds = full.wallMs / 1000;
  const throughput = full.processed / wallSeconds;
  kv('处理事件总数', full.processed.toLocaleString());
  kv('墙钟耗时(s)', wallSeconds.toFixed(2));
  kv('引擎吞吐(事件/秒)', Math.round(throughput).toLocaleString());
  kv('单事件平均处理(μs)', ((full.wallMs / full.processed) * 1000).toFixed(2));
  kv('重复事件拦截', full.duplicates.toLocaleString());
  kv('迟到修正帧数', full.corrections.toLocaleString());
  kv('迟到留痕条数', full.lateRecords.toLocaleString());
  kv('快照完成数', full.checkpoints);
  kv('结果表行数', full.results.size.toLocaleString());
  kv('末水位线', new Date(full.endWatermark).toISOString());

  // ---------- 2. 节流延迟(p99 门禁) ----------
  console.log(`\n[2/3] 节流延迟测试(${cfg.generator.ratePerSecond.toLocaleString()} 事件/秒, ${pacedSeconds}s)...`);
  const pacedCfg = structuredClone(cfg);
  pacedCfg.generator.durationSeconds = pacedSeconds;
  pacedCfg.checkpoint.dir = `${cfg.checkpoint.dir}-paced`;
  const paced = await pacedLatencyRun(pacedCfg);
  kv('实际投递(事件/秒)', Math.round(paced.achievedRate).toLocaleString());
  kv('端到端 p50(ms)', paced.p50.toFixed(1));
  kv('端到端 p95(ms)', paced.p95.toFixed(1));
  kv('端到端 p99(ms)', paced.p99.toFixed(1));
  kv('端到端 max(ms)', paced.max.toFixed(1));
  kv('端到端 avg(ms)', paced.avg.toFixed(2));
  const p99Gate = paced.p99 <= 2000;
  kv('p99 <= 2000ms 门禁', p99Gate ? '通过 ✅' : '未通过 ❌');

  // ---------- 3. 重启一致性冒烟(短时长) ----------
  console.log('\n[3/3] 重启恢复一致性(50% 进度模拟崩溃)...');
  const restartCfg = structuredClone(cfg);
  restartCfg.generator.durationSeconds = Number(args.get('restart-duration') ?? 120);
  restartCfg.checkpoint.dir = `${cfg.checkpoint.dir}-restart`;
  const baseline = runOnce(restartCfg);
  const restarted = runWithRestart(restartCfg, 0.5);
  const diff = compareResults(baseline.results, restarted.results);
  kv('重启点快照 ID', restarted.restartCheckpointId);
  kv('结果完全一致行数', diff.identical);
  kv('仅基准存在', diff.onlyInA.length);
  kv('仅重启运行存在', diff.onlyInB.length);
  kv('值不一致', diff.valueMismatch.length);
  const consistencyGate =
    diff.onlyInA.length === 0 &&
    diff.onlyInB.length === 0 &&
    diff.valueMismatch.length === 0;
  kv('重启一致性门禁', consistencyGate ? '通过 ✅' : '未通过 ❌');

  header('压测结论');
  const throughputGate = throughput >= cfg.generator.ratePerSecond;
  kv('吞吐门禁(>= 目标速率)', throughputGate ? '通过 ✅' : '未通过 ❌');
  kv('延迟门禁(p99 <= 2s)', p99Gate ? '通过 ✅' : '未通过 ❌');
  kv('一致性门禁', consistencyGate ? '通过 ✅' : '未通过 ❌');
  if (!(throughputGate && p99Gate && consistencyGate)) process.exitCode = 1;
}

async function pacedLatencyRun(cfg: EngineConfig): Promise<{
  achievedRate: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  avg: number;
}> {
  const coordinator = new JobCoordinator(cfg);
  const generator = new EventGenerator(cfg);
  coordinator.attachGenerator(generator);

  const startWall = Date.now();
  const base = generator.baseEventTime;
  const targetRate = cfg.generator.ratePerSecond;
  let processed = 0;
  let delivered = 0;

  // 追赶式批次节流:按事件时间计算每批的墙钟释放目标,落后时只睡一次后由引擎
  // 全速追赶。引擎全速吞吐(见第 1 段)远高于目标速率,因此平均投递速率贴近目标,
  // 同时端到端延迟真实反映持续高负载下的处理时延。
  for (;;) {
    const batch = generator.nextBatch(4_000);
    if (batch.length === 0) break;
    const last = batch[batch.length - 1]!;
    const expectedByNow = ((last.eventTime - base) / 1000) * targetRate;
    const targetWall = startWall + (expectedByNow / targetRate) * 1000;
    const wait = targetWall - Date.now();
    if (wait > 2) await new Promise((r) => setTimeout(r, wait));
    const batchWall = performance.now();
    for (const event of batch) {
      event.ingestWallMs = batchWall;
      coordinator.feed(event);
      delivered++;
      processed++;
    }
    coordinator.endBatch();
  }
  coordinator.flush();
  const wall = (Date.now() - startWall) / 1000;
  const m = coordinator.topology.metrics;
  return {
    achievedRate: delivered / wall,
    p50: quantileFromMetrics(m, 0.5),
    p95: m.p95,
    p99: m.p99,
    max: m.maxLatency,
    avg: m.avgLatency,
  };
}

function quantileFromMetrics(
  m: JobCoordinator['topology']['metrics'],
  p: number,
): number {
  return m.quantile(p);
}

main().catch((err) => {
  console.error('压测失败:', err);
  process.exit(1);
});
