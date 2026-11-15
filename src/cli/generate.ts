/**
 * 事件导出 CLI:把确定性生成器的产出写为 JSONL,供文件回放与离线核对。
 *   npm run generate -- --duration=60 --out=data/events.jsonl
 */
import { createWriteStream } from 'node:fs';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { buildConfig, header, kv, parseArgs } from './common.ts';
import { EventGenerator } from '../generator/generator.ts';

function main(): void {
  const args = parseArgs(process.argv);
  const cfg = buildConfig(args);
  const out = args.get('out') ?? 'data/events.jsonl';
  mkdirSync(dirname(out), { recursive: true });

  header('事件生成器(JSONL 导出)');
  kv('输出文件', out);
  kv('种子', cfg.generator.seed);
  kv('时长(事件时间秒)', cfg.generator.durationSeconds);
  kv('目标速率(事件/秒)', cfg.generator.ratePerSecond.toLocaleString());
  kv('乱序上限(ms)', cfg.generator.maxOutOfOrderMs);
  kv('重复率', cfg.generator.duplicateRate);
  kv('迟到率', cfg.generator.lateRate);

  const generator = new EventGenerator(cfg);
  const stream = createWriteStream(out);
  let total = 0;
  let uniqueIds = new Set<string>();
  let duplicates = 0;

  for (;;) {
    const batch = generator.nextBatch(5_000);
    if (batch.length === 0) break;
    for (const event of batch) {
      stream.write(JSON.stringify(event) + '\n');
      total++;
      if (uniqueIds.has(event.eventId)) duplicates++;
      else uniqueIds.add(event.eventId);
    }
    if (generator.progress >= 1) break;
  }
  stream.end(() => {
    kv('导出事件总数', total.toLocaleString());
    kv('唯一 eventId 数', uniqueIds.size.toLocaleString());
    kv('其中重复投递', duplicates.toLocaleString());
    kv('事件时间跨度(秒)', (generator.totalEventTimeSpanMs / 1000).toFixed(1));
    console.log('\n导出完成。');
  });
}

main();
