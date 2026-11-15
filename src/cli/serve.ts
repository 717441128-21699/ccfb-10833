/**
 * 服务启动 CLI:
 *   npm run serve -- --port=8080
 */
import { buildConfig, header, kv, parseArgs } from './common.ts';
import { JobService } from '../job/job-service.ts';
import { createHttpServer } from '../server/http-server.ts';

function main(): void {
  const args = parseArgs(process.argv);
  const cfg = buildConfig(args);
  const jobs = new JobService(cfg);
  const server = createHttpServer(cfg, jobs);

  server.listen(cfg.server.port, cfg.server.host, () => {
    header('网约车流计算服务');
    kv('监听地址', `http://${cfg.server.host}:${cfg.server.port}`);
    kv('快照目录', cfg.checkpoint.dir);
    kv('城市数 / 区域 / 司机', `${cfg.generator.cities} / ${cfg.generator.regionsPerCity} / ${cfg.generator.driversPerRegion}`);
    kv('目标速率(事件/秒)', cfg.generator.ratePerSecond.toLocaleString());
    kv('水位线乱序边界(ms)', cfg.watermark.outOfOrderBoundMs);
    kv('窗口(滚动/滑动/会话ms)', `${cfg.windows.tumbleMs} / ${cfg.windows.slideMs}-${cfg.windows.slideSizeMs} / ${cfg.windows.sessionGapMs}`);
    kv('迟到宽限(ms)', cfg.windows.allowedLatenessMs);
    console.log('\n看板页面:打开监听地址即可。Ctrl+C 退出。');
  });

  const shutdown = (): void => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 3000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main();
