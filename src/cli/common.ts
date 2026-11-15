/**
 * CLI 共享工具:简单的 --key=value / --flag 参数解析与配置覆盖。
 */
import { DEFAULT_CONFIG, type EngineConfig } from '../config.ts';

export function parseArgs(argv: string[]): Map<string, string> {
  const args = new Map<string, string>();
  for (const token of argv.slice(2)) {
    if (!token.startsWith('--')) continue;
    const eq = token.indexOf('=');
    if (eq === -1) args.set(token.slice(2), 'true');
    else args.set(token.slice(2, eq), token.slice(eq + 1));
  }
  return args;
}

function num(args: Map<string, string>, key: string, fallback: number): number {
  const v = args.get(key);
  if (v === undefined) return fallback;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`参数 --${key} 不是合法数字: ${v}`);
  return n;
}

/** 用命令行参数覆盖默认配置,覆盖项仅包含需要频繁调整的压测旋钮 */
export function buildConfig(args: Map<string, string>): EngineConfig {
  const cfg = structuredClone(DEFAULT_CONFIG);
  const g = cfg.generator;
  g.seed = num(args, 'seed', g.seed);
  g.cities = num(args, 'cities', g.cities);
  g.ratePerSecond = num(args, 'rate', g.ratePerSecond);
  g.durationSeconds = num(args, 'duration', g.durationSeconds);
  g.maxOutOfOrderMs = num(args, 'out-of-order-ms', g.maxOutOfOrderMs);
  g.duplicateRate = num(args, 'duplicate-rate', g.duplicateRate);
  g.lateRate = num(args, 'late-rate', g.lateRate);
  g.latenessLagMs = num(args, 'lateness-lag-ms', g.latenessLagMs);
  g.skew = num(args, 'skew', g.skew);
  g.stallRate = num(args, 'stall-rate', g.stallRate);
  cfg.replaySpeed = num(args, 'replay-speed', cfg.replaySpeed);
  cfg.windows.allowedLatenessMs = num(
    args,
    'allowed-lateness-ms',
    cfg.windows.allowedLatenessMs,
  );
  if (args.has('chk-dir')) cfg.checkpoint.dir = args.get('chk-dir')!;
  if (args.has('port')) cfg.server.port = num(args, 'port', cfg.server.port);
  return cfg;
}

export function header(title: string): void {
  console.log('='.repeat(72));
  console.log(`  ${title}`);
  console.log('='.repeat(72));
}

export function kv(label: string, value: unknown): void {
  console.log(`  ${label.padEnd(28)} ${String(value)}`);
}
