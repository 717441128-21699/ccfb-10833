/**
 * 运行指标收集。所有计数只用于看板与压测报告,不参与计算口径。
 * 延迟直方图采用固定对数桶,p99/p95 等从桶估计(足够运营看板与压测门禁使用)。
 */

/** 延迟桶上界(毫秒),最后一个桶为 +Inf */
const LATENCY_BUCKETS_MS = [
  50, 100, 150, 200, 300, 400, 600, 800, 1000, 1500, 2000, 3000, 5000, 10000,
];

export class MetricsCollector {
  received = 0;
  duplicates = 0;
  emitted = 0;
  corrections = 0;
  lateMerged = 0;
  lateDropped = 0;
  watermarksEmitted = 0;
  checkpointsCompleted = 0;

  private latBucket = new Array<number>(LATENCY_BUCKETS_MS.length + 1).fill(0);
  private latSum = 0;
  private latCount = 0;
  private latMax = 0;

  /** 处理速率(每秒),环形采样 */
  private throughputWindow: number[] = [];
  private lastThroughputTs = 0;
  private sinceLastSample = 0;

  observeLatency(ms: number): void {
    this.latSum += ms;
    this.latCount++;
    if (ms > this.latMax) this.latMax = ms;
    let i = 0;
    while (i < LATENCY_BUCKETS_MS.length && ms > LATENCY_BUCKETS_MS[i]!) i++;
    this.latBucket[i]!++;
  }

  /** 每处理一个事件调用,wallMs 为当前墙钟 */
  observeThroughput(wallMs: number): void {
    if (this.lastThroughputTs === 0) this.lastThroughputTs = wallMs;
    this.sinceLastSample++;
    const elapsed = wallMs - this.lastThroughputTs;
    if (elapsed >= 1000) {
      this.throughputWindow.push((this.sinceLastSample * 1000) / elapsed);
      if (this.throughputWindow.length > 60) this.throughputWindow.shift();
      this.lastThroughputTs = wallMs;
      this.sinceLastSample = 0;
    }
  }

  /** 估计分位数(p 取 0..1) */
  quantile(p: number): number {
    const total = this.latBucket.reduce((a, b) => a + b, 0);
    if (total === 0) return 0;
    const target = p * total;
    let cum = 0;
    for (let i = 0; i < this.latBucket.length; i++) {
      cum += this.latBucket[i]!;
      if (cum >= target) {
        return i === LATENCY_BUCKETS_MS.length
          ? LATENCY_BUCKETS_MS[LATENCY_BUCKETS_MS.length - 1]!
          : LATENCY_BUCKETS_MS[i]!;
      }
    }
    return LATENCY_BUCKETS_MS[LATENCY_BUCKETS_MS.length - 1]!;
  }

  get p99(): number {
    return this.quantile(0.99);
  }

  get p95(): number {
    return this.quantile(0.95);
  }

  get avgLatency(): number {
    return this.latCount === 0 ? 0 : this.latSum / this.latCount;
  }

  get maxLatency(): number {
    return this.latMax;
  }

  get throughputSeries(): number[] {
    return this.throughputWindow;
  }

  get currentThroughput(): number {
    return this.throughputWindow.at(-1) ?? 0;
  }

  snapshot(): Record<string, number | number[]> {
    return {
      received: this.received,
      duplicates: this.duplicates,
      emitted: this.emitted,
      corrections: this.corrections,
      lateMerged: this.lateMerged,
      lateDropped: this.lateDropped,
      watermarksEmitted: this.watermarksEmitted,
      checkpointsCompleted: this.checkpointsCompleted,
      p99: this.p99,
      p95: this.p95,
      avgLatency: Math.round(this.avgLatency * 100) / 100,
      maxLatency: this.latMax,
      throughput: this.currentThroughput,
      throughputSeries: [...this.throughputWindow],
    };
  }
}
