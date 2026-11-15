/**
 * 全局配置。所有时间单位均为毫秒,除非字段名另有说明。
 * 这套配置同时驱动实时作业与回放作业,保证两条路径行为一致。
 */
export interface EngineConfig {
  /** 生成器 / 作业参数 */
  generator: {
    cities: number;
    regionsPerCity: number;
    driversPerRegion: number;
    seed: number;
    /** 每秒目标事件数(实时模式节流用;回放模式忽略,只受 replaySpeed 控制) */
    ratePerSecond: number;
    /** 运行时长(秒) */
    durationSeconds: number;
    /**
     * 逻辑事件时间起点的 epoch 毫秒。
     * 0 表示用固定确定性锚点(2026-01-01 UTC),保证离线/回放/测试逐次运行一致;
     * 仅实时服务需要"当前时间"语义时由启动方显式覆盖为 Date.now()。
     */
    baseEventTime: number;
    /** 乱序:事件被随机延后调度的最大毫秒数 */
    maxOutOfOrderMs: number;
    /** 0~1,重复投递比例(同一 eventId 再次出现) */
    duplicateRate: number;
    /** 0~1,迟到事件比例(事件时间明显落后于当前水位线) */
    lateRate: number;
    /** 迟到事件落后当前水位线的最大毫秒数 */
    latenessLagMs: number;
    /** 0~1,城市倾斜:把额外流量集中到热点城市 */
    skew: number;
    /** 某城市事件流卡住的概率(演示空闲检测);0 关闭 */
    stallRate: number;
  };
  /** 水位线参数,按城市各自推进 */
  watermark: {
    /** 水位线 = 该城市最大事件时间 - outOfOrderBoundMs */
    outOfOrderBoundMs: number;
    /** 事件时间口径下的空闲超时:某城市最大事件时间落后全局最大事件时间超过该值即视为空闲 */
    idleTimeoutMs: number;
    /** 水位线推进的最小步进,避免每个事件都向下游广播 */
    intervalMs: number;
  };
  /** 窗口参数 */
  windows: {
    tumbleMs: number;
    slideMs: number;
    slideSizeMs: number;
    sessionGapMs: number;
    /** 迟到宽限:窗口结束后多久内仍接受迟到事件并发出修正 */
    allowedLatenessMs: number;
  };
  /**
   * 去重保留余量(毫秒)。去重保留窗口 = 水位线乱序边界 + 该余量,
   * 按到达顺序保留 eventId。重复副本都在乱序边界内到达,无需长保留。
   */
  dedupTtlMs: number;
  /** 快照 */
  checkpoint: {
    /** 事件时间口径的快照间隔(barrier 每推进该事件时间发出一次) */
    intervalMs: number;
    dir: string;
    /** 保留最近多少个快照 */
    retain: number;
  };
  /** 司机在线时长切日的时区偏移分钟(中国为 +480) */
  dayBoundaryOffsetMinutes: number;
  /** 看板 / 服务 */
  server: {
    port: number;
    host: string;
  };
  /** 回放倍速(1 = 按事件时间原速;60 = 一分钟跑完一分钟事件时间) */
  replaySpeed: number;
}

export const DEFAULT_CONFIG: EngineConfig = {
  generator: {
    cities: 30,
    regionsPerCity: 4,
    driversPerRegion: 80,
    seed: 20261115,
    ratePerSecond: 50_000,
    durationSeconds: 600,
    baseEventTime: Date.UTC(2026, 0, 1, 0, 0, 0),
    maxOutOfOrderMs: 5_000,
    duplicateRate: 0.03,
    lateRate: 0.02,
    latenessLagMs: 30_000,
    skew: 0.3,
    stallRate: 0.02,
  },
  watermark: {
    outOfOrderBoundMs: 2_000,
    idleTimeoutMs: 15_000,
    intervalMs: 200,
  },
  windows: {
    tumbleMs: 60_000,
    slideMs: 60_000,
    slideSizeMs: 10_000,
    sessionGapMs: 300_000,
    allowedLatenessMs: 30_000,
  },
  dedupTtlMs: 3_000,
  checkpoint: {
    intervalMs: 10_000,
    dir: 'data/checkpoints',
    retain: 3,
  },
  dayBoundaryOffsetMinutes: 480,
  server: {
    port: 8080,
    host: '127.0.0.1',
  },
  replaySpeed: 60,
};

/** 深拷贝配置,避免作业之间共享可变引用 */
export function cloneConfig(cfg: EngineConfig): EngineConfig {
  return structuredClone(cfg);
}
