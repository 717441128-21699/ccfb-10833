/**
 * 事件模型与流元素定义。
 *
 * 业务事件分两类:
 *  - ORDER_* :订单生命周期事件,用于区域接单率
 *  - DRIVER_*:司机上下线事件,用于会话在线时长
 */
export type OrderEventType = 'ORDER_CREATED' | 'ORDER_ACCEPTED';
export type DriverEventType = 'DRIVER_ONLINE' | 'DRIVER_OFFLINE';
export type EventType = OrderEventType | DriverEventType;

export interface StreamEvent {
  /** 全局唯一事件 ID(去重主键,重复投递时保持不变) */
  eventId: string;
  /** 事件类型 */
  type: EventType;
  /** 事件时间(epoch 毫秒),窗口与水位线的唯一时间口径 */
  eventTime: number;
  /** 城市 ID,水位线按城市分区推进 */
  cityId: string;
  /** 区域 ID,窗口 key */
  regionId: string;
  /** 订单事件:订单 ID;司机事件:司机 ID */
  subjectId: string;
  /** 生成器单调序列号,用于源端快照与回放(重复投递共享同一 seq) */
  seq: number;
  /**
   * 投递序号:每次交付(含重复投递)单调递增且唯一。
   * WAL 与检查点位点按 deliveryId 跟踪,保证重复事件不会在恢复时被漏掉。
   */
  deliveryId: number;
  /** 首次进入引擎的墙钟时间(仅用于延迟度量,不参与任何计算口径) */
  ingestWallMs?: number;
}

/** 流元素:数据事件 */
export interface DataElement {
  kind: 'data';
  event: StreamEvent;
}

/** 流元素:水位线(事件时间语义,值为全局已观测到的最小有效水位线) */
export interface WatermarkElement {
  kind: 'watermark';
  /** 全局水位线 */
  globalWatermark: number;
  /** 各城市水位线快照(供看板展示分城市推进情况) */
  cityWatermarks: Record<string, number>;
}

/** 流元素:检查点屏障,携带单调递增的检查点 ID */
export interface BarrierElement {
  kind: 'barrier';
  checkpointId: number;
  /** 屏障对齐的事件时间 */
  watermark: number;
}

/** 流元素:流结束标记,触发所有残留窗口/会话闭合 */
export interface EndOfStreamElement {
  kind: 'eos';
}

export type StreamElement =
  | DataElement
  | WatermarkElement
  | BarrierElement
  | EndOfStreamElement;

export function isOrderEvent(t: EventType): t is OrderEventType {
  return t === 'ORDER_CREATED' || t === 'ORDER_ACCEPTED';
}

/** 接单率聚合的累加器 */
export interface OrderCounters {
  created: number;
  accepted: number;
}

/** 司机在线时长聚合的累加器(毫秒) */
export interface DurationAcc {
  onlineMs: number;
}

/**
 * 窗口结果信封。resultId 由 (算子名, key, 窗口类型, 窗口起止, 指标名) 确定性派生,
 * 下游据此幂等 upsert,实现精确一次投递。
 */
export interface WindowResult<K, V> {
  resultId: string;
  operator: string;
  windowType: 'tumble' | 'sliding' | 'session';
  regionId: string;
  key: K;
  windowStart: number;
  windowEnd: number;
  metric: string;
  value: V;
  /** 1 = 首次触发;同一窗口被迟到事件修正时版本号递增 */
  version: number;
  /** 发出该结果时的事件时间水位线 */
  emittedAtWatermark: number;
  /** 是否为迟到修正(首帧为 false) */
  correction: boolean;
}

/** 迟到事件的留痕记录(看板"迟到数据"页) */
export interface LateRecord {
  eventId: string;
  eventTime: number;
  cityId: string;
  regionId: string;
  watermarkAtArrival: number;
  behindMs: number;
  reason: 'merged-with-correction' | 'dropped-expired';
  windowStart: number;
  windowEnd: number;
  operator: string;
  seq: number;
}
