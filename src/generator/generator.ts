/**
 * 确定性事件生成器。
 *
 * 产出两类事件流:
 *  - 订单流:ORDER_CREATED 后以一定概率、在短延迟内产生 ORDER_ACCEPTED(同 orderId)
 *  - 司机流:每个司机周期性 ONLINE / OFFLINE,会话长度与间隔随机
 *
 * 可控参数(全部进 config.generator):
 *  - maxOutOfOrderMs:事件交付顺序相对事件时间的乱序程度
 *  - duplicateRate :重复投递比例(复制已生成事件,eventId 不变)
 *  - lateRate      :迟到事件比例(事件时间落在水位线之后)
 *  - skew          :城市流量倾斜
 *  - stallRate     :城市卡住概率(演示分城市水位线空闲检测)
 *
 * 高性能结构:
 *  - 待发接单事件放在按就绪时间排序的最小堆,O(log n) 插入/弹出
 *  - 司机上下线调度放在按下次翻转时间排序的最小堆,消除全量扫描
 *
 * 确定性:相同 seed + 相同配置产出逐字节一致的事件序列;
 * 实时节流只影响交付墙钟、不改变事件本身,因此回放结果与实时一致。
 */
import type { EngineConfig } from '../config.ts';
import type { DriverEventType, EventType, StreamEvent } from '../types.ts';
import { DeterministicRng, stableHash } from '../rng.ts';
import { MinHeap } from '../util/heap.ts';

interface PendingOrder {
  /** 就绪事件时间 = 下单时间 + 接单延迟 */
  readyAt: number;
  /** 下单事件时间 */
  eventTime: number;
  cityId: string;
  regionId: string;
  orderId: string;
  /** 同就绪时间时的稳定次序 */
  tie: number;
}

interface DriverTask {
  driverId: string;
  nextChangeAt: number;
  nextType: DriverEventType;
  regionId: string;
  tie: number;
}

export interface GeneratorOptions {
  durationSeconds?: number;
  ratePerSecond?: number;
}

export class EventGenerator {
  private rng: DeterministicRng;
  private cities: string[] = [];
  private regionsByCity = new Map<string, string[]>();
  private driversByRegion = new Map<string, string[]>();
  /** 待发出的接单事件,按就绪时间的最小堆 */
  private acceptHeap: MinHeap<PendingOrder> = new MinHeap(
    (x, y) => x.readyAt < y.readyAt || (x.readyAt === y.readyAt && x.tie < y.tie),
  );
  /** 司机下次上下线任务,按翻转时间的最小堆 */
  private driverHeap: MinHeap<DriverTask> = new MinHeap(
    (x, y) => x.nextChangeAt < y.nextChangeAt || (x.nextChangeAt === y.nextChangeAt && x.tie < y.tie),
  );
  /** 被卡住的城市 -> 恢复的事件时间 */
  private stalledCities = new Map<string, number>();
  private seq = 0;
  private deliveryId = 0;
  private acceptTie = 0;
  private readonly cfg: EngineConfig;
  private baseTime: number;
  private endTime: number;
  private cursor: number;
  private eventIndex = 0;
  /** 流末补发已完成,下一次 nextBatch 必定返回空批 */
  private exhausted = false;

  constructor(cfg: EngineConfig, opts: GeneratorOptions = {}) {
    this.cfg = cfg;
    this.rng = new DeterministicRng(cfg.generator.seed);
    const g = cfg.generator;
    const duration = (opts.durationSeconds ?? g.durationSeconds) * 1000;
    void opts.ratePerSecond;

    for (let c = 0; c < g.cities; c++) {
      const cityId = `city_${String(c).padStart(3, '0')}`;
      this.cities.push(cityId);
      const regions: string[] = [];
      for (let r = 0; r < g.regionsPerCity; r++) {
        const regionId = `${cityId}__region_${r}`;
        regions.push(regionId);
        const drivers: string[] = [];
        for (let d = 0; d < g.driversPerRegion; d++) {
          drivers.push(`driver_${cityId}_${r}_${String(d).padStart(4, '0')}`);
        }
        this.driversByRegion.set(regionId, drivers);
      }
      this.regionsByCity.set(cityId, regions);
    }

    this.baseTime = g.baseEventTime > 0 ? g.baseEventTime : Date.now();
    this.endTime = this.baseTime + duration;
    this.cursor = this.baseTime;

    // 初始化每个司机的首个上线任务(tie 用确定性序号)
    let tie = 0;
    for (const [regionId, drivers] of this.driversByRegion) {
      for (const driverId of drivers) {
        this.driverHeap.push({
          driverId,
          nextChangeAt: this.baseTime + this.rng.int(0, 180_000),
          nextType: 'DRIVER_ONLINE',
          regionId,
          tie: tie++,
        });
      }
    }
  }

  private pickCity(): string {
    const g = this.cfg.generator;
    if (this.rng.chance(g.skew)) {
      const hotCount = Math.max(1, Math.floor(this.cities.length * 0.2));
      return this.cities[this.rng.int(0, hotCount)]!;
    }
    return this.cities[this.rng.int(0, this.cities.length)]!;
  }

  private pickRegion(cityId: string): string {
    const regions = this.regionsByCity.get(cityId)!;
    return regions[this.rng.int(0, regions.length)]!;
  }

  private isStalled(cityId: string, atEventTime: number): boolean {
    const until = this.stalledCities.get(cityId);
    if (until === undefined) return false;
    if (atEventTime >= until) {
      this.stalledCities.delete(cityId);
      return false;
    }
    return true;
  }

  private maybeStall(cityId: string, atEventTime: number): void {
    const g = this.cfg.generator;
    if (g.stallRate <= 0 || this.stalledCities.has(cityId)) return;
    if (this.rng.chance(g.stallRate / 1000)) {
      const stallMs = this.rng.int(10_000, 40_000);
      this.stalledCities.set(cityId, atEventTime + stallMs);
    }
  }

  private makeEvent(
    type: EventType,
    eventTime: number,
    cityId: string,
    regionId: string,
    subjectId: string,
  ): StreamEvent {
    this.seq++;
    this.deliveryId++;
    return {
      eventId: `evt_${stableHash([type, subjectId, eventTime, this.seq])}_${this.seq}`,
      type,
      eventTime,
      cityId,
      regionId,
      subjectId,
      seq: this.seq,
      deliveryId: this.deliveryId,
    };
  }

  nextBatch(batchSize: number): StreamEvent[] {
    if (this.exhausted) return [];
    const g = this.cfg.generator;
    const out: StreamEvent[] = [];
    let guard = 0;

    while (
      !this.exhausted &&
      out.length < batchSize &&
      this.cursor < this.endTime &&
      guard < batchSize * 20
    ) {
      guard++;
      this.cursor += 1000 / g.ratePerSecond;
      const at = this.cursor;

      if (this.rng.chance(0.15)) {
        this.generateDueDriverEvent(at, out);
      } else {
        this.generateOrderEvent(at, out);
      }

      if (this.rng.chance(g.lateRate)) {
        this.generateLateEvent(at, out);
      }

      // 弹出所有已就绪的接单事件(堆顶始终最早就绪)
      const emitHorizon = at + g.maxOutOfOrderMs;
      let top = this.acceptHeap.peek();
      while (top && top.readyAt <= emitHorizon) {
        this.acceptHeap.pop();
        this.emitPendingAccept(top, out);
        top = this.acceptHeap.peek();
      }

      if (this.cursor >= this.endTime) {
        // 流结束:剩余接单事件截断到终点补发,保证接单率口径
        let rest = this.acceptHeap.pop();
        while (rest) {
          const acceptAt = Math.min(rest.readyAt, this.endTime - 1);
          this.emitPendingAccept({ ...rest, readyAt: acceptAt }, out);
          rest = this.acceptHeap.pop();
        }
        // 在途接单已全部补发,后续调用应返回空批
        this.exhausted = true;
      }
    }

    // 重复投递:复制事件,eventId/seq 不变,交付序号重新分配
    const dupCount = Math.round(out.length * g.duplicateRate);
    for (let i = 0; i < dupCount; i++) {
      const src = out[this.rng.int(0, out.length)];
      if (src) {
        this.deliveryId++;
        out.push({ ...src, deliveryId: this.deliveryId });
      }
    }

    // 乱序:按事件时间排序后做有限距离随机交换
    out.sort((a, b) => a.eventTime - b.eventTime);
    const swapRadius = Math.max(1, Math.floor(g.maxOutOfOrderMs / 100));
    for (let i = 0; i < out.length; i++) {
      if (this.rng.chance(0.5)) {
        const j = Math.min(
          out.length - 1,
          Math.max(0, i + this.rng.int(-swapRadius, swapRadius + 1)),
        );
        const tmp = out[i]!;
        out[i] = out[j]!;
        out[j] = tmp;
      }
    }

    this.eventIndex += out.length;
    return out;
  }

  private generateLateEvent(nowEventTime: number, out: StreamEvent[]): void {
    const g = this.cfg.generator;
    const upper =
      nowEventTime - this.cfg.watermark.outOfOrderBoundMs - g.latenessLagMs * 0.2;
    if (upper <= this.baseTime + 1000) return;
    const lateEventTime =
      this.baseTime + this.rng.int(1000, Math.max(1001, Math.floor(upper - this.baseTime)));
    const cityId = this.pickCity();
    const regionId = this.pickRegion(cityId);
    const orderId = `order_late_${cityId}_${stableHash([lateEventTime, this.seq + 1])}`;
    out.push(
      this.makeEvent('ORDER_CREATED', lateEventTime, cityId, regionId, orderId),
    );
    if (this.rng.chance(0.5)) {
      const accept = this.makeEvent(
        'ORDER_ACCEPTED',
        Math.min(lateEventTime + this.rng.int(2_000, 20_000), upper),
        cityId,
        regionId,
        orderId,
      );
      accept.eventId = `evt_acc_${stableHash([orderId, lateEventTime])}`;
      out.push(accept);
    }
  }

  private emitPendingAccept(p: PendingOrder, out: StreamEvent[]): void {
    const accept = this.makeEvent(
      'ORDER_ACCEPTED',
      p.readyAt,
      p.cityId,
      p.regionId,
      p.orderId,
    );
    accept.eventId = `evt_acc_${stableHash([p.orderId, p.eventTime])}`;
    out.push(accept);
  }

  private generateOrderEvent(at: number, out: StreamEvent[]): void {
    const cityId = this.pickCity();
    if (this.isStalled(cityId, at)) {
      this.maybeStall(cityId, at);
      return;
    }
    this.maybeStall(cityId, at);
    const regionId = this.pickRegion(cityId);
    const orderId = `order_${cityId}_${stableHash([this.seq + 1, at])}`;
    out.push(this.makeEvent('ORDER_CREATED', at, cityId, regionId, orderId));

    if (this.rng.chance(0.7)) {
      // 接单延迟 2~10 秒:贴合网约车秒级接单节奏,同时把在途堆稳态规模控制在有界范围
      const acceptDelayMs = this.rng.int(2_000, 10_000);
      this.acceptHeap.push({
        readyAt: at + acceptDelayMs,
        eventTime: at,
        cityId,
        regionId,
        orderId,
        tie: this.acceptTie++,
      });
    }
  }

  private generateDueDriverEvent(at: number, out: StreamEvent[]): void {
    const top = this.driverHeap.peek();
    // 只处理最早到点的一个司机;其余到点司机留待后续槽位,保证约 15% 占比
    if (!top || top.nextChangeAt > at) return;
    this.driverHeap.pop();
    const cityId = top.regionId.split('__')[0]!;
    if (this.isStalled(cityId, at)) {
      // 城市卡住:任务原样推回,等恢复后再处理
      this.driverHeap.push(top);
      return;
    }

    out.push(
      this.makeEvent(top.nextType, top.nextChangeAt, cityId, top.regionId, top.driverId),
    );

    if (top.nextType === 'DRIVER_ONLINE') {
      top.nextType = 'DRIVER_OFFLINE';
      top.nextChangeAt = top.nextChangeAt + this.rng.int(5 * 60_000, 40 * 60_000);
    } else {
      top.nextType = 'DRIVER_ONLINE';
      top.nextChangeAt = top.nextChangeAt + this.rng.int(2 * 60_000, 20 * 60_000);
    }
    this.driverHeap.push(top);
  }

  *allEvents(batchSize = 5_000): Generator<StreamEvent> {
    while (true) {
      const batch = this.nextBatch(batchSize);
      if (batch.length === 0) return;
      for (const e of batch) yield e;
    }
  }

  get totalEventTimeSpanMs(): number {
    return this.endTime - this.baseTime;
  }

  get baseEventTime(): number {
    return this.baseTime;
  }

  get progress(): number {
    return Math.min(1, (this.cursor - this.baseTime) / (this.endTime - this.baseTime));
  }

  get lastSeq(): number {
    return this.seq;
  }

  /** 待发接单事件数(诊断/看板) */
  get pendingAcceptCount(): number {
    return this.acceptHeap.size;
  }

  /** 小游标:进主检查点(不含大体量堆) */
  compactCursor(): CompactGeneratorCursor {
    return {
      rng: this.rng.snapshot(),
      seq: this.seq,
      deliveryId: this.deliveryId,
      acceptTie: this.acceptTie,
      cursor: this.cursor,
      eventIndex: this.eventIndex,
      exhausted: this.exhausted,
      stalledCities: [...this.stalledCities.entries()],
    };
  }

  /** 大堆状态:写独立生成器 sidecar,避免撑大周期性主检查点 */
  heapState(): GeneratorHeapState {
    return {
      acceptHeap: this.acceptHeap.toArray(),
      driverHeap: this.driverHeap.toArray(),
    };
  }

  /** 从小游标 + 堆状态完整恢复 */
  restoreFromState(compact: CompactGeneratorCursor, heaps: GeneratorHeapState): void {
    const acceptLess = (x: PendingOrder, y: PendingOrder): boolean =>
      x.readyAt < y.readyAt || (x.readyAt === y.readyAt && x.tie < y.tie);
    const driverLess = (x: DriverTask, y: DriverTask): boolean =>
      x.nextChangeAt < y.nextChangeAt || (x.nextChangeAt === y.nextChangeAt && x.tie < y.tie);
    this.rng = DeterministicRng.restore(compact.rng);
    this.seq = compact.seq;
    this.deliveryId = compact.deliveryId;
    this.acceptTie = compact.acceptTie;
    this.cursor = compact.cursor;
    this.eventIndex = compact.eventIndex;
    this.exhausted = compact.exhausted;
    this.stalledCities = new Map(compact.stalledCities);
    this.acceptHeap = MinHeap.fromHeapArray(acceptLess, heaps.acceptHeap);
    this.driverHeap = MinHeap.fromHeapArray(driverLess, heaps.driverHeap);
  }

  cursorSnapshot(): GeneratorCursor {
    return {
      ...this.compactCursor(),
      ...this.heapState(),
    };
  }

  restoreCursor(s: GeneratorCursor): void {
    this.restoreFromState(
      {
        rng: s.rng,
        seq: s.seq,
        deliveryId: s.deliveryId,
        acceptTie: s.acceptTie,
        cursor: s.cursor,
        eventIndex: s.eventIndex,
        exhausted: s.exhausted,
        stalledCities: s.stalledCities,
      },
      { acceptHeap: s.acceptHeap, driverHeap: s.driverHeap },
    );
  }
}

export interface CompactGeneratorCursor {
  rng: ReturnType<DeterministicRng['snapshot']>;
  seq: number;
  deliveryId: number;
  acceptTie: number;
  cursor: number;
  eventIndex: number;
  exhausted: boolean;
  stalledCities: Array<[string, number]>;
}

export interface GeneratorHeapState {
  acceptHeap: PendingOrder[];
  driverHeap: DriverTask[];
}

export interface GeneratorCursor extends CompactGeneratorCursor, GeneratorHeapState {}
