/**
 * 窗口聚合函数实现。
 */
import type { AggregateFunction } from './window.ts';
import type { OrderCounters, StreamEvent } from '../types.ts';

/**
 * 接单率聚合:created / accepted 双计数。
 * 派生指标 acceptRate 由展示层或结果消费方用 accepted/created 计算,
 * 这里同时输出以方便看板直接渲染。
 */
export class OrderCountersAggregate
  implements AggregateFunction<OrderCounters>
{
  readonly metric = 'order_acceptance';

  create(): OrderCounters {
    return { created: 0, accepted: 0 };
  }

  add(acc: OrderCounters, event: StreamEvent): void {
    if (event.type === 'ORDER_CREATED') acc.created++;
    else if (event.type === 'ORDER_ACCEPTED') acc.accepted++;
  }

  merge(a: OrderCounters, b: OrderCounters): OrderCounters {
    return { created: a.created + b.created, accepted: a.accepted + b.accepted };
  }

  value(acc: OrderCounters): Record<string, number> {
    return {
      created: acc.created,
      accepted: acc.accepted,
      acceptRate: acc.created === 0 ? 0 : acc.accepted / acc.created,
    };
  }
}
