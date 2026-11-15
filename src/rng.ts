/**
 * 确定性随机工具。整个系统(生成器、回放、测试)只依赖种子化 PRNG,
 * 不允许在事件时间语义路径里使用 Math.random / Date.now,保证两次运行逐事件一致。
 */

/** mulberry32:小而快的 32 位 PRNG,返回 [0,1) */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return function next(): number {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** 字符串哈希(FNV-1a 32 位),用于把 ID 派生为稳定数值 */
export function fnv1a32(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** 可序列化的随机流状态(用于快照恢复后继续生成) */
export interface RngState {
  state: number;
}

export class DeterministicRng {
  private a: number;

  constructor(seed: number) {
    this.a = seed >>> 0;
  }

  /** [0,1) */
  next(): number {
    this.a = (this.a + 0x6d2b79f5) | 0;
    let t = Math.imul(this.a ^ (this.a >>> 15), 1 | this.a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** [min,max) 整数 */
  int(min: number, max: number): number {
    return min + Math.floor(this.next() * (max - min));
  }

  /** 以概率 p 返回 true */
  chance(p: number): boolean {
    return this.next() < p;
  }

  /** 从数组中等概率取一个元素 */
  pick<T>(arr: readonly T[]): T {
    const v = arr[this.int(0, arr.length)];
    if (v === undefined) throw new Error('pick from empty array');
    return v;
  }

  snapshot(): RngState {
    return { state: this.a >>> 0 };
  }

  static restore(s: RngState): DeterministicRng {
    const r = new DeterministicRng(0);
    r.a = s.state >>> 0;
    return r;
  }
}

/** 稳定字符串哈希,组合多个字段生成确定性 ID 后缀 */
export function stableHash(parts: Array<string | number>): string {
  return fnv1a32(parts.join('|')).toString(16).padStart(8, '0');
}
