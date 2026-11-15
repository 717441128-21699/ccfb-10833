/**
 * 通用二叉最小堆。比较器决定排序键;同键时按插入顺序(稳定),保证确定性。
 * 内部数组即完整堆结构,快照可直接存取,恢复时无需重新建堆。
 */
export class MinHeap<T> {
  private a: T[] = [];
  private readonly less: (x: T, y: T) => boolean;

  constructor(less: (x: T, y: T) => boolean, initial?: T[]) {
    this.less = less;
    if (initial && initial.length > 0) {
      this.a = initial.slice();
      // 从最后一个非叶节点下沉,线性建堆
      for (let i = (this.a.length >>> 1) - 1; i >= 0; i--) this.siftDown(i);
    }
  }

  get size(): number {
    return this.a.length;
  }

  push(x: T): void {
    this.a.push(x);
    this.siftUp(this.a.length - 1);
  }

  peek(): T | undefined {
    return this.a[0];
  }

  pop(): T | undefined {
    if (this.a.length === 0) return undefined;
    const top = this.a[0]!;
    const last = this.a.pop()!;
    if (this.a.length > 0) {
      this.a[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  /** 堆数组快照(已是合法堆序,恢复直接用) */
  toArray(): T[] {
    return this.a.slice();
  }

  /** 从一个已按堆序排列的数组直接装载(用于恢复,不再建堆) */
  static fromHeapArray<T>(less: (x: T, y: T) => boolean, heapArray: T[]): MinHeap<T> {
    const h = new MinHeap<T>(less);
    h.a = heapArray.slice();
    return h;
  }

  private siftUp(i: number): void {
    const x = this.a[i]!;
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      const p = this.a[parent]!;
      if (!this.less(x, p)) break;
      this.a[i] = p;
      i = parent;
    }
    this.a[i] = x;
  }

  private siftDown(i: number): void {
    const n = this.a.length;
    const x = this.a[i]!;
    while (true) {
      const l = i * 2 + 1;
      const r = l + 1;
      let smallest = i;
      if (l < n && this.less(this.a[l]!, this.a[smallest]!)) smallest = l;
      if (r < n && this.less(this.a[r]!, this.a[smallest]!)) smallest = r;
      if (smallest === i) break;
      this.a[i] = this.a[smallest]!;
      this.a[smallest] = x;
      i = smallest;
    }
    this.a[i] = x;
  }
}
