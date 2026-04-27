export type HeapItem = {
  readonly rowIndex: number;
  readonly value: number;
};

type Compare = (left: HeapItem, right: HeapItem) => number;

export class BinaryHeap {
  private readonly items: HeapItem[] = [];

  constructor(private readonly compare: Compare) {}

  get size(): number {
    return this.items.length;
  }

  peek(): HeapItem | undefined {
    return this.items[0];
  }

  push(item: HeapItem): void {
    this.items.push(item);
    this.bubbleUp(this.items.length - 1);
  }

  replaceRoot(item: HeapItem): void {
    if (this.items.length === 0) {
      this.items.push(item);
      return;
    }

    this.items[0] = item;
    this.bubbleDown(0);
  }

  toArray(): HeapItem[] {
    return this.items.slice();
  }

  private bubbleUp(index: number): void {
    let current = index;
    while (current > 0) {
      const parent = Math.floor((current - 1) / 2);
      if (this.compare(this.items[current], this.items[parent]) >= 0) {
        return;
      }

      this.swap(current, parent);
      current = parent;
    }
  }

  private bubbleDown(index: number): void {
    let current = index;

    while (true) {
      const left = current * 2 + 1;
      const right = left + 1;
      let next = current;

      if (left < this.items.length && this.compare(this.items[left], this.items[next]) < 0) {
        next = left;
      }

      if (right < this.items.length && this.compare(this.items[right], this.items[next]) < 0) {
        next = right;
      }

      if (next === current) {
        return;
      }

      this.swap(current, next);
      current = next;
    }
  }

  private swap(left: number, right: number): void {
    const item = this.items[left];
    this.items[left] = this.items[right];
    this.items[right] = item;
  }
}
