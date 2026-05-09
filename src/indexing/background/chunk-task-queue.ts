import type { BackgroundChunkTask, BackgroundJobId } from "./types";

type QueuedJob<TPayload> = {
  readonly jobId: BackgroundJobId;
  readonly tasks: BackgroundChunkTask<TPayload>[];
};

export class ChunkTaskQueue<TPayload = unknown> {
  private readonly jobs = new Map<BackgroundJobId, QueuedJob<TPayload>>();
  private readonly jobOrder: BackgroundJobId[] = [];
  private nextJobOffset = 0;

  get size(): number {
    let total = 0;
    for (const job of this.jobs.values()) {
      total += job.tasks.length;
    }
    return total;
  }

  get jobCount(): number {
    return this.jobs.size;
  }

  enqueue(tasks: readonly BackgroundChunkTask<TPayload>[]): void {
    for (const task of tasks) {
      let job = this.jobs.get(task.jobId);
      if (job === undefined) {
        job = { jobId: task.jobId, tasks: [] };
        this.jobs.set(task.jobId, job);
        this.jobOrder.push(task.jobId);
      }

      job.tasks.push(task);
    }
  }

  dequeue(): BackgroundChunkTask<TPayload> | undefined {
    if (this.jobOrder.length === 0) {
      return undefined;
    }

    const attempts = this.jobOrder.length;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (this.nextJobOffset >= this.jobOrder.length) {
        this.nextJobOffset = 0;
      }

      const jobId = this.jobOrder[this.nextJobOffset];
      const job = this.jobs.get(jobId);
      if (job === undefined || job.tasks.length === 0) {
        this.removeJobAt(this.nextJobOffset);
        continue;
      }

      const task = job.tasks.shift();
      if (job.tasks.length === 0) {
        this.removeJobAt(this.nextJobOffset);
      } else {
        this.nextJobOffset = (this.nextJobOffset + 1) % this.jobOrder.length;
      }

      return task;
    }

    return undefined;
  }

  cancelJob(jobId: BackgroundJobId): number {
    const job = this.jobs.get(jobId);
    if (job === undefined) {
      return 0;
    }

    const removed = job.tasks.length;
    this.jobs.delete(jobId);
    const orderIndex = this.jobOrder.indexOf(jobId);
    if (orderIndex !== -1) {
      this.removeJobAt(orderIndex);
    }

    return removed;
  }

  clear(): void {
    this.jobs.clear();
    this.jobOrder.length = 0;
    this.nextJobOffset = 0;
  }

  private removeJobAt(index: number): void {
    this.jobOrder.splice(index, 1);
    if (this.jobOrder.length === 0) {
      this.nextJobOffset = 0;
      return;
    }

    if (this.nextJobOffset > index) {
      this.nextJobOffset -= 1;
      return;
    }

    if (this.nextJobOffset >= this.jobOrder.length) {
      this.nextJobOffset = 0;
    }
  }
}
