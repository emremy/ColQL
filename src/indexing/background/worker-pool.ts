import * as os from "node:os";
import { ColQLError } from "../../errors";
import { ChunkTaskQueue } from "./chunk-task-queue";
import type {
  BackgroundChunkJob,
  BackgroundChunkTask,
  BackgroundJobId,
  BackgroundJobSnapshot,
  BackgroundTaskResult,
} from "./types";

const MAX_WORKER_COUNT = 32;
const DEFAULT_MAX_QUEUED_JOBS = 64;

type JobState = {
  readonly jobId: BackgroundJobId;
  readonly generation: number;
  readonly columnEpoch: number;
  status: BackgroundJobSnapshot["status"];
  queuedTasks: number;
  activeTasks: number;
  completedTasks: number;
  readonly totalTasks: number;
};

export type BackgroundTaskExecutor<TPayload, TResult> = (
  task: BackgroundChunkTask<TPayload>,
  workerId: number,
) => Promise<TResult> | TResult;

export type BackgroundWorkerPoolOptions<TPayload, TResult> = {
  readonly workerCount?: number;
  readonly availableParallelism?: () => number;
  readonly executor: BackgroundTaskExecutor<TPayload, TResult>;
  readonly maxQueuedJobs?: number;
  readonly onTaskComplete?: (
    result: BackgroundTaskResult<TResult>,
    task: BackgroundChunkTask<TPayload>,
  ) => void;
  readonly onTaskFailure?: (
    error: unknown,
    task: BackgroundChunkTask<TPayload>,
  ) => void;
  readonly onJobQueued?: (snapshot: BackgroundJobSnapshot) => void;
  readonly onJobStarted?: (snapshot: BackgroundJobSnapshot) => void;
  readonly onJobCompleted?: (snapshot: BackgroundJobSnapshot) => void;
  readonly onJobCancelled?: (snapshot: BackgroundJobSnapshot) => void;
  readonly onJobFailed?: (
    snapshot: BackgroundJobSnapshot,
    error: unknown,
  ) => void;
};

export class BackgroundWorkerPool<TPayload = unknown, TResult = unknown> {
  private readonly queue = new ChunkTaskQueue<TPayload>();
  private readonly jobs = new Map<BackgroundJobId, JobState>();
  private readonly workerCountValue: number;
  private readonly maxQueuedJobs: number;
  private readonly executor: BackgroundTaskExecutor<TPayload, TResult>;
  private activeWorkers = 0;
  private createdWorkers = 0;
  private disposed = false;
  private staleResults = 0;

  constructor(private readonly options: BackgroundWorkerPoolOptions<TPayload, TResult>) {
    this.workerCountValue = normalizeWorkerCount(
      options.workerCount ?? defaultWorkerCount(options.availableParallelism),
    );
    this.maxQueuedJobs = options.maxQueuedJobs ?? DEFAULT_MAX_QUEUED_JOBS;
    this.executor = options.executor;
  }

  get workerCount(): number {
    return this.workerCountValue;
  }

  get logicalWorkersCreated(): number {
    return this.createdWorkers;
  }

  get staleResultCount(): number {
    return this.staleResults;
  }

  submitJob(job: BackgroundChunkJob<TPayload>): BackgroundJobSnapshot {
    this.assertNotDisposed();
    if (this.jobs.has(job.jobId)) {
      throw new ColQLError(
        "COLQL_INVALID_INDEX_OPERATION",
        `Background index job "${job.jobId}" already exists.`,
      );
    }

    if (this.jobs.size >= this.maxQueuedJobs) {
      throw new ColQLError(
        "COLQL_INVALID_LIMIT",
        `Too many queued background index jobs: maximum is ${this.maxQueuedJobs}.`,
      );
    }

    const tasks = job.tasks.map((task) => ({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));
    const state: JobState = {
      jobId: job.jobId,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
      status: tasks.length === 0 ? "completed" : "queued",
      queuedTasks: tasks.length,
      activeTasks: 0,
      completedTasks: 0,
      totalTasks: tasks.length,
    };
    this.jobs.set(job.jobId, state);
    this.queue.enqueue(tasks);
    this.options.onJobQueued?.(this.snapshotFor(state));

    if (tasks.length === 0) {
      this.options.onJobCompleted?.(this.snapshotFor(state));
      return this.snapshotFor(state);
    }

    this.pump();
    return this.snapshotFor(state);
  }

  cancelJob(jobId: BackgroundJobId): void {
    const state = this.jobs.get(jobId);
    if (state === undefined || state.status === "completed" || state.status === "failed") {
      return;
    }

    state.queuedTasks = Math.max(0, state.queuedTasks - this.queue.cancelJob(jobId));
    state.status = "cancelled";
    this.options.onJobCancelled?.(this.snapshotFor(state));
  }

  invalidateJob(jobId: BackgroundJobId): void {
    this.cancelJob(jobId);
  }

  snapshot(jobId: BackgroundJobId): BackgroundJobSnapshot | undefined {
    const state = this.jobs.get(jobId);
    return state === undefined ? undefined : this.snapshotFor(state);
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }

    this.disposed = true;
    this.queue.clear();
    for (const state of this.jobs.values()) {
      if (state.status === "queued" || state.status === "rebuilding") {
        state.queuedTasks = 0;
        state.status = "cancelled";
        this.options.onJobCancelled?.(this.snapshotFor(state));
      }
    }
  }

  private pump(): void {
    while (!this.disposed && this.activeWorkers < this.workerCountValue) {
      const task = this.queue.dequeue();
      if (task === undefined) {
        return;
      }

      const state = this.jobs.get(task.jobId);
      if (state === undefined || state.status === "cancelled" || state.status === "failed") {
        this.staleResults += 1;
        continue;
      }

      if (this.createdWorkers < this.workerCountValue) {
        this.createdWorkers += 1;
      }

      if (state.status === "queued") {
        state.status = "rebuilding";
        this.options.onJobStarted?.(this.snapshotFor(state));
      }

      state.queuedTasks -= 1;
      state.activeTasks += 1;
      this.activeWorkers += 1;
      const workerId = this.activeWorkers <= this.createdWorkers
        ? this.activeWorkers - 1
        : this.createdWorkers - 1;

      Promise.resolve()
        .then(() => this.executor(task, workerId))
        .then(
          (result) => this.completeTask(task, result),
          (error) => this.failTask(task, error),
        );
    }
  }

  private completeTask(task: BackgroundChunkTask<TPayload>, result: TResult): void {
    this.activeWorkers -= 1;
    const state = this.jobs.get(task.jobId);
    if (
      state === undefined ||
      state.status === "cancelled" ||
      state.status === "failed" ||
      state.generation !== task.generation ||
      state.columnEpoch !== task.columnEpoch
    ) {
      this.staleResults += 1;
      this.pump();
      return;
    }

    state.activeTasks -= 1;
    state.completedTasks += 1;
    this.options.onTaskComplete?.(
      {
        jobId: task.jobId,
        taskId: task.taskId,
        generation: task.generation,
        columnEpoch: task.columnEpoch,
        result,
      },
      task,
    );

    if (state.completedTasks === state.totalTasks) {
      state.status = "completed";
      this.options.onJobCompleted?.(this.snapshotFor(state));
    }

    this.pump();
  }

  private failTask(task: BackgroundChunkTask<TPayload>, error: unknown): void {
    this.activeWorkers -= 1;
    const state = this.jobs.get(task.jobId);
    if (state === undefined || state.status === "cancelled") {
      this.staleResults += 1;
      this.pump();
      return;
    }

    state.activeTasks = Math.max(0, state.activeTasks - 1);
    state.queuedTasks = Math.max(0, state.queuedTasks - this.queue.cancelJob(task.jobId));
    state.status = "failed";
    this.options.onTaskFailure?.(error, task);
    this.options.onJobFailed?.(this.snapshotFor(state), error);
    this.pump();
  }

  private snapshotFor(state: JobState): BackgroundJobSnapshot {
    return {
      jobId: state.jobId,
      status: state.status,
      queuedTasks: state.queuedTasks,
      activeTasks: state.activeTasks,
      completedTasks: state.completedTasks,
      totalTasks: state.totalTasks,
      generation: state.generation,
      columnEpoch: state.columnEpoch,
    };
  }

  private assertNotDisposed(): void {
    if (this.disposed) {
      throw new ColQLError(
        "COLQL_UNSUPPORTED_OPERATION",
        "Background worker pool has been disposed.",
      );
    }
  }
}

export function defaultWorkerCount(availableParallelism = availableCpuCount): number {
  return normalizeWorkerCount(Math.max(1, Math.min(4, availableParallelism() - 1)));
}

export function normalizeWorkerCount(workerCount: number): number {
  if (!Number.isInteger(workerCount) || workerCount < 1) {
    throw new ColQLError(
      "COLQL_INVALID_LIMIT",
      `Invalid worker count: expected positive integer, received ${String(workerCount)}.`,
    );
  }

  return Math.min(workerCount, MAX_WORKER_COUNT);
}

function availableCpuCount(): number {
  if (typeof os.availableParallelism === "function") {
    return os.availableParallelism();
  }

  return os.cpus().length;
}
