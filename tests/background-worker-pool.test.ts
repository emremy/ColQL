import { describe, expect, it } from "vitest";
import { column } from "../src";
import { ChunkTaskQueue } from "../src/indexing/background/chunk-task-queue";
import {
  BackgroundWorkerPool,
  defaultWorkerCount,
  normalizeWorkerCount,
} from "../src/indexing/background/worker-pool";
import type { BackgroundChunkJob, BackgroundChunkTask } from "../src/indexing/background/types";
import { IndexManager } from "../src/indexing/index-manager";

type MockPayload = { readonly value: number };
type MockResult = { readonly value: number; readonly workerId: number };

function createJob(
  jobId: string,
  values: readonly number[],
  generation = 1,
  columnEpoch = 1,
): BackgroundChunkJob<MockPayload> {
  return {
    jobId,
    indexId: `index:${jobId}`,
    indexKind: "equality",
    columnName: "status",
    generation,
    columnEpoch,
    tasks: values.map((value, index) => ({
      taskId: `${jobId}:${index}`,
      chunkIndex: index,
      payload: { value },
    })),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("background worker pool infrastructure", () => {
  it("computes default worker count from available parallelism", () => {
    expect(defaultWorkerCount(() => 1)).toBe(1);
    expect(defaultWorkerCount(() => 2)).toBe(1);
    expect(defaultWorkerCount(() => 4)).toBe(3);
    expect(defaultWorkerCount(() => 64)).toBe(4);
  });

  it("validates and caps configured worker counts", () => {
    expect(normalizeWorkerCount(1)).toBe(1);
    expect(normalizeWorkerCount(4)).toBe(4);
    expect(normalizeWorkerCount(999)).toBe(32);
    expect(() => normalizeWorkerCount(0)).toThrow(/Invalid worker count/);
    expect(() => normalizeWorkerCount(1.5)).toThrow(/Invalid worker count/);
  });

  it("schedules a single job FIFO through a bounded pool", async () => {
    const started: string[] = [];
    const completed: string[] = [];
    const gates = new Map<string, ReturnType<typeof deferred<MockResult>>>();
    const pool = new BackgroundWorkerPool<MockPayload, MockResult>({
      workerCount: 2,
      executor: (task, workerId) => {
        started.push(task.taskId);
        const gate = deferred<MockResult>();
        gates.set(task.taskId, gate);
        return gate.promise.then((result) => ({ ...result, workerId }));
      },
      onTaskComplete: (result) => completed.push(result.taskId),
    });

    pool.submitJob(createJob("a", [1, 2, 3, 4]));
    await flushMicrotasks();

    expect(pool.workerCount).toBe(2);
    expect(pool.logicalWorkersCreated).toBe(2);
    expect(started).toEqual(["a:0", "a:1"]);

    gates.get("a:0")?.resolve({ value: 1, workerId: 0 });
    await flushMicrotasks();
    expect(completed).toEqual(["a:0"]);
    expect(started).toEqual(["a:0", "a:1", "a:2"]);

    gates.get("a:1")?.resolve({ value: 2, workerId: 1 });
    gates.get("a:2")?.resolve({ value: 3, workerId: 0 });
    await flushMicrotasks();
    expect(started).toEqual(["a:0", "a:1", "a:2", "a:3"]);

    gates.get("a:3")?.resolve({ value: 4, workerId: 1 });
    await flushMicrotasks();
    expect(completed).toEqual(["a:0", "a:1", "a:2", "a:3"]);
    expect(pool.snapshot("a")).toEqual(expect.objectContaining({
      status: "completed",
      completedTasks: 4,
      totalTasks: 4,
    }));
  });

  it("round-robins queued tasks across jobs", () => {
    const queue = new ChunkTaskQueue<MockPayload>();
    const first = createJob("a", [1, 2, 3]);
    const second = createJob("b", [4, 5, 6]);
    queue.enqueue(first.tasks.map((task) => ({
      ...task,
      jobId: first.jobId,
      indexId: first.indexId,
      indexKind: first.indexKind,
      columnName: first.columnName,
      generation: first.generation,
      columnEpoch: first.columnEpoch,
    })));
    queue.enqueue(second.tasks.map((task) => ({
      ...task,
      jobId: second.jobId,
      indexId: second.indexId,
      indexKind: second.indexKind,
      columnName: second.columnName,
      generation: second.generation,
      columnEpoch: second.columnEpoch,
    })));

    expect([
      queue.dequeue()?.taskId,
      queue.dequeue()?.taskId,
      queue.dequeue()?.taskId,
      queue.dequeue()?.taskId,
      queue.dequeue()?.taskId,
      queue.dequeue()?.taskId,
    ]).toEqual(["a:0", "b:0", "a:1", "b:1", "a:2", "b:2"]);
  });

  it("ignores stale job results after cancellation", async () => {
    const gate = deferred<MockResult>();
    const completed: string[] = [];
    const pool = new BackgroundWorkerPool<MockPayload, MockResult>({
      workerCount: 1,
      executor: () => gate.promise,
      onTaskComplete: (result) => completed.push(result.taskId),
    });

    pool.submitJob(createJob("stale", [1]));
    await flushMicrotasks();
    pool.invalidateJob("stale");
    gate.resolve({ value: 1, workerId: 0 });
    await flushMicrotasks();

    expect(completed).toEqual([]);
    expect(pool.staleResultCount).toBe(1);
    expect(pool.snapshot("stale")).toEqual(expect.objectContaining({
      status: "cancelled",
    }));
  });

  it("marks jobs failed when mock task execution rejects", async () => {
    const failures: string[] = [];
    const failedJobs: string[] = [];
    const pool = new BackgroundWorkerPool<MockPayload, MockResult>({
      workerCount: 2,
      executor: (task) => {
        if (task.taskId === "fail:0") {
          throw new Error("mock failure");
        }
        return { value: task.payload.value, workerId: 0 };
      },
      onTaskFailure: (_error, task) => failures.push(task.taskId),
      onJobFailed: (snapshot) => failedJobs.push(snapshot.jobId),
    });

    pool.submitJob(createJob("fail", [1, 2, 3]));
    await flushMicrotasks();

    expect(failures).toEqual(["fail:0"]);
    expect(failedJobs).toEqual(["fail"]);
    expect(pool.snapshot("fail")).toEqual(expect.objectContaining({
      status: "failed",
    }));
  });

  it("rejects new jobs after dispose and cancels queued work", async () => {
    const gate = deferred<MockResult>();
    const cancelled: string[] = [];
    const pool = new BackgroundWorkerPool<MockPayload, MockResult>({
      workerCount: 1,
      executor: () => gate.promise,
      onJobCancelled: (snapshot) => cancelled.push(snapshot.jobId),
    });

    pool.submitJob(createJob("dispose", [1, 2]));
    await flushMicrotasks();
    pool.dispose();

    expect(cancelled).toEqual(["dispose"]);
    expect(pool.snapshot("dispose")).toEqual(expect.objectContaining({
      status: "cancelled",
    }));
    expect(() => pool.submitJob(createJob("late", [1]))).toThrow(/disposed/);

    gate.resolve({ value: 1, workerId: 0 });
    await flushMicrotasks();
    expect(pool.staleResultCount).toBe(1);
  });

  it("can drive mock lifecycle transitions without changing query behavior", async () => {
    const schema = {
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
    };
    const rows = [
      { id: 1, status: 0 },
      { id: 2, status: 1 },
    ];
    const manager = new IndexManager();
    manager.create("status", schema.status, rows.length, (rowIndex) => rows[rowIndex].status);
    const pool = new BackgroundWorkerPool<MockPayload, MockResult>({
      workerCount: 1,
      executor: (task, workerId) => ({ value: task.payload.value, workerId }),
      onJobQueued: () => manager.markLifecycleQueued("equality", "status", "update:indexed-column"),
      onJobStarted: () => manager.markLifecycleRebuilding("equality", "status", "update:indexed-column"),
      onJobFailed: () => manager.markLifecycleFailed("equality", "status", "mock failure"),
    });

    pool.submitJob(createJob("lifecycle", [1]));
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "rebuilding",
    });
    await flushMicrotasks();

    expect(pool.snapshot("lifecycle")).toEqual(expect.objectContaining({
      status: "completed",
    }));
  });
});
