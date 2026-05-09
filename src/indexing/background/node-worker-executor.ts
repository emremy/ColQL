import { Worker } from "node:worker_threads";
import { fileURLToPath, pathToFileURL } from "node:url";
import { dirname, join } from "node:path";
import { ColQLError } from "../../errors";
import type { BackgroundTaskExecutor } from "./worker-pool";
import type {
  BackgroundWorkerFailure,
  BackgroundWorkerRequest,
  BackgroundWorkerResponse,
  BackgroundWorkerTask,
  BackgroundWorkerTaskPayload,
  BackgroundWorkerTaskResult,
} from "./worker-protocol";
import type { BackgroundChunkTask } from "./types";

type PendingRequest = {
  readonly resolve: (result: BackgroundWorkerTaskResult | "pong") => void;
  readonly reject: (error: unknown) => void;
};

type WorkerSlot = {
  readonly worker: Worker;
  readonly pending: Map<number, PendingRequest>;
};

export type NodeBackgroundWorkerExecutorOptions = {
  readonly workerCount?: number;
  readonly workerUrl?: URL;
};

export class NodeBackgroundWorkerExecutor {
  private readonly workerCount: number;
  private readonly workerUrl: URL;
  private readonly slots: WorkerSlot[] = [];
  private nextRequestId = 1;
  private disposed = false;

  constructor(options: NodeBackgroundWorkerExecutorOptions = {}) {
    this.workerCount = normalizeNodeWorkerCount(options.workerCount ?? 1);
    this.workerUrl = options.workerUrl ?? resolveDefaultWorkerEntryUrl();
  }

  readonly execute: BackgroundTaskExecutor<BackgroundWorkerTaskPayload, BackgroundWorkerTaskResult> = (
    task: BackgroundChunkTask<BackgroundWorkerTaskPayload>,
  ) => {
    this.assertNotDisposed();
    this.assertSharedInput(task);
    return this.postRequest({
      type: "task",
      requestId: this.nextRequestId++,
      task: task as BackgroundWorkerTask,
    }) as Promise<BackgroundWorkerTaskResult>;
  };

  ping(): Promise<"pong"> {
    this.assertNotDisposed();
    return this.postRequest({
      type: "ping",
      requestId: this.nextRequestId++,
    }) as Promise<"pong">;
  }

  async dispose(): Promise<void> {
    if (this.disposed) {
      return;
    }

    this.disposed = true;
    const slots = this.slots.splice(0);
    await Promise.all(slots.map(async (slot) => {
      for (const [requestId, pending] of slot.pending) {
        pending.reject(new ColQLError(
          "COLQL_INVALID_INDEX_OPERATION",
          `Background worker request ${String(requestId)} was cancelled during disposal.`,
        ));
      }
      slot.pending.clear();
      await slot.worker.terminate();
    }));
  }

  private postRequest(
    request: BackgroundWorkerRequest,
  ): Promise<BackgroundWorkerTaskResult | "pong"> {
    const slot = this.leastBusySlot();
    return new Promise((resolve, reject) => {
      slot.pending.set(request.requestId, { resolve, reject });
      try {
        slot.worker.postMessage(request);
      } catch (error) {
        slot.pending.delete(request.requestId);
        reject(error);
      }
    });
  }

  private leastBusySlot(): WorkerSlot {
    if (this.slots.length < this.workerCount) {
      const slot = this.createSlot();
      this.slots.push(slot);
      return slot;
    }

    let selected = this.slots[0];
    for (let index = 1; index < this.slots.length; index += 1) {
      if (this.slots[index].pending.size < selected.pending.size) {
        selected = this.slots[index];
      }
    }
    return selected;
  }

  private createSlot(): WorkerSlot {
    const pending = new Map<number, PendingRequest>();
    const worker = new Worker(this.workerUrl);
    const slot = { worker, pending };

    worker.on("message", (response: BackgroundWorkerResponse) => {
      const request = pending.get(response.requestId);
      if (request === undefined) {
        return;
      }

      pending.delete(response.requestId);
      if (response.type === "failure") {
        request.reject(errorFromWorker(response));
        return;
      }

      request.resolve(response.type === "pong" ? "pong" : response.result);
    });

    worker.on("error", (error) => {
      rejectAll(pending, error);
    });

    worker.on("exit", (code) => {
      if (code === 0 || this.disposed) {
        return;
      }

      rejectAll(pending, new ColQLError(
        "COLQL_INVALID_INDEX_OPERATION",
        `Background worker exited with code ${String(code)}.`,
      ));
    });

    return slot;
  }

  private assertSharedInput(task: BackgroundChunkTask<BackgroundWorkerTaskPayload>): void {
    const { descriptor } = task.payload;
    if (
      !descriptor.zeroCopyEligible ||
      descriptor.bufferKind !== "shared-array-buffer" ||
      descriptor.sharedBuffer === undefined
    ) {
      throw new ColQLError(
        "COLQL_UNSUPPORTED_OPERATION",
        "Real background workers require SharedArrayBuffer-backed chunk input.",
      );
    }
  }

  private assertNotDisposed(): void {
    if (this.disposed) {
      throw new ColQLError("COLQL_INVALID_INDEX_OPERATION", "Background worker executor is disposed.");
    }
  }
}

export function resolveDefaultWorkerEntryUrl(): URL {
  const currentFile = typeof __filename === "string"
    ? __filename
    : fileURLToPath(import.meta.url);
  const extension = currentFile.endsWith(".mjs") ? ".mjs" : ".js";
  return pathToFileURL(join(dirname(currentFile), `worker-entry${extension}`));
}

function normalizeNodeWorkerCount(workerCount: number): number {
  if (!Number.isInteger(workerCount) || workerCount < 1) {
    throw new ColQLError(
      "COLQL_INVALID_LIMIT",
      `Invalid worker count: expected positive integer, received ${String(workerCount)}.`,
    );
  }

  return Math.min(workerCount, 32);
}

function errorFromWorker(response: BackgroundWorkerFailure): ColQLError {
  return new ColQLError(
    response.errorCode,
    response.message,
    {
      jobId: response.jobId,
      taskId: response.taskId,
    },
  );
}

function rejectAll(pending: Map<number, PendingRequest>, error: unknown): void {
  for (const request of pending.values()) {
    request.reject(error);
  }
  pending.clear();
}
