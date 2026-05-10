import { parentPort } from "node:worker_threads";
import { ColQLError } from "../../errors";
import { executeEqualityChunkRebuild } from "./equality-rebuild";
import { executeSortedChunkRebuild } from "./sorted-rebuild";
import type {
  BackgroundWorkerFailure,
  BackgroundWorkerRequest,
  BackgroundWorkerResponse,
  BackgroundWorkerTask,
  BackgroundWorkerTaskResult,
} from "./worker-protocol";

if (parentPort === null) {
  throw new Error("ColQL background worker entry requires worker_threads parentPort.");
}

parentPort.on("message", (message: BackgroundWorkerRequest) => {
  if (message.type === "ping") {
    parentPort?.postMessage({
      type: "pong",
      requestId: message.requestId,
    } satisfies BackgroundWorkerResponse);
    return;
  }

  if (message.type !== "task") {
    parentPort?.postMessage(failure(message, "COLQL_INVALID_INDEX_OPERATION", "Unknown background worker message."));
    return;
  }

  try {
    assertSharedInput(message.task);
    const result = executeWorkerTask(message.task);
    parentPort?.postMessage(
      {
        type: "success",
        requestId: message.requestId,
        jobId: message.task.jobId,
        taskId: message.task.taskId,
        result,
      } satisfies BackgroundWorkerResponse,
      transferListForResult(result),
    );
  } catch (error) {
    parentPort?.postMessage(failure(
      message,
      error instanceof ColQLError ? error.code : "COLQL_BACKGROUND_WORKER_FAILED",
      error instanceof Error ? error.message : String(error),
    ));
  }
});

function executeWorkerTask(task: BackgroundWorkerTask): BackgroundWorkerTaskResult {
  if (task.indexKind === "equality") {
    return executeEqualityChunkRebuild(task);
  }

  if (task.indexKind === "sorted") {
    return executeSortedChunkRebuild(task);
  }

  throw new ColQLError(
    "COLQL_UNSUPPORTED_OPERATION",
    `Unsupported background worker index kind: ${String(task.indexKind)}.`,
  );
}

function assertSharedInput(task: BackgroundWorkerTask): void {
  const { descriptor } = task.payload;
  if (
    !descriptor.zeroCopyEligible ||
    descriptor.bufferKind !== "shared-array-buffer" ||
    descriptor.sharedBuffer === undefined
  ) {
    throw new ColQLError(
      "COLQL_UNSUPPORTED_OPERATION",
      "Background worker tasks require SharedArrayBuffer-backed chunk input.",
    );
  }
}

function transferListForResult(result: BackgroundWorkerTaskResult): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  if ("keyBuffer" in result) {
    buffers.add(result.keyBuffer);
    buffers.add(result.offsetsBuffer);
    buffers.add(result.rowIdsBuffer);
  } else {
    buffers.add(result.valuesBuffer);
    buffers.add(result.rowIdsBuffer);
  }

  return [...buffers].filter((buffer) => !(buffer instanceof SharedArrayBuffer));
}

function failure(
  message: BackgroundWorkerRequest,
  errorCode: string,
  errorMessage: string,
): BackgroundWorkerFailure {
  return {
    type: "failure",
    requestId: message.requestId,
    ...(message.type === "task"
      ? {
          jobId: message.task.jobId,
          taskId: message.task.taskId,
        }
      : {}),
    errorCode,
    message: errorMessage,
  };
}
