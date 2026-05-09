import type {
  EqualityBackgroundRebuildTaskPayload,
  EqualityEncodedChunkResult,
} from "./equality-rebuild";
import type {
  SortedBackgroundRebuildTaskPayload,
  SortedEncodedChunkResult,
} from "./sorted-rebuild";
import type { BackgroundChunkTask } from "./types";

export type BackgroundWorkerTaskPayload =
  | EqualityBackgroundRebuildTaskPayload
  | SortedBackgroundRebuildTaskPayload;

export type BackgroundWorkerTask =
  | BackgroundChunkTask<EqualityBackgroundRebuildTaskPayload>
  | BackgroundChunkTask<SortedBackgroundRebuildTaskPayload>;

export type BackgroundWorkerTaskResult =
  | EqualityEncodedChunkResult
  | SortedEncodedChunkResult;

export type BackgroundWorkerTaskRequest = {
  readonly type: "task";
  readonly requestId: number;
  readonly task: BackgroundWorkerTask;
};

export type BackgroundWorkerPingRequest = {
  readonly type: "ping";
  readonly requestId: number;
};

export type BackgroundWorkerRequest =
  | BackgroundWorkerTaskRequest
  | BackgroundWorkerPingRequest;

export type BackgroundWorkerTaskSuccess = {
  readonly type: "success";
  readonly requestId: number;
  readonly jobId: string;
  readonly taskId: string;
  readonly result: BackgroundWorkerTaskResult;
};

export type BackgroundWorkerPingSuccess = {
  readonly type: "pong";
  readonly requestId: number;
};

export type BackgroundWorkerFailure = {
  readonly type: "failure";
  readonly requestId: number;
  readonly jobId?: string;
  readonly taskId?: string;
  readonly errorCode: string;
  readonly message: string;
};

export type BackgroundWorkerResponse =
  | BackgroundWorkerTaskSuccess
  | BackgroundWorkerPingSuccess
  | BackgroundWorkerFailure;
