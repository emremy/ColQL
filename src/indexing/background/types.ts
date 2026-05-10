import type { IndexLifecycleKind } from "../index-manager";

export type BackgroundJobId = string;
export type BackgroundTaskId = string;

export type BackgroundChunkTask<TPayload = unknown> = {
  readonly jobId: BackgroundJobId;
  readonly taskId: BackgroundTaskId;
  readonly indexId: string;
  readonly indexKind: IndexLifecycleKind;
  readonly columnName: string;
  readonly generation: number;
  readonly columnEpoch: number;
  readonly chunkIndex: number;
  readonly payload: TPayload;
};

export type BackgroundChunkJob<TPayload = unknown> = {
  readonly jobId: BackgroundJobId;
  readonly indexId: string;
  readonly indexKind: IndexLifecycleKind;
  readonly columnName: string;
  readonly generation: number;
  readonly columnEpoch: number;
  readonly tasks: readonly Omit<
    BackgroundChunkTask<TPayload>,
    | "jobId"
    | "indexId"
    | "indexKind"
    | "columnName"
    | "generation"
    | "columnEpoch"
  >[];
};

export type BackgroundTaskResult<TResult = unknown> = {
  readonly jobId: BackgroundJobId;
  readonly taskId: BackgroundTaskId;
  readonly generation: number;
  readonly columnEpoch: number;
  readonly result: TResult;
};

export type BackgroundJobStatus =
  | "queued"
  | "rebuilding"
  | "completed"
  | "cancelled"
  | "failed";

export type BackgroundJobSnapshot = {
  readonly jobId: BackgroundJobId;
  readonly status: BackgroundJobStatus;
  readonly queuedTasks: number;
  readonly activeTasks: number;
  readonly completedTasks: number;
  readonly totalTasks: number;
  readonly generation: number;
  readonly columnEpoch: number;
};
