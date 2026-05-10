import type { IndexLifecycleState } from "../index-lifecycle";
import type { BackgroundJobId } from "./types";

export type BackgroundApplyIndexKind = "equality" | "sorted";

export type BackgroundApplyMetadata = {
  readonly jobId: BackgroundJobId;
  readonly indexId: string;
  readonly indexKind: BackgroundApplyIndexKind;
  readonly columnName: string;
  readonly generation: number;
  readonly columnEpoch: number;
};

export type BackgroundApplyValidationReason =
  | "missing-index"
  | "job-id"
  | "index-id"
  | "index-kind"
  | "column"
  | "generation"
  | "column-epoch"
  | "lifecycle-state";

export type BackgroundApplyValidationResult =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly reason: BackgroundApplyValidationReason;
    };

export type BackgroundApplyValidationInput = {
  readonly metadata: BackgroundApplyMetadata;
  readonly expectedJobId?: BackgroundJobId;
  readonly expectedIndexId: string;
  readonly expectedIndexKind: BackgroundApplyIndexKind;
  readonly expectedColumnName: string;
  readonly liveGeneration: number;
  readonly liveColumnEpoch: number;
  readonly lifecycleState: IndexLifecycleState;
  readonly allowedLifecycleStates: readonly IndexLifecycleState[];
  readonly indexExists: boolean;
};

export function validateBackgroundApply(
  input: BackgroundApplyValidationInput,
): BackgroundApplyValidationResult {
  if (!input.indexExists) {
    return { ok: false, reason: "missing-index" };
  }

  if (input.expectedJobId === undefined || input.metadata.jobId !== input.expectedJobId) {
    return { ok: false, reason: "job-id" };
  }

  if (input.metadata.indexId !== input.expectedIndexId) {
    return { ok: false, reason: "index-id" };
  }

  if (input.metadata.indexKind !== input.expectedIndexKind) {
    return { ok: false, reason: "index-kind" };
  }

  if (input.metadata.columnName !== input.expectedColumnName) {
    return { ok: false, reason: "column" };
  }

  if (input.metadata.generation !== input.liveGeneration) {
    return { ok: false, reason: "generation" };
  }

  if (input.metadata.columnEpoch !== input.liveColumnEpoch) {
    return { ok: false, reason: "column-epoch" };
  }

  if (!input.allowedLifecycleStates.includes(input.lifecycleState)) {
    return { ok: false, reason: "lifecycle-state" };
  }

  return { ok: true };
}
