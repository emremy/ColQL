export type IndexLifecycleState =
  | "fresh"
  | "dirty"
  | "queued"
  | "rebuilding"
  | "failed";

export type IndexDirtyReason =
  | "insert"
  | "insertMany"
  | "update:indexed-column"
  | "delete:row-position-shift"
  | "restore"
  | "worker-failed"
  | "config-change";

export type IndexLifecycleSnapshot = {
  readonly state: IndexLifecycleState;
  readonly generation: number;
  readonly dirtyReason?: IndexDirtyReason;
  readonly failureReason?: string;
};

export class IndexLifecycle {
  private stateValue: IndexLifecycleState;
  private generationValue: number;
  private dirtyReasonValue: IndexDirtyReason | undefined;
  private failureReasonValue: string | undefined;

  constructor(initialState: IndexLifecycleState = "fresh", initialGeneration = 0) {
    this.stateValue = initialState;
    this.generationValue = initialGeneration;
  }

  get state(): IndexLifecycleState {
    return this.stateValue;
  }

  get generation(): number {
    return this.generationValue;
  }

  get dirtyReason(): IndexDirtyReason | undefined {
    return this.dirtyReasonValue;
  }

  snapshot(): IndexLifecycleSnapshot {
    return {
      state: this.stateValue,
      generation: this.generationValue,
      ...(this.dirtyReasonValue !== undefined
        ? { dirtyReason: this.dirtyReasonValue }
        : {}),
      ...(this.failureReasonValue !== undefined
        ? { failureReason: this.failureReasonValue }
        : {}),
    };
  }

  markFresh(): void {
    this.stateValue = "fresh";
    this.dirtyReasonValue = undefined;
    this.failureReasonValue = undefined;
  }

  markDirty(reason: IndexDirtyReason, incrementGeneration = true): void {
    this.stateValue = "dirty";
    this.dirtyReasonValue = reason;
    this.failureReasonValue = undefined;
    if (incrementGeneration) {
      this.bumpGeneration();
    }
  }

  markQueued(reason?: IndexDirtyReason): void {
    this.stateValue = "queued";
    this.dirtyReasonValue = reason;
    this.failureReasonValue = undefined;
  }

  markRebuilding(reason?: IndexDirtyReason): void {
    this.stateValue = "rebuilding";
    this.dirtyReasonValue = reason;
    this.failureReasonValue = undefined;
  }

  markFailed(failureReason?: string): void {
    this.stateValue = "failed";
    this.dirtyReasonValue = "worker-failed";
    this.failureReasonValue = failureReason;
    this.bumpGeneration();
  }

  bumpGeneration(): void {
    this.generationValue += 1;
  }
}
