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
  readonly dirtyReason?: IndexDirtyReason;
  readonly failureReason?: string;
};

export class IndexLifecycle {
  private stateValue: IndexLifecycleState;
  private dirtyReasonValue: IndexDirtyReason | undefined;
  private failureReasonValue: string | undefined;

  constructor(initialState: IndexLifecycleState = "fresh") {
    this.stateValue = initialState;
  }

  get state(): IndexLifecycleState {
    return this.stateValue;
  }

  get dirtyReason(): IndexDirtyReason | undefined {
    return this.dirtyReasonValue;
  }

  snapshot(): IndexLifecycleSnapshot {
    return {
      state: this.stateValue,
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

  markDirty(reason: IndexDirtyReason): void {
    this.stateValue = "dirty";
    this.dirtyReasonValue = reason;
    this.failureReasonValue = undefined;
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
  }
}
