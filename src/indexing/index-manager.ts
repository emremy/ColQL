import { ColQLError } from "../errors";
import type { ColumnDefinition, QueryExplainReasonCode } from "../types";
import {
  validateBackgroundApply,
  type BackgroundApplyValidationReason,
} from "./background/apply-guard";
import {
  mergeEqualityEncodedResults,
  type EqualityBackgroundRebuildJobMetadata,
  type EqualityEncodedChunkResult,
} from "./background/equality-rebuild";
import {
  mergeSortedEncodedResults,
  type SortedBackgroundRebuildJobMetadata,
  type SortedEncodedChunkResult,
} from "./background/sorted-rebuild";
import { EqualityIndex, type EqualityIndexStats, type IndexableValue } from "./equality-index";
import { IndexLifecycle, type IndexDirtyReason, type IndexLifecycleSnapshot, type IndexLifecycleState } from "./index-lifecycle";
import { SortedIndex, type RangeOperator, type SortedIndexStats } from "./sorted-index";
import { UniqueIndex, type UniqueIndexStats, type UniqueIndexValue } from "./unique-index";

const DEFAULT_INDEX_SELECTIVITY_THRESHOLD = 0.4;

export type IndexFilter = {
  readonly columnName: string;
  readonly operator: string;
  readonly value: number | boolean | readonly (number | boolean)[];
};

export type IndexCandidatePlan = {
  readonly source: "equality" | "sorted";
  readonly column: string;
  readonly operator: "=" | "in" | RangeOperator;
  readonly candidateCount: number;
  readonly rowCount: number;
  readonly threshold: number;
  readonly rowIndexes: Iterable<number>;
};

export type IndexFallbackReason =
  | "dirty-index"
  | "queued-index"
  | "rebuilding-index"
  | "failed-index"
  | "background-disabled"
  | "not-zero-copy-capable"
  | "memory-budget"
  | "no-usable-index";

export type IndexDebugPlan =
  | {
      readonly mode: "index";
      readonly source: "equality" | "sorted";
      readonly column: string;
      readonly operator: "=" | "in" | RangeOperator;
      readonly candidateCount: number;
      readonly rowCount: number;
      readonly threshold: number;
    }
  | {
      readonly mode: "scan";
      readonly source?: "equality" | "sorted";
      readonly column?: string;
      readonly operator?: "=" | "in" | RangeOperator;
      readonly candidateCount?: number;
      readonly rowCount: number;
      readonly threshold: number;
    };

export type IndexExplainPlan =
  | {
      readonly mode: "index";
      readonly source: "equality" | "sorted";
      readonly column: string;
      readonly operator: "=" | "in" | RangeOperator;
      readonly candidateCount?: number;
      readonly rowCount: number;
      readonly threshold: number;
      readonly indexState: IndexLifecycleState;
      readonly fallbackReason?: IndexFallbackReason;
      readonly reasonCode?: QueryExplainReasonCode;
      readonly backgroundRebuildState?: Exclude<IndexLifecycleState, "fresh" | "dirty">;
    }
  | {
      readonly mode: "scan";
      readonly source?: "equality" | "sorted";
      readonly column?: string;
      readonly operator?: "=" | "in" | RangeOperator;
      readonly candidateCount?: number;
      readonly rowCount: number;
      readonly threshold: number;
      readonly reasonCode: QueryExplainReasonCode;
      readonly indexState?: IndexLifecycleState;
      readonly fallbackReason?: IndexFallbackReason;
      readonly backgroundRebuildState?: Exclude<IndexLifecycleState, "fresh" | "dirty">;
    };

type EqualityCandidateEstimate = {
  readonly source: "equality";
  readonly column: string;
  readonly operator: "=" | "in";
  readonly index: EqualityIndex;
  readonly value: IndexableValue | readonly IndexableValue[];
  readonly candidateCount: number;
};

type SortedCandidateEstimate = {
  readonly source: "sorted";
  readonly column: string;
  readonly operator: RangeOperator;
  readonly index: SortedIndex;
  readonly value: number;
  readonly candidateCount: number;
  readonly bounds: ReturnType<SortedIndex["bounds"]>;
};

type CandidateEstimate = EqualityCandidateEstimate | SortedCandidateEstimate;

type DirtyCandidateEstimate = {
  readonly source: "equality" | "sorted";
  readonly column: string;
  readonly operator: "=" | "in" | RangeOperator;
  readonly state: IndexLifecycleState;
  readonly fallbackReason: IndexFallbackReason;
};

export type IndexLifecycleKind = "equality" | "sorted" | "unique";

export type IndexRuntimeSnapshot = IndexLifecycleSnapshot & {
  readonly columnEpoch: number;
};

export type BackgroundIndexRebuildMode = "sync" | "mock-background";

export type IndexDiagnosticsSnapshot = IndexRuntimeSnapshot & {
  readonly kind: IndexLifecycleKind;
  readonly column: string;
  readonly queued: boolean;
  readonly rebuilding: boolean;
  readonly jobId?: string;
  readonly chunksDone?: number;
  readonly chunksTotal?: number;
  readonly workerCount?: number;
  readonly lastRebuildMode?: BackgroundIndexRebuildMode;
  readonly staleResultsDiscarded: number;
  readonly fallbackCount: number;
  readonly lastErrorCode?: string;
  readonly lastErrorMessage?: string;
  readonly lastDiscardReason?: BackgroundApplyValidationReason;
};

export type EqualityBackgroundApplyResult =
  | "applied"
  | "stale"
  | "missing-index";

export type SortedBackgroundApplyResult =
  | "applied"
  | "stale"
  | "missing-index";

export class IndexManager {
  private readonly indexesByColumn = new Map<string, EqualityIndex>();
  private readonly sortedIndexesByColumn = new Map<string, SortedIndex>();
  private readonly uniqueIndexesByColumn = new Map<string, UniqueIndex>();
  private readonly dirtyEqualityColumns = new Set<string>();
  private readonly equalityLifecyclesByColumn = new Map<string, IndexLifecycle>();
  private readonly equalityBackgroundJobsByColumn = new Map<string, string>();
  private readonly sortedBackgroundJobsByColumn = new Map<string, string>();
  private backgroundStaleResultsDiscarded = 0;
  private lastBackgroundApplyDiscardReason: BackgroundApplyValidationReason | undefined;
  private readonly backgroundDiagnosticsByIndex = new Map<string, {
    jobId?: string;
    chunksDone?: number;
    chunksTotal?: number;
    workerCount?: number;
    lastRebuildMode?: BackgroundIndexRebuildMode;
    staleResultsDiscarded: number;
    fallbackCount: number;
    lastErrorCode?: string;
    lastErrorMessage?: string;
    lastDiscardReason?: BackgroundApplyValidationReason;
  }>();
  private readonly columnEpochsByColumn = new Map<string, number>();

  create(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.indexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_INDEX_EXISTS", `Index already exists for column "${columnName}".`);
    }

    this.assertEqualitySupported(columnName, definition);
    const index = new EqualityIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
    }

    this.indexesByColumn.set(columnName, index);
    this.equalityLifecyclesByColumn.set(columnName, new IndexLifecycle());
    this.ensureColumnEpoch(columnName);
  }

  drop(columnName: string): void {
    if (!this.indexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_INDEX_NOT_FOUND", `Index not found for column "${columnName}".`);
    }
    this.dirtyEqualityColumns.delete(columnName);
    this.equalityLifecyclesByColumn.delete(columnName);
    this.equalityBackgroundJobsByColumn.delete(columnName);
  }

  has(columnName: string): boolean {
    return this.indexesByColumn.has(columnName);
  }

  list(): string[] {
    return [...this.indexesByColumn.keys()];
  }

  stats(): EqualityIndexStats[] {
    return this.list().map((columnName) => this.indexesByColumn.get(columnName)?.stats()).filter((stats): stats is EqualityIndexStats => stats !== undefined);
  }

  rebuild(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.indexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_INDEX_NOT_FOUND", `Index not found for column "${columnName}".`);
    }

    if (this.dirtyEqualityColumns.has(columnName)) {
      this.indexesByColumn.set(columnName, this.buildEqualityIndex(columnName, rowCount, readComparableValue));
      this.dirtyEqualityColumns.delete(columnName);
      this.backgroundDiagnostics("equality", columnName).lastRebuildMode = "sync";
      this.equalityLifecycle(columnName).markFresh();
      return;
    }

    this.indexesByColumn.set(columnName, this.buildEqualityIndex(columnName, rowCount, readComparableValue));
    this.backgroundDiagnostics("equality", columnName).lastRebuildMode = "sync";
    this.equalityLifecycle(columnName).markFresh();
  }

  rebuildAll(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    this.rebuildEqualityIndexes(rowCount, readComparableValue);
    this.rebuildSortedIndexes(rowCount, readNumericValue);
  }

  addRow(columnName: string, value: number | boolean, rowIndex: number): void {
    const index = this.indexesByColumn.get(columnName);
    if (index === undefined) {
      return;
    }

    if (!this.sortedIndexesByColumn.has(columnName)) {
      this.bumpColumnEpoch(columnName);
    }
    this.equalityLifecycle(columnName).bumpGeneration();

    if (this.dirtyEqualityColumns.has(columnName)) {
      return;
    }

    index.add(value as IndexableValue, rowIndex);
  }

  createSorted(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    if (this.sortedIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_SORTED_INDEX_EXISTS", `Sorted index already exists for column "${columnName}".`);
    }

    this.assertSortedSupported(columnName, definition);
    const index = new SortedIndex(columnName);
    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, columnName));
    this.sortedIndexesByColumn.set(columnName, index);
    this.ensureColumnEpoch(columnName);
  }

  dropSorted(columnName: string): void {
    if (!this.sortedIndexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_SORTED_INDEX_NOT_FOUND", `Sorted index not found for column "${columnName}".`);
    }
    this.sortedBackgroundJobsByColumn.delete(columnName);
  }

  hasSorted(columnName: string): boolean {
    return this.sortedIndexesByColumn.has(columnName);
  }

  listSorted(): string[] {
    return [...this.sortedIndexesByColumn.keys()];
  }

  sortedStats(): SortedIndexStats[] {
    return this.listSorted()
      .map((columnName) => this.sortedIndexesByColumn.get(columnName)?.stats())
      .filter((stats): stats is SortedIndexStats => stats !== undefined);
  }

  createUnique(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.uniqueIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_EXISTS", `Unique index already exists for column "${columnName}".`);
    }

    this.assertUniqueSupported(columnName, definition);
    this.uniqueIndexesByColumn.set(columnName, this.buildUniqueIndex(columnName, rowCount, readComparableValue));
    this.ensureColumnEpoch(columnName);
  }

  dropUnique(columnName: string): void {
    if (!this.uniqueIndexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }
  }

  hasUnique(columnName: string): boolean {
    return this.uniqueIndexesByColumn.has(columnName);
  }

  listUnique(): string[] {
    return [...this.uniqueIndexesByColumn.keys()];
  }

  uniqueStats(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): UniqueIndexStats[] {
    this.rebuildUniqueIfDirty(rowCount, readComparableValue);
    return this.listUnique()
      .map((columnName) => this.uniqueIndexesByColumn.get(columnName)?.stats())
      .filter((stats): stats is UniqueIndexStats => stats !== undefined);
  }

  /**
   * @internal Lifecycle diagnostics for v0.6 background-indexing phases. This
   * is intentionally not exported from the package root.
   */
  lifecycleSnapshot(
    kind: IndexLifecycleKind,
    columnName: string,
  ): IndexRuntimeSnapshot | undefined {
    if (kind === "equality") {
      return this.snapshotWithColumnEpoch(columnName, this.equalityLifecyclesByColumn.get(columnName));
    }

    if (kind === "sorted") {
      const snapshot = this.sortedIndexesByColumn.get(columnName)?.lifecycleSnapshot();
      return snapshot === undefined ? undefined : { ...snapshot, columnEpoch: this.columnEpoch(columnName) };
    }

    const snapshot = this.uniqueIndexesByColumn.get(columnName)?.lifecycleSnapshot();
    return snapshot === undefined ? undefined : { ...snapshot, columnEpoch: this.columnEpoch(columnName) };
  }

  /**
   * @internal Cheap, unstable diagnostics for v0.6 background-indexing phases.
   * This intentionally avoids per-chunk details and is not exported from the
   * package root as a stable public API.
   */
  diagnostics(): IndexDiagnosticsSnapshot[] {
    const snapshots: IndexDiagnosticsSnapshot[] = [];
    for (const column of this.list()) {
      const snapshot = this.lifecycleSnapshot("equality", column);
      if (snapshot !== undefined) {
        snapshots.push(this.diagnosticsSnapshot("equality", column, snapshot));
      }
    }

    for (const column of this.listSorted()) {
      const snapshot = this.lifecycleSnapshot("sorted", column);
      if (snapshot !== undefined) {
        snapshots.push(this.diagnosticsSnapshot("sorted", column, snapshot));
      }
    }

    for (const column of this.listUnique()) {
      const snapshot = this.lifecycleSnapshot("unique", column);
      if (snapshot !== undefined) {
        snapshots.push(this.diagnosticsSnapshot("unique", column, snapshot));
      }
    }

    return snapshots;
  }

  /**
   * @internal Represents future worker failure states without changing current
   * query behavior. Background workers are not implemented in this phase.
   */
  markLifecycleFailed(
    kind: IndexLifecycleKind,
    columnName: string,
    failureReason?: string,
  ): void {
    if (kind === "equality") {
      if (!this.indexesByColumn.has(columnName)) return;
      this.dirtyEqualityColumns.add(columnName);
      this.equalityLifecycle(columnName).markFailed(failureReason);
      this.bumpColumnEpoch(columnName);
      return;
    }

    if (kind === "sorted") {
      this.sortedIndexesByColumn.get(columnName)?.markFailed(failureReason);
      if (this.sortedIndexesByColumn.has(columnName)) {
        this.bumpColumnEpoch(columnName);
      }
      return;
    }

    const index = this.uniqueIndexesByColumn.get(columnName);
    if (index !== undefined) {
      index.markFailed(failureReason);
      this.bumpColumnEpoch(columnName);
    }
  }

  /**
   * @internal Represents future queued worker states without scheduling
   * background work in this phase.
   */
  markLifecycleQueued(
    kind: IndexLifecycleKind,
    columnName: string,
    reason?: IndexDirtyReason,
  ): void {
    if (kind === "equality") {
      if (!this.indexesByColumn.has(columnName)) return;
      this.dirtyEqualityColumns.add(columnName);
      this.equalityLifecycle(columnName).markQueued(reason);
      return;
    }

    if (kind === "sorted") {
      this.sortedIndexesByColumn.get(columnName)?.markQueued(reason);
      return;
    }

    this.uniqueIndexesByColumn.get(columnName)?.markQueued(reason);
  }

  /**
   * @internal Represents future rebuilding worker states without scheduling
   * background work in this phase.
   */
  markLifecycleRebuilding(
    kind: IndexLifecycleKind,
    columnName: string,
    reason?: IndexDirtyReason,
  ): void {
    if (kind === "equality") {
      if (!this.indexesByColumn.has(columnName)) return;
      this.dirtyEqualityColumns.add(columnName);
      this.equalityLifecycle(columnName).markRebuilding(reason);
      return;
    }

    if (kind === "sorted") {
      this.sortedIndexesByColumn.get(columnName)?.markRebuilding(reason);
      return;
    }

    this.uniqueIndexesByColumn.get(columnName)?.markRebuilding(reason);
  }

  /**
   * @internal Starts an equality background rebuild job without scheduling it.
   * This is used by v0.6 background infrastructure tests and future schedulers.
   */
  startEqualityBackgroundRebuild(
    metadata: EqualityBackgroundRebuildJobMetadata,
    reason?: IndexDirtyReason,
    diagnostics?: { readonly chunksTotal?: number; readonly workerCount?: number },
  ): boolean {
    if (!this.validateEqualityBackgroundMetadata(metadata)) {
      return false;
    }

    if (this.equalityLifecycle(metadata.columnName).state !== "dirty") {
      return false;
    }

    this.dirtyEqualityColumns.add(metadata.columnName);
    this.equalityBackgroundJobsByColumn.set(metadata.columnName, metadata.jobId);
    this.recordBackgroundJobQueued("equality", metadata.columnName, metadata.jobId, diagnostics);
    this.equalityLifecycle(metadata.columnName).markQueued(reason);
    return true;
  }

  /**
   * @internal Marks a previously accepted equality background job as active.
   */
  markEqualityBackgroundRebuildStarted(
    metadata: EqualityBackgroundRebuildJobMetadata,
    reason?: IndexDirtyReason,
  ): boolean {
    if (
      !this.validateEqualityBackgroundMetadata(metadata) ||
      !this.validateEqualityBackgroundJobId(metadata)
    ) {
      return false;
    }

    const lifecycle = this.equalityLifecycle(metadata.columnName);
    if (lifecycle.state !== "queued") {
      return false;
    }

    this.dirtyEqualityColumns.add(metadata.columnName);
    this.recordBackgroundJobStarted("equality", metadata.columnName);
    lifecycle.markRebuilding(reason);
    return true;
  }

  /**
   * @internal Applies a completed equality background rebuild only if the
   * captured generation and column epoch still match the live index state.
   */
  completeEqualityBackgroundRebuild(
    metadata: EqualityBackgroundRebuildJobMetadata,
    results: readonly EqualityEncodedChunkResult[],
  ): EqualityBackgroundApplyResult {
    const lifecycle = this.equalityLifecyclesByColumn.get(metadata.columnName);
    const validation = validateBackgroundApply({
      metadata,
      expectedJobId: this.equalityBackgroundJobsByColumn.get(metadata.columnName),
      expectedIndexId: `equality:${metadata.columnName}`,
      expectedIndexKind: "equality",
      expectedColumnName: metadata.columnName,
      liveGeneration: lifecycle?.generation ?? -1,
      liveColumnEpoch: this.columnEpoch(metadata.columnName),
      lifecycleState: lifecycle?.state ?? "failed",
      allowedLifecycleStates: ["queued", "rebuilding"],
      indexExists: this.indexesByColumn.has(metadata.columnName),
    });
    if (!validation.ok) {
      return this.discardBackgroundApply(validation.reason, "equality", metadata.columnName);
    }

    let index: EqualityIndex;
    try {
      for (const result of results) {
        if (result.columnName !== metadata.columnName) {
          throw new ColQLError(
            "COLQL_INVALID_INDEX_OPERATION",
            `Equality background rebuild result column "${result.columnName}" does not match "${metadata.columnName}".`,
          );
        }
      }
      index = mergeEqualityEncodedResults(metadata.columnName, results);
    } catch (error) {
      this.dirtyEqualityColumns.add(metadata.columnName);
      this.equalityBackgroundJobsByColumn.delete(metadata.columnName);
      this.recordBackgroundJobFailed(
        "equality",
        metadata.columnName,
        error instanceof Error ? error.message : String(error),
      );
      lifecycle?.markFailed(
        error instanceof Error ? error.message : String(error),
      );
      throw error;
    }

    this.indexesByColumn.set(metadata.columnName, index);
    this.dirtyEqualityColumns.delete(metadata.columnName);
    this.equalityBackgroundJobsByColumn.delete(metadata.columnName);
    this.recordBackgroundJobApplied("equality", metadata.columnName);
    lifecycle?.markFresh();
    return "applied";
  }

  /**
   * @internal Marks an equality background rebuild failed if the job still
   * matches the live generation/epoch. Stale failures are ignored.
   */
  failEqualityBackgroundRebuild(
    metadata: EqualityBackgroundRebuildJobMetadata,
    failureReason?: string,
  ): boolean {
    if (
      !this.validateEqualityBackgroundMetadata(metadata) ||
      !this.validateEqualityBackgroundJobId(metadata)
    ) {
      return false;
    }

    this.dirtyEqualityColumns.add(metadata.columnName);
    this.equalityBackgroundJobsByColumn.delete(metadata.columnName);
    this.recordBackgroundJobFailed("equality", metadata.columnName, failureReason);
    this.equalityLifecycle(metadata.columnName).markFailed(failureReason);
    return true;
  }

  /**
   * @internal Starts a sorted background rebuild job without scheduling it.
   * This is used by v0.6 background infrastructure tests and future schedulers.
   */
  startSortedBackgroundRebuild(
    metadata: SortedBackgroundRebuildJobMetadata,
    reason?: IndexDirtyReason,
    diagnostics?: { readonly chunksTotal?: number; readonly workerCount?: number },
  ): boolean {
    if (!this.validateSortedBackgroundMetadata(metadata)) {
      return false;
    }

    const index = this.sortedIndexesByColumn.get(metadata.columnName);
    if (index === undefined || index.lifecycleSnapshot().state !== "dirty") {
      return false;
    }

    this.sortedBackgroundJobsByColumn.set(metadata.columnName, metadata.jobId);
    this.recordBackgroundJobQueued("sorted", metadata.columnName, metadata.jobId, diagnostics);
    index.markQueued(reason);
    return true;
  }

  /**
   * @internal Marks a previously accepted sorted background job as active.
   */
  markSortedBackgroundRebuildStarted(
    metadata: SortedBackgroundRebuildJobMetadata,
    reason?: IndexDirtyReason,
  ): boolean {
    if (
      !this.validateSortedBackgroundMetadata(metadata) ||
      !this.validateSortedBackgroundJobId(metadata)
    ) {
      return false;
    }

    const index = this.sortedIndexesByColumn.get(metadata.columnName);
    if (index === undefined || index.lifecycleSnapshot().state !== "queued") {
      return false;
    }

    this.recordBackgroundJobStarted("sorted", metadata.columnName);
    index.markRebuilding(reason);
    return true;
  }

  /**
   * @internal Applies a completed sorted background rebuild only if the
   * captured generation and column epoch still match the live index state.
   */
  completeSortedBackgroundRebuild(
    metadata: SortedBackgroundRebuildJobMetadata,
    results: readonly SortedEncodedChunkResult[],
  ): SortedBackgroundApplyResult {
    const index = this.sortedIndexesByColumn.get(metadata.columnName);
    const snapshot = this.lifecycleSnapshot("sorted", metadata.columnName);
    const validation = validateBackgroundApply({
      metadata,
      expectedJobId: this.sortedBackgroundJobsByColumn.get(metadata.columnName),
      expectedIndexId: `sorted:${metadata.columnName}`,
      expectedIndexKind: "sorted",
      expectedColumnName: metadata.columnName,
      liveGeneration: snapshot?.generation ?? -1,
      liveColumnEpoch: snapshot?.columnEpoch ?? -1,
      lifecycleState: snapshot?.state ?? "failed",
      allowedLifecycleStates: ["queued", "rebuilding"],
      indexExists: index !== undefined,
    });
    if (!validation.ok) {
      return this.discardBackgroundApply(validation.reason, "sorted", metadata.columnName);
    }
    if (index === undefined) {
      return this.discardBackgroundApply("missing-index", "sorted", metadata.columnName);
    }

    let rowIds: Uint32Array;
    try {
      for (const result of results) {
        if (result.columnName !== metadata.columnName) {
          throw new ColQLError(
            "COLQL_INVALID_INDEX_OPERATION",
            `Sorted background rebuild result column "${result.columnName}" does not match "${metadata.columnName}".`,
          );
        }
      }
      rowIds = mergeSortedEncodedResults(results, metadata.rowCount);
    } catch (error) {
      this.sortedBackgroundJobsByColumn.delete(metadata.columnName);
      this.recordBackgroundJobFailed(
        "sorted",
        metadata.columnName,
        error instanceof Error ? error.message : String(error),
      );
      index.markFailed(error instanceof Error ? error.message : String(error));
      throw error;
    }

    index.replaceSortedRows(rowIds);
    this.sortedBackgroundJobsByColumn.delete(metadata.columnName);
    this.recordBackgroundJobApplied("sorted", metadata.columnName);
    return "applied";
  }

  /**
   * @internal Marks a sorted background rebuild failed if the job still
   * matches the live generation/epoch. Stale failures are ignored.
   */
  failSortedBackgroundRebuild(
    metadata: SortedBackgroundRebuildJobMetadata,
    failureReason?: string,
  ): boolean {
    if (
      !this.validateSortedBackgroundMetadata(metadata) ||
      !this.validateSortedBackgroundJobId(metadata)
    ) {
      return false;
    }

    this.sortedBackgroundJobsByColumn.delete(metadata.columnName);
    this.recordBackgroundJobFailed("sorted", metadata.columnName, failureReason);
    this.sortedIndexesByColumn.get(metadata.columnName)?.markFailed(failureReason);
    return true;
  }

  rebuildUnique(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.uniqueIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }

    const generation = this.uniqueIndexesByColumn.get(columnName)?.lifecycleSnapshot().generation ?? 0;
    this.uniqueIndexesByColumn.set(columnName, this.buildUniqueIndex(columnName, rowCount, readComparableValue, generation));
    this.ensureColumnEpoch(columnName);
  }

  rebuildUniqueIndexes(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    const columns = this.listUnique();
    const next = new Map<string, UniqueIndex>();
    for (const column of columns) {
      const generation = this.uniqueIndexesByColumn.get(column)?.lifecycleSnapshot().generation ?? 0;
      next.set(column, this.buildUniqueIndex(column, rowCount, readComparableValue, generation));
    }

    this.uniqueIndexesByColumn.clear();
    for (const [column, index] of next) {
      this.uniqueIndexesByColumn.set(column, index);
    }
  }

  addUniqueRow(columnName: string, value: number | boolean, rowIndex: number): void {
    const index = this.uniqueIndexesByColumn.get(columnName);
    if (index === undefined || index.isDirty()) {
      if (index !== undefined) {
        if (!this.indexesByColumn.has(columnName) && !this.sortedIndexesByColumn.has(columnName)) {
          this.bumpColumnEpoch(columnName);
        }
        index.bumpGeneration();
      }
      return;
    }

    if (!this.indexesByColumn.has(columnName) && !this.sortedIndexesByColumn.has(columnName)) {
      this.bumpColumnEpoch(columnName);
    }
    index.bumpGeneration();
    index.add(value as UniqueIndexValue, rowIndex);
  }

  uniqueLookup(
    columnName: string,
    value: number,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): number | undefined {
    const index = this.uniqueIndexesByColumn.get(columnName);
    if (index === undefined) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }

    if (index.isDirty()) {
      this.rebuildUnique(columnName, rowCount, readComparableValue);
    }

    return this.uniqueIndexesByColumn.get(columnName)?.get(value);
  }

  rebuildSorted(
    columnName: string,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    const index = this.sortedIndexesByColumn.get(columnName);
    if (index === undefined) {
      throw new ColQLError("COLQL_SORTED_INDEX_NOT_FOUND", `Sorted index not found for column "${columnName}".`);
    }

    index.markDirty("update:indexed-column", false);
    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, columnName));
    this.backgroundDiagnostics("sorted", columnName).lastRebuildMode = "sync";
  }

  markSortedDirty(reason: IndexDirtyReason = "update:indexed-column"): void {
    for (const [column, index] of this.sortedIndexesByColumn) {
      this.bumpColumnEpoch(column);
      index.markDirty(reason);
    }
  }

  markSortedColumnsDirty(
    columns: readonly string[],
    reason: IndexDirtyReason = "update:indexed-column",
  ): void {
    for (const column of columns) {
      const index = this.sortedIndexesByColumn.get(column);
      if (index !== undefined) {
        this.bumpColumnEpoch(column);
        index.markDirty(reason);
      }
    }
  }

  markEqualityDirty(reason: IndexDirtyReason = "update:indexed-column"): void {
    for (const column of this.indexesByColumn.keys()) {
      this.bumpColumnEpoch(column);
      this.dirtyEqualityColumns.add(column);
      this.equalityLifecycle(column).markDirty(reason);
    }
  }

  markEqualityColumnsDirty(
    columns: readonly string[],
    reason: IndexDirtyReason = "update:indexed-column",
  ): void {
    for (const column of columns) {
      if (this.indexesByColumn.has(column)) {
        this.bumpColumnEpoch(column);
        this.dirtyEqualityColumns.add(column);
        this.equalityLifecycle(column).markDirty(reason);
      }
    }
  }

  markPerformanceDirty(reason: IndexDirtyReason = "update:indexed-column"): void {
    const columns = new Set([...this.indexesByColumn.keys(), ...this.sortedIndexesByColumn.keys()]);
    for (const column of columns) {
      this.bumpColumnEpoch(column);
      if (this.indexesByColumn.has(column)) {
        this.dirtyEqualityColumns.add(column);
        this.equalityLifecycle(column).markDirty(reason);
      }
      this.sortedIndexesByColumn.get(column)?.markDirty(reason);
    }
  }

  markPerformanceColumnsDirty(
    columns: readonly string[],
    reason: IndexDirtyReason = "update:indexed-column",
  ): void {
    for (const column of columns) {
      const equalityIndex = this.indexesByColumn.has(column);
      const sortedIndex = this.sortedIndexesByColumn.get(column);
      if (!equalityIndex && sortedIndex === undefined) {
        continue;
      }

      this.bumpColumnEpoch(column);
      if (equalityIndex) {
        this.dirtyEqualityColumns.add(column);
        this.equalityLifecycle(column).markDirty(reason);
      }
      sortedIndex?.markDirty(reason);
    }
  }

  markUniqueDirty(
    columns?: readonly string[],
    reason: IndexDirtyReason = "update:indexed-column",
  ): void {
    const entries = columns === undefined
      ? [...this.uniqueIndexesByColumn.entries()]
      : columns
          .map((column) => [column, this.uniqueIndexesByColumn.get(column)] as const)
          .filter((entry): entry is readonly [string, UniqueIndex] => entry[1] !== undefined);

    for (const [column, index] of entries) {
      if (!this.indexesByColumn.has(column) && !this.sortedIndexesByColumn.has(column)) {
        this.bumpColumnEpoch(column);
      }
      index.markDirty(reason);
    }
  }

  markDeletedRow(rowIndex: number): void {
    this.markPerformanceDirty("delete:row-position-shift");
    for (const [column, index] of this.uniqueIndexesByColumn) {
      if (!this.indexesByColumn.has(column) && !this.sortedIndexesByColumn.has(column)) {
        this.bumpColumnEpoch(column);
      }
      index.bumpGeneration();
      index.deleteRow(rowIndex);
    }
  }

  markDirty(): void {
    this.markPerformanceDirty("delete:row-position-shift");
    this.markUniqueDirty(undefined, "delete:row-position-shift");
  }

  bestCandidate(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexCandidatePlan | undefined {
    this.rebuildEqualityIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters, rowCount, readNumericValue);
    if (best === undefined) {
      this.recordFallback(filters);
      return undefined;
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      this.recordFallback(filters);
      return undefined;
    }

    const rowIndexes = this.rowIndexesFor(best);

    return {
      source: best.source,
      column: best.column,
      operator: best.operator,
      candidateCount: best.candidateCount,
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      rowIndexes,
    };
  }

  debugPlan(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexDebugPlan {
    this.rebuildEqualityIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters, rowCount, readNumericValue);
    if (best === undefined) {
      return { mode: "scan", rowCount, threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD };
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      return {
        mode: "scan",
        source: best.source,
        column: best.column,
        operator: best.operator,
        candidateCount: best.candidateCount,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      };
    }

    return {
      mode: "index",
      source: best.source,
      column: best.column,
      operator: best.operator,
      candidateCount: best.candidateCount,
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
    };
  }

  explainPlan(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): IndexExplainPlan {
    if (filters.length === 0) {
      return {
        mode: "scan",
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
        reasonCode: "NO_PREDICATES",
      };
    }

    const dirty = this.unusableCandidateEstimate(filters, "dirty");
    if (dirty !== undefined) {
      return {
        mode: "index",
        source: dirty.source,
        column: dirty.column,
        operator: dirty.operator,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
        indexState: dirty.state,
        fallbackReason: dirty.fallbackReason,
        ...(dirty.state === "dirty"
          ? { reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION" as const }
          : {}),
        ...(this.backgroundRebuildState(dirty.state) !== undefined
          ? { backgroundRebuildState: this.backgroundRebuildState(dirty.state) }
          : {}),
      };
    }

    const best = this.bestCandidateEstimate(filters, rowCount, readNumericValue);
    if (best !== undefined) {
      const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
      if (best.candidateCount > threshold) {
        return {
          mode: "scan",
          source: best.source,
          column: best.column,
          operator: best.operator,
          candidateCount: best.candidateCount,
          rowCount,
          threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
          reasonCode: "INDEX_CANDIDATE_SET_TOO_LARGE",
        };
      }

      return {
        mode: "index",
        source: best.source,
        column: best.column,
        operator: best.operator,
        candidateCount: best.candidateCount,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
        indexState: "fresh",
      };
    }

    const unavailable = this.unusableCandidateEstimate(filters);
    if (unavailable !== undefined) {
      return {
        mode: "scan",
        source: unavailable.source,
        column: unavailable.column,
        operator: unavailable.operator,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
        reasonCode: this.reasonCodeForNoCandidate(filters),
        indexState: unavailable.state,
        fallbackReason: unavailable.fallbackReason,
        ...(this.backgroundRebuildState(unavailable.state) !== undefined
          ? { backgroundRebuildState: this.backgroundRebuildState(unavailable.state) }
          : {}),
      };
    }

    return {
      mode: "scan",
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      reasonCode: this.reasonCodeForNoCandidate(filters),
    };
  }

  private bestCandidateEstimate(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): CandidateEstimate | undefined {
    let best: CandidateEstimate | undefined;

    for (const filter of filters) {
      const equality = this.equalityEstimate(filter);
      if (equality !== undefined && (best === undefined || equality.candidateCount < best.candidateCount)) {
        best = equality;
      }

      const sorted = this.sortedEstimate(filter, rowCount, readNumericValue);
      if (sorted !== undefined && (best === undefined || sorted.candidateCount < best.candidateCount)) {
        best = sorted;
      }
    }

    return best;
  }

  private unusableCandidateEstimate(
    filters: readonly IndexFilter[],
    onlyState?: IndexLifecycleState,
  ): DirtyCandidateEstimate | undefined {
    for (const filter of filters) {
      if (
        (filter.operator === "=" || filter.operator === "in") &&
        this.dirtyEqualityColumns.has(filter.columnName) &&
        this.indexesByColumn.has(filter.columnName)
      ) {
        const state = this.equalityLifecycle(filter.columnName).state;
        if (onlyState !== undefined && state !== onlyState) {
          continue;
        }
        if (state === "fresh") {
          continue;
        }
        return {
          source: "equality",
          column: filter.columnName,
          operator: filter.operator,
          state,
          fallbackReason: this.fallbackReasonForIndexState(state),
        };
      }

      if (this.isRangeOperator(filter.operator)) {
        const index = this.sortedIndexesByColumn.get(filter.columnName);
        if (index !== undefined && index.isDirty()) {
          const state = index.lifecycleSnapshot().state;
          if (onlyState !== undefined && state !== onlyState) {
            continue;
          }
          if (state === "fresh") {
            continue;
          }
          return {
            source: "sorted",
            column: filter.columnName,
            operator: filter.operator,
            state,
            fallbackReason: this.fallbackReasonForIndexState(state),
          };
        }
      }
    }

    return undefined;
  }

  private reasonCodeForNoCandidate(
    filters: readonly IndexFilter[],
  ): QueryExplainReasonCode {
    if (
      filters.some(
        (filter) => filter.operator === "!=" || filter.operator === "not in",
      )
    ) {
      return "UNSUPPORTED_INDEX_OPERATOR";
    }

    if (
      filters.some(
        (filter) =>
          this.isRangeOperator(filter.operator) &&
          !this.sortedIndexesByColumn.has(filter.columnName),
      )
    ) {
      return "RANGE_QUERY_WITHOUT_SORTED_INDEX";
    }

    if (
      filters.some(
        (filter) =>
          (filter.operator === "=" || filter.operator === "in") &&
          !this.indexesByColumn.has(filter.columnName),
      )
    ) {
      return "NO_INDEX_FOR_COLUMN";
    }

    return "NO_INDEX_FOR_COLUMN";
  }

  private equalityEstimate(filter: IndexFilter): EqualityCandidateEstimate | undefined {
    const index = this.indexesByColumn.get(filter.columnName);
    if (index === undefined || (filter.operator !== "=" && filter.operator !== "in")) {
      return undefined;
    }

    if (this.equalityLifecycle(filter.columnName).state !== "fresh") {
      return undefined;
    }

    const candidateCount = filter.operator === "="
      ? index.count(filter.value as IndexableValue)
      : index.countIn(filter.value as readonly IndexableValue[]);

    return {
      source: "equality",
      column: filter.columnName,
      operator: filter.operator,
      index,
      value: filter.value as IndexableValue | readonly IndexableValue[],
      candidateCount,
    };
  }

  private sortedEstimate(
    filter: IndexFilter,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): SortedCandidateEstimate | undefined {
    const index = this.sortedIndexesByColumn.get(filter.columnName);
    if (index === undefined || !this.isRangeOperator(filter.operator) || typeof filter.value !== "number") {
      return undefined;
    }

    const state = index.lifecycleSnapshot().state;
    if (state !== "fresh" && state !== "dirty") {
      return undefined;
    }

    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, filter.columnName));
    const bounds = index.bounds(filter.operator, filter.value, (rowIndex) => readNumericValue(rowIndex, filter.columnName));

    return {
      source: "sorted",
      column: filter.columnName,
      operator: filter.operator,
      index,
      value: filter.value,
      candidateCount: bounds.count,
      bounds,
    };
  }

  private rowIndexesFor(best: CandidateEstimate): Iterable<number> {
    if (best.source === "equality") {
      return best.operator === "="
        ? best.index.get(best.value as IndexableValue)
        : best.index.getIn(best.value as readonly IndexableValue[]);
    }

    return best.index.rows(best.bounds);
  }

  private rebuildEqualityIfDirty(
    rowCount: number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.dirtyEqualityColumns.size === 0 || readComparableValue === undefined) {
      return;
    }

    for (const column of [...this.dirtyEqualityColumns]) {
      if (!this.indexesByColumn.has(column)) {
        this.dirtyEqualityColumns.delete(column);
        continue;
      }
      if (this.equalityLifecycle(column).state !== "dirty") {
        continue;
      }
      this.indexesByColumn.set(column, this.buildEqualityIndex(column, rowCount, readComparableValue));
      this.dirtyEqualityColumns.delete(column);
      this.backgroundDiagnostics("equality", column).lastRebuildMode = "sync";
      this.equalityLifecycle(column).markFresh();
    }
  }

  private rebuildEqualityIndexes(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    const columns = this.list();
    this.indexesByColumn.clear();
    for (const column of columns) {
      this.indexesByColumn.set(column, this.buildEqualityIndex(column, rowCount, readComparableValue));
      this.backgroundDiagnostics("equality", column).lastRebuildMode = "sync";
      this.equalityLifecycle(column).markFresh();
    }

    this.dirtyEqualityColumns.clear();
  }

  private rebuildUniqueIfDirty(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (![...this.uniqueIndexesByColumn.values()].some((index) => index.isDirty())) {
      return;
    }

    this.rebuildUniqueIndexes(rowCount, readComparableValue);
  }

  private buildUniqueIndex(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
    generation = 0,
  ): UniqueIndex {
    const index = new UniqueIndex(columnName, generation);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      try {
        index.add(readComparableValue(rowIndex, columnName) as UniqueIndexValue, rowIndex);
      } catch (error) {
        if (error instanceof ColQLError && error.code === "COLQL_DUPLICATE_KEY") {
          throw new ColQLError(
            "COLQL_DUPLICATE_KEY",
            `Duplicate key found while building unique index for column "${columnName}".`,
            { ...(error.details as object), columnName, operation: "rebuildUniqueIndex" },
          );
        }

        throw error;
      }
    }

    index.markFresh();
    return index;
  }

  private rebuildSortedIndexes(
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    for (const column of this.listSorted()) {
      this.rebuildSorted(column, rowCount, readNumericValue);
    }
  }

  private buildEqualityIndex(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): EqualityIndex {
    const index = new EqualityIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
    }

    return index;
  }

  private equalityLifecycle(columnName: string): IndexLifecycle {
    let lifecycle = this.equalityLifecyclesByColumn.get(columnName);
    if (lifecycle === undefined) {
      lifecycle = new IndexLifecycle();
      this.equalityLifecyclesByColumn.set(columnName, lifecycle);
    }

    return lifecycle;
  }

  private fallbackReasonForIndexState(state: IndexLifecycleState): IndexFallbackReason {
    switch (state) {
      case "dirty":
        return "dirty-index";
      case "queued":
        return "queued-index";
      case "rebuilding":
        return "rebuilding-index";
      case "failed":
        return "failed-index";
      case "fresh":
        return "no-usable-index";
    }
  }

  private backgroundRebuildState(
    state: IndexLifecycleState,
  ): Exclude<IndexLifecycleState, "fresh" | "dirty"> | undefined {
    return state === "queued" || state === "rebuilding" || state === "failed"
      ? state
      : undefined;
  }

  private validateEqualityBackgroundMetadata(
    metadata: EqualityBackgroundRebuildJobMetadata,
  ): boolean {
    if (
      metadata.indexKind !== "equality" ||
      metadata.indexId !== `equality:${metadata.columnName}` ||
      !this.indexesByColumn.has(metadata.columnName)
    ) {
      return false;
    }

    const snapshot = this.lifecycleSnapshot("equality", metadata.columnName);
    return snapshot !== undefined &&
      snapshot.generation === metadata.generation &&
      snapshot.columnEpoch === metadata.columnEpoch;
  }

  private validateEqualityBackgroundJobId(
    metadata: EqualityBackgroundRebuildJobMetadata,
  ): boolean {
    return this.equalityBackgroundJobsByColumn.get(metadata.columnName) === metadata.jobId;
  }

  private validateSortedBackgroundMetadata(
    metadata: SortedBackgroundRebuildJobMetadata,
  ): boolean {
    if (
      metadata.indexKind !== "sorted" ||
      metadata.indexId !== `sorted:${metadata.columnName}` ||
      !this.sortedIndexesByColumn.has(metadata.columnName)
    ) {
      return false;
    }

    const snapshot = this.lifecycleSnapshot("sorted", metadata.columnName);
    return snapshot !== undefined &&
      snapshot.generation === metadata.generation &&
      snapshot.columnEpoch === metadata.columnEpoch;
  }

  private validateSortedBackgroundJobId(
    metadata: SortedBackgroundRebuildJobMetadata,
  ): boolean {
    return this.sortedBackgroundJobsByColumn.get(metadata.columnName) === metadata.jobId;
  }

  private discardBackgroundApply(
    reason: BackgroundApplyValidationReason,
    kind?: IndexLifecycleKind,
    columnName?: string,
  ): "stale" | "missing-index" {
    this.backgroundStaleResultsDiscarded += 1;
    this.lastBackgroundApplyDiscardReason = reason;
    if (kind !== undefined && columnName !== undefined) {
      const diagnostics = this.backgroundDiagnostics(kind, columnName);
      diagnostics.staleResultsDiscarded += 1;
      diagnostics.lastDiscardReason = reason;
    }
    return reason === "missing-index" ? "missing-index" : "stale";
  }

  private diagnosticsSnapshot(
    kind: IndexLifecycleKind,
    column: string,
    snapshot: IndexRuntimeSnapshot,
  ): IndexDiagnosticsSnapshot {
    const diagnostics = this.backgroundDiagnosticsByIndex.get(this.indexKey(kind, column));
    return {
      kind,
      column,
      ...snapshot,
      queued: snapshot.state === "queued",
      rebuilding: snapshot.state === "rebuilding",
      ...(diagnostics?.jobId !== undefined ? { jobId: diagnostics.jobId } : {}),
      ...(diagnostics?.chunksDone !== undefined ? { chunksDone: diagnostics.chunksDone } : {}),
      ...(diagnostics?.chunksTotal !== undefined ? { chunksTotal: diagnostics.chunksTotal } : {}),
      ...(diagnostics?.workerCount !== undefined ? { workerCount: diagnostics.workerCount } : {}),
      ...(diagnostics?.lastRebuildMode !== undefined ? { lastRebuildMode: diagnostics.lastRebuildMode } : {}),
      staleResultsDiscarded: diagnostics?.staleResultsDiscarded ?? 0,
      fallbackCount: diagnostics?.fallbackCount ?? 0,
      ...(diagnostics?.lastErrorCode !== undefined ? { lastErrorCode: diagnostics.lastErrorCode } : {}),
      ...(diagnostics?.lastErrorMessage !== undefined ? { lastErrorMessage: diagnostics.lastErrorMessage } : {}),
      ...(diagnostics?.lastDiscardReason !== undefined ? { lastDiscardReason: diagnostics.lastDiscardReason } : {}),
    };
  }

  private recordBackgroundJobQueued(
    kind: IndexLifecycleKind,
    columnName: string,
    jobId: string,
    options?: { readonly chunksTotal?: number; readonly workerCount?: number },
  ): void {
    const diagnostics = this.backgroundDiagnostics(kind, columnName);
    diagnostics.jobId = jobId;
    diagnostics.chunksDone = 0;
    diagnostics.chunksTotal = options?.chunksTotal;
    diagnostics.workerCount = options?.workerCount;
    diagnostics.lastRebuildMode = "mock-background";
    diagnostics.lastErrorCode = undefined;
    diagnostics.lastErrorMessage = undefined;
  }

  private recordBackgroundJobStarted(
    kind: IndexLifecycleKind,
    columnName: string,
  ): void {
    this.backgroundDiagnostics(kind, columnName).lastRebuildMode = "mock-background";
  }

  private recordBackgroundJobApplied(
    kind: IndexLifecycleKind,
    columnName: string,
  ): void {
    const diagnostics = this.backgroundDiagnostics(kind, columnName);
    diagnostics.chunksDone = diagnostics.chunksTotal;
    diagnostics.jobId = undefined;
    diagnostics.lastRebuildMode = "mock-background";
    diagnostics.lastErrorCode = undefined;
    diagnostics.lastErrorMessage = undefined;
  }

  private recordBackgroundJobFailed(
    kind: IndexLifecycleKind,
    columnName: string,
    failureReason?: string,
  ): void {
    const diagnostics = this.backgroundDiagnostics(kind, columnName);
    diagnostics.jobId = undefined;
    diagnostics.lastRebuildMode = "mock-background";
    diagnostics.lastErrorCode = "COLQL_BACKGROUND_REBUILD_FAILED";
    diagnostics.lastErrorMessage = failureReason;
  }

  private recordFallback(filters: readonly IndexFilter[]): void {
    const unusable = this.unusableCandidateEstimate(filters);
    if (unusable === undefined) {
      return;
    }

    this.backgroundDiagnostics(unusable.source, unusable.column).fallbackCount += 1;
  }

  private backgroundDiagnostics(
    kind: IndexLifecycleKind,
    columnName: string,
  ): {
    jobId?: string;
    chunksDone?: number;
    chunksTotal?: number;
    workerCount?: number;
    lastRebuildMode?: BackgroundIndexRebuildMode;
    staleResultsDiscarded: number;
    fallbackCount: number;
    lastErrorCode?: string;
    lastErrorMessage?: string;
    lastDiscardReason?: BackgroundApplyValidationReason;
  } {
    const key = this.indexKey(kind, columnName);
    let diagnostics = this.backgroundDiagnosticsByIndex.get(key);
    if (diagnostics === undefined) {
      diagnostics = {
        staleResultsDiscarded: 0,
        fallbackCount: 0,
      };
      this.backgroundDiagnosticsByIndex.set(key, diagnostics);
    }

    return diagnostics;
  }

  private indexKey(kind: IndexLifecycleKind, columnName: string): string {
    return `${kind}:${columnName}`;
  }

  private snapshotWithColumnEpoch(
    columnName: string,
    lifecycle: IndexLifecycle | undefined,
  ): IndexRuntimeSnapshot | undefined {
    const snapshot = lifecycle?.snapshot();
    return snapshot === undefined ? undefined : { ...snapshot, columnEpoch: this.columnEpoch(columnName) };
  }

  private ensureColumnEpoch(columnName: string): void {
    if (!this.columnEpochsByColumn.has(columnName)) {
      this.columnEpochsByColumn.set(columnName, 0);
    }
  }

  private columnEpoch(columnName: string): number {
    return this.columnEpochsByColumn.get(columnName) ?? 0;
  }

  private bumpColumnEpoch(columnName: string): void {
    this.columnEpochsByColumn.set(columnName, this.columnEpoch(columnName) + 1);
  }

  private isRangeOperator(operator: string): operator is RangeOperator {
    return operator === ">" || operator === ">=" || operator === "<" || operator === "<=";
  }

  private assertEqualitySupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind === "boolean") {
      throw new ColQLError(
        "COLQL_INDEX_UNSUPPORTED_COLUMN",
        `Indexing is not supported for boolean column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }

  private assertSortedSupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind !== "numeric") {
      throw new ColQLError(
        "COLQL_SORTED_INDEX_UNSUPPORTED_COLUMN",
        `Sorted indexing is not supported for ${definition.kind} column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }

  private assertUniqueSupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind === "boolean") {
      throw new ColQLError(
        "COLQL_UNIQUE_INDEX_UNSUPPORTED",
        `Unique indexing is not supported for boolean column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }
}
