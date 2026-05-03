import { Query } from "./query";
import { column } from "./column";
import { BooleanColumnStorage } from "./storage/boolean-column";
import { DictionaryColumnStorage } from "./storage/dictionary-column";
import { NumericColumnStorage } from "./storage/numeric-column";
import { ColQLError } from "./errors";
import {
  IndexManager,
  type IndexCandidatePlan,
  type IndexDebugPlan,
  type IndexFilter,
} from "./indexing/index-manager";
import type { EqualityIndexStats } from "./indexing/equality-index";
import type { SortedIndexStats } from "./indexing/sorted-index";
import type { UniqueIndexStats } from "./indexing/unique-index";
import {
  assertColumnExists,
  assertNonNegativeInteger,
  assertOperator,
  assertOperatorAllowed,
  assertPositiveInteger,
  assertRowIndex,
  assertValidSchema,
  validateColumnValue,
} from "./validation";
import type {
  ColumnDefinition,
  ColumnStorage,
  ColumnValue,
  Filter,
  MutationResult,
  NumericColumnKey,
  ObjectWherePredicate,
  Operator,
  QueryInfo,
  QueryHook,
  RowPredicate,
  RowForSchema,
  Schema,
  SelectedRow,
  TableOptions,
  UniqueColumnKey,
} from "./types";

const DEFAULT_CAPACITY = 1024;
const SERIALIZATION_VERSION = "@colql/colql@0.2.0";
const SERIALIZATION_MAGIC = "COLQL003";
const MAGIC_BYTES = 8;
const HEADER_LENGTH_BYTES = 4;
const SERIALIZATION_PREFIX_BYTES = MAGIC_BYTES + HEADER_LENGTH_BYTES;

type SerializedColumnMeta = {
  readonly name: string;
  readonly kind: "numeric" | "dictionary" | "boolean";
  readonly type?: string;
  readonly values?: readonly string[];
  readonly byteLength: number;
  readonly byteOffset: number;
  readonly alignment: number;
};

type SerializedTableMeta = {
  readonly version: typeof SERIALIZATION_VERSION;
  readonly rowCount: number;
  readonly capacity: number;
  readonly columns: readonly SerializedColumnMeta[];
};

type StorageMap<TSchema extends Schema> = {
  [Key in keyof TSchema]: ColumnStorage<ColumnValue<TSchema[Key]>>;
};

type BulkDeleteStorage = {
  deleteMany(rowIndexes: readonly number[]): void;
};

type InternalFilter = {
  readonly columnName: string;
  readonly operator: Operator;
  readonly value: number | boolean | readonly (number | boolean)[];
};

type ValueForOperator<TValue, TOperator extends Operator> = TOperator extends
  | "in"
  | "not in"
  ? readonly TValue[]
  : TValue;

type PartialRowForSchema<TSchema extends Schema> = Partial<
  RowForSchema<TSchema>
>;

type TableConstructorOptions<TSchema extends Schema> = TableOptions & {
  readonly storages?: StorageMap<TSchema>;
  readonly rowCount?: number;
};

export class Table<TSchema extends Schema> {
  readonly schema: TSchema;
  private readonly storages: StorageMap<TSchema>;
  private currentRowCount = 0;
  private currentCapacity: number;
  private materializedRows = 0;
  private scannedRows = 0;
  private readonly indexManager = new IndexManager();
  private readonly onQuery?: QueryHook;

  constructor(schema: TSchema, options?: TableConstructorOptions<TSchema>);
  constructor(
    schema: TSchema,
    initialCapacity?: number,
    options?: TableConstructorOptions<TSchema>,
  );
  constructor(
    schema: TSchema,
    initialCapacityOrOptions:
      | number
      | TableConstructorOptions<TSchema> = DEFAULT_CAPACITY,
    options?: TableConstructorOptions<TSchema>,
  ) {
    assertValidSchema(schema);
    const initialCapacity =
      typeof initialCapacityOrOptions === "number"
        ? initialCapacityOrOptions
        : DEFAULT_CAPACITY;
    const tableOptions =
      typeof initialCapacityOrOptions === "number"
        ? options
        : initialCapacityOrOptions;

    if (!Number.isInteger(initialCapacity) || initialCapacity < 1) {
      throw new ColQLError(
        "COLQL_INVALID_LIMIT",
        `Invalid initial capacity: expected positive integer, received ${initialCapacity}.`,
      );
    }

    this.schema = schema;
    this.currentCapacity = initialCapacity;
    this.storages =
      tableOptions?.storages ?? this.createStorages(initialCapacity);
    this.currentRowCount = tableOptions?.rowCount ?? 0;
    this.onQuery = tableOptions?.onQuery;
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  get capacity(): number {
    return this.currentCapacity;
  }

  get materializedRowCount(): number {
    return this.materializedRows;
  }

  resetMaterializationCounter(): void {
    this.materializedRows = 0;
  }

  get scannedRowCount(): number {
    return this.scannedRows;
  }

  resetScanCounter(): void {
    this.scannedRows = 0;
  }

  recordRowScan(): void {
    this.scannedRows += 1;
  }

  recordRowScans(count: number): void {
    this.scannedRows += count;
  }

  hasQueryHook(): boolean {
    return this.onQuery !== undefined;
  }

  notifyQuery(info: QueryInfo): void {
    this.onQuery?.(info);
  }

  insert(row: RowForSchema<TSchema>): this {
    this.validateRow(row);
    this.assertUniqueInsert(row, "insert");
    this.ensureCapacity(this.currentRowCount + 1);
    this.appendRow(row);
    this.addRowToIndexes(this.currentRowCount - 1);
    return this;
  }

  delete(rowIndex: number): this {
    assertRowIndex(rowIndex, this.currentRowCount);
    this.deleteRowAt(rowIndex);
    this.indexManager.markDeletedRow(rowIndex);
    return this;
  }

  update(
    rowIndex: number,
    partialRow: PartialRowForSchema<TSchema>,
  ): MutationResult {
    assertRowIndex(rowIndex, this.currentRowCount);
    const values = this.validatePartialRow(partialRow, "updated row");
    this.assertUniqueUpdate([rowIndex], values, "update");
    this.applyPartialRow(rowIndex, values);
    this.markIndexesAfterUpdate(values);
    return { affectedRows: 1 };
  }

  updateWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    partialRow: PartialRowForSchema<TSchema>,
  ): MutationResult {
    return this.where(columnName, operator, value).update(partialRow);
  }

  deleteWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): MutationResult {
    return this.where(columnName, operator, value).delete();
  }

  updateMany(
    predicate: ObjectWherePredicate<TSchema>,
    partialRow: PartialRowForSchema<TSchema>,
  ): MutationResult {
    return this.where(predicate).update(partialRow);
  }

  deleteMany(predicate: ObjectWherePredicate<TSchema>): MutationResult {
    return this.where(predicate).delete();
  }

  rebuildIndex<Key extends keyof TSchema>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "rebuildIndex()");
    this.indexManager.rebuild(
      String(columnName),
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
    return this;
  }

  rebuildSortedIndex<Key extends NumericColumnKey<TSchema>>(
    columnName: Key,
  ): this {
    assertColumnExists(this.schema, columnName, "rebuildSortedIndex()");
    this.indexManager.rebuildSorted(
      String(columnName),
      this.currentRowCount,
      (rowIndex, name) =>
        this.getNumericValue(rowIndex, name as NumericColumnKey<TSchema>),
    );
    return this;
  }

  rebuildIndexes(): this {
    this.indexManager.rebuildAll(
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
      (rowIndex, name) =>
        this.getNumericValue(rowIndex, name as NumericColumnKey<TSchema>),
    );
    return this;
  }

  private updateRows(
    rowIndexes: readonly number[],
    partialRow: PartialRowForSchema<TSchema>,
  ): MutationResult {
    const values = this.validatePartialRow(partialRow, "updated row");
    const indexes = this.uniqueRowIndexes(rowIndexes);
    if (indexes.length === 0) {
      return { affectedRows: 0 };
    }

    for (const rowIndex of indexes) {
      this.assertReadableRow(rowIndex);
    }

    this.assertUniqueUpdate(indexes, values, "updateMany");
    for (const rowIndex of indexes) {
      this.applyPartialRow(rowIndex, values);
    }

    this.markIndexesAfterUpdate(values);
    return { affectedRows: indexes.length };
  }

  private deleteRows(rowIndexes: readonly number[]): MutationResult {
    const indexes = this.uniqueRowIndexes(rowIndexes).sort(
      (left, right) => right - left,
    );
    if (indexes.length === 0) {
      return { affectedRows: 0 };
    }

    for (const rowIndex of indexes) {
      this.assertReadableRow(rowIndex);
    }

    this.deleteRowsAt(indexes);

    this.indexManager.markDirty();
    return { affectedRows: indexes.length };
  }

  private deleteRowAt(rowIndex: number): void {
    for (const key of this.schemaKeys()) {
      this.storages[key].deleteAt(rowIndex);
    }

    this.currentRowCount -= 1;
    this.currentCapacity = Math.max(
      1,
      ...this.schemaKeys().map((key) => this.storages[key].capacity),
    );
  }

  private deleteRowsAt(rowIndexesDescending: readonly number[]): void {
    const rowIndexes = [...rowIndexesDescending].sort((left, right) => left - right);
    if (rowIndexes.length === 1) {
      this.deleteRowAt(rowIndexes[0]);
      return;
    }

    for (const key of this.schemaKeys()) {
      const storage = this.storages[key];
      if (this.hasBulkDelete(storage)) {
        storage.deleteMany(rowIndexes);
        continue;
      }

      for (let index = rowIndexes.length - 1; index >= 0; index -= 1) {
        storage.deleteAt(rowIndexes[index]);
      }
    }

    this.currentRowCount -= rowIndexes.length;
    this.currentCapacity = Math.max(
      1,
      ...this.schemaKeys().map((key) => this.storages[key].capacity),
    );
  }

  insertMany(rows: readonly RowForSchema<TSchema>[]): this {
    if (!Array.isArray(rows)) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        "insertMany() expected an array of rows.",
      );
    }

    rows.forEach((row, index) => {
      try {
        this.validateRow(row);
      } catch (error) {
        if (error instanceof ColQLError) {
          throw new ColQLError(
            error.code,
            `Invalid row at index ${index}: ${error.message}`,
            { index, cause: error.details },
          );
        }
        throw error;
      }
    });

    if (rows.length === 0) {
      return this;
    }

    this.assertUniqueInsertMany(rows);
    const firstRowIndex = this.currentRowCount;
    this.ensureCapacity(this.currentRowCount + rows.length);
    for (const row of rows) {
      this.appendRow(row);
    }

    this.indexManager.markSortedDirty();
    for (
      let rowIndex = firstRowIndex;
      rowIndex < this.currentRowCount;
      rowIndex += 1
    ) {
      this.addRowToEqualityIndexes(rowIndex);
      this.addRowToUniqueIndexes(rowIndex);
    }

    return this;
  }

  whereIn<Key extends keyof TSchema>(
    columnName: Key,
    values: readonly ColumnValue<TSchema[Key]>[],
  ): Query<TSchema, RowForSchema<TSchema>> {
    return this.where(columnName, "in", values);
  }

  whereNotIn<Key extends keyof TSchema>(
    columnName: Key,
    values: readonly ColumnValue<TSchema[Key]>[],
  ): Query<TSchema, RowForSchema<TSchema>> {
    return this.where(columnName, "not in", values);
  }

  filter(
    predicate: RowPredicate<TSchema>,
  ): Query<TSchema, RowForSchema<TSchema>> {
    return this.query().filter(predicate);
  }

  where(
    predicate: ObjectWherePredicate<TSchema>,
  ): Query<TSchema, RowForSchema<TSchema>>;
  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, RowForSchema<TSchema>>;
  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnNameOrPredicate: Key | ObjectWherePredicate<TSchema>,
    operator?: TOperator,
    value?: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, RowForSchema<TSchema>> {
    if (arguments.length === 1) {
      return this.query().where(
        columnNameOrPredicate as ObjectWherePredicate<TSchema>,
      );
    }

    return this.query().where(
      columnNameOrPredicate as Key,
      operator as TOperator,
      value as ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    );
  }

  select<const Keys extends readonly (keyof TSchema)[]>(
    columns: Keys,
  ): Query<TSchema, SelectedRow<TSchema, Keys>> {
    return this.query().select(columns);
  }

  limit(n: number): Query<TSchema, RowForSchema<TSchema>> {
    return this.query().limit(n);
  }

  offset(n: number): Query<TSchema, RowForSchema<TSchema>> {
    return this.query().offset(n);
  }

  toArray(): RowForSchema<TSchema>[] {
    return this.query().toArray();
  }

  first(): RowForSchema<TSchema> | undefined {
    return this.query().first();
  }

  count(): number {
    return this.query().count();
  }

  size(): number {
    return this.count();
  }

  isEmpty(): boolean {
    return this.currentRowCount === 0;
  }

  get(rowIndex: number): RowForSchema<TSchema> {
    assertRowIndex(rowIndex, this.currentRowCount);
    return this.materializeRow(rowIndex);
  }

  getSchema(): TSchema {
    return this.schema;
  }

  sum<Key extends NumericColumnKey<TSchema>>(columnName: Key): number {
    return this.query().sum(columnName);
  }

  avg<Key extends NumericColumnKey<TSchema>>(
    columnName: Key,
  ): number | undefined {
    return this.query().avg(columnName);
  }

  min<Key extends NumericColumnKey<TSchema>>(
    columnName: Key,
  ): number | undefined {
    return this.query().min(columnName);
  }

  max<Key extends NumericColumnKey<TSchema>>(
    columnName: Key,
  ): number | undefined {
    return this.query().max(columnName);
  }

  top<Key extends NumericColumnKey<TSchema>>(
    n: number,
    columnName: Key,
  ): RowForSchema<TSchema>[] {
    assertPositiveInteger(n, "top");
    return this.query().top(n, columnName);
  }

  bottom<Key extends NumericColumnKey<TSchema>>(
    n: number,
    columnName: Key,
  ): RowForSchema<TSchema>[] {
    assertPositiveInteger(n, "bottom");
    return this.query().bottom(n, columnName);
  }

  createIndex<Key extends keyof TSchema>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "createIndex()");
    this.indexManager.create(
      String(columnName),
      this.schema[columnName],
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
    return this;
  }

  dropIndex<Key extends keyof TSchema>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "dropIndex()");
    this.indexManager.drop(String(columnName));
    return this;
  }

  hasIndex<Key extends keyof TSchema>(columnName: Key): boolean {
    assertColumnExists(this.schema, columnName, "hasIndex()");
    return this.indexManager.has(String(columnName));
  }

  indexes(): string[] {
    return this.indexManager.list();
  }

  indexStats(): EqualityIndexStats[] {
    return this.indexManager.stats();
  }

  createSortedIndex<Key extends NumericColumnKey<TSchema>>(
    columnName: Key,
  ): this {
    assertColumnExists(this.schema, columnName, "createSortedIndex()");
    this.indexManager.createSorted(
      String(columnName),
      this.schema[columnName],
      this.currentRowCount,
      (rowIndex, name) =>
        this.getNumericValue(rowIndex, name as NumericColumnKey<TSchema>),
    );
    return this;
  }

  dropSortedIndex<Key extends keyof TSchema>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "dropSortedIndex()");
    this.indexManager.dropSorted(String(columnName));
    return this;
  }

  hasSortedIndex<Key extends keyof TSchema>(columnName: Key): boolean {
    assertColumnExists(this.schema, columnName, "hasSortedIndex()");
    return this.indexManager.hasSorted(String(columnName));
  }

  sortedIndexes(): string[] {
    return this.indexManager.listSorted();
  }

  sortedIndexStats(): SortedIndexStats[] {
    return this.indexManager.sortedStats();
  }

  createUniqueIndex<Key extends UniqueColumnKey<TSchema>>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "createUniqueIndex()");
    this.indexManager.createUnique(
      String(columnName),
      this.schema[columnName],
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
    return this;
  }

  dropUniqueIndex<Key extends keyof TSchema>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "dropUniqueIndex()");
    this.indexManager.dropUnique(String(columnName));
    return this;
  }

  hasUniqueIndex<Key extends keyof TSchema>(columnName: Key): boolean {
    assertColumnExists(this.schema, columnName, "hasUniqueIndex()");
    return this.indexManager.hasUnique(String(columnName));
  }

  uniqueIndexes(): string[] {
    return this.indexManager.listUnique();
  }

  uniqueIndexStats(): UniqueIndexStats[] {
    return this.indexManager.uniqueStats(
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
  }

  rebuildUniqueIndex<Key extends UniqueColumnKey<TSchema>>(columnName: Key): this {
    assertColumnExists(this.schema, columnName, "rebuildUniqueIndex()");
    this.indexManager.rebuildUnique(
      String(columnName),
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
    return this;
  }

  rebuildUniqueIndexes(): this {
    this.indexManager.rebuildUniqueIndexes(
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
    return this;
  }

  findBy<Key extends UniqueColumnKey<TSchema>>(
    columnName: Key,
    value: ColumnValue<TSchema[Key]>,
  ): RowForSchema<TSchema> | undefined {
    const rowIndex = this.uniqueRowIndexByValue(columnName, value, "findBy");
    return rowIndex === undefined ? undefined : this.materializeRow(rowIndex);
  }

  updateBy<Key extends UniqueColumnKey<TSchema>>(
    columnName: Key,
    value: ColumnValue<TSchema[Key]>,
    partialRow: PartialRowForSchema<TSchema>,
  ): MutationResult {
    const rowIndex = this.uniqueRowIndexByValue(columnName, value, "updateBy");
    if (rowIndex === undefined) {
      this.validatePartialRow(partialRow, "updated row");
      return { affectedRows: 0 };
    }

    return this.update(rowIndex, partialRow);
  }

  deleteBy<Key extends UniqueColumnKey<TSchema>>(
    columnName: Key,
    value: ColumnValue<TSchema[Key]>,
  ): MutationResult {
    const rowIndex = this.uniqueRowIndexByValue(columnName, value, "deleteBy");
    if (rowIndex === undefined) {
      return { affectedRows: 0 };
    }

    this.delete(rowIndex);
    return { affectedRows: 1 };
  }

  firstWhere(predicate: ObjectWherePredicate<TSchema>): RowForSchema<TSchema> | undefined;
  firstWhere(predicate: RowPredicate<TSchema>): RowForSchema<TSchema> | undefined;
  firstWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): RowForSchema<TSchema> | undefined;
  firstWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnNameOrPredicate: Key | ObjectWherePredicate<TSchema> | RowPredicate<TSchema>,
    operator?: TOperator,
    value?: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): RowForSchema<TSchema> | undefined {
    if (typeof columnNameOrPredicate === "function") {
      return this.filter(columnNameOrPredicate).first();
    }

    if (arguments.length === 1) {
      return this.where(columnNameOrPredicate as ObjectWherePredicate<TSchema>).first();
    }

    return this.where(
      columnNameOrPredicate as Key,
      operator as TOperator,
      value as ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    ).first();
  }

  countWhere(predicate: ObjectWherePredicate<TSchema>): number;
  countWhere(predicate: RowPredicate<TSchema>): number;
  countWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): number;
  countWhere<Key extends keyof TSchema, TOperator extends Operator>(
    columnNameOrPredicate: Key | ObjectWherePredicate<TSchema> | RowPredicate<TSchema>,
    operator?: TOperator,
    value?: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): number {
    if (typeof columnNameOrPredicate === "function") {
      return this.filter(columnNameOrPredicate).count();
    }

    if (arguments.length === 1) {
      return this.where(columnNameOrPredicate as ObjectWherePredicate<TSchema>).count();
    }

    return this.where(
      columnNameOrPredicate as Key,
      operator as TOperator,
      value as ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    ).count();
  }

  exists(predicate: ObjectWherePredicate<TSchema>): boolean;
  exists(predicate: RowPredicate<TSchema>): boolean;
  exists<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): boolean;
  exists<Key extends keyof TSchema, TOperator extends Operator>(
    columnNameOrPredicate: Key | ObjectWherePredicate<TSchema> | RowPredicate<TSchema>,
    operator?: TOperator,
    value?: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): boolean {
    if (typeof columnNameOrPredicate === "function") {
      return this.filter(columnNameOrPredicate).limit(1).count() > 0;
    }

    if (arguments.length === 1) {
      return this.where(columnNameOrPredicate as ObjectWherePredicate<TSchema>).limit(1).count() > 0;
    }

    return this.where(
      columnNameOrPredicate as Key,
      operator as TOperator,
      value as ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    ).limit(1).count() > 0;
  }

  forEach(callback: (row: RowForSchema<TSchema>, index: number) => void): void {
    this.query().forEach(callback);
  }

  stream(): Iterable<RowForSchema<TSchema>> {
    return this.query();
  }

  [Symbol.iterator](): Iterator<RowForSchema<TSchema>> {
    return this.query()[Symbol.iterator]();
  }

  serialize(): ArrayBuffer {
    const columns = this.schemaKeys().map((key) =>
      this.createSerializedColumnMeta(String(key)),
    );
    const headerColumns = this.withPayloadOffsets(columns);
    const header = this.encodeHeader({
      version: SERIALIZATION_VERSION,
      rowCount: this.currentRowCount,
      capacity: this.currentCapacity,
      columns: headerColumns,
    });
    const totalBytes = this.totalSerializedBytes(
      header.byteLength,
      headerColumns,
    );
    const output = new ArrayBuffer(totalBytes);
    const bytes = new Uint8Array(output);
    bytes.set(new TextEncoder().encode(SERIALIZATION_MAGIC), 0);
    new DataView(output).setUint32(MAGIC_BYTES, header.byteLength, true);
    bytes.set(header, SERIALIZATION_PREFIX_BYTES);

    for (const meta of headerColumns) {
      bytes.set(this.getSerializedColumnBytes(meta.name), meta.byteOffset);
    }

    return output;
  }

  getValue<Key extends keyof TSchema>(
    rowIndex: number,
    columnName: Key,
  ): ColumnValue<TSchema[Key]> {
    this.assertReadableRow(rowIndex);
    return this.storages[columnName].get(rowIndex);
  }

  getComparableValue(
    rowIndex: number,
    columnName: keyof TSchema,
  ): number | boolean {
    this.assertReadableRow(rowIndex);
    const storage = this.storages[columnName];
    if (storage instanceof DictionaryColumnStorage) {
      return storage.getCode(rowIndex);
    }

    return storage.get(rowIndex) as number | boolean;
  }

  getNumericValue<Key extends NumericColumnKey<TSchema>>(
    rowIndex: number,
    columnName: Key,
  ): number {
    if (this.schema[columnName].kind !== "numeric") {
      throw new ColQLError(
        "COLQL_INVALID_COLUMN_TYPE",
        `Column "${String(columnName)}" must be numeric for this operation.`,
      );
    }

    return this.getValue(rowIndex, columnName) as number;
  }

  materializeRow<Keys extends readonly (keyof TSchema)[] | undefined>(
    rowIndex: number,
    selectedColumns?: Keys,
  ): Keys extends readonly (keyof TSchema)[]
    ? SelectedRow<TSchema, Keys>
    : RowForSchema<TSchema> {
    this.assertReadableRow(rowIndex);
    this.materializedRows += 1;

    const columns = selectedColumns ?? this.schemaKeys();
    const row: Partial<RowForSchema<TSchema>> = {};
    for (const key of columns) {
      row[key] = this.getValue(rowIndex, key);
    }

    return row as Keys extends readonly (keyof TSchema)[]
      ? SelectedRow<TSchema, Keys>
      : RowForSchema<TSchema>;
  }

  createFilter<Key extends keyof TSchema>(
    filter: Filter<TSchema, Key>,
  ): InternalFilter {
    assertColumnExists(this.schema, filter.columnName, "where()");
    assertOperator(filter.operator);

    const definition = this.schema[filter.columnName];
    assertOperatorAllowed(
      String(filter.columnName),
      definition,
      filter.operator,
    );
    const isMultiValue =
      filter.operator === "in" || filter.operator === "not in";

    if (isMultiValue && !Array.isArray(filter.value)) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        `Operator "${filter.operator}" expects a non-empty array value.`,
      );
    }

    if (
      isMultiValue &&
      Array.isArray(filter.value) &&
      filter.value.length === 0
    ) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        `Operator "${filter.operator}" expects a non-empty array value.`,
      );
    }

    if (!isMultiValue && Array.isArray(filter.value)) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        `Operator "${filter.operator}" expects a single value.`,
      );
    }

    const values = isMultiValue
      ? (filter.value as readonly unknown[])
      : [filter.value];
    for (const value of values) {
      validateColumnValue(String(filter.columnName), definition, value);
    }

    if (definition.kind === "dictionary") {
      const storage = this.storages[filter.columnName];
      if (!(storage instanceof DictionaryColumnStorage)) {
        throw new ColQLError(
          "COLQL_INVALID_COLUMN_TYPE",
          `Column "${String(filter.columnName)}" is not backed by dictionary storage.`,
        );
      }

      const encode = (value: ColumnValue<TSchema[Key]>): number =>
        storage.encode(value as Extract<ColumnValue<TSchema[Key]>, string>);

      return {
        columnName: String(filter.columnName),
        operator: filter.operator,
        value: isMultiValue
          ? (filter.value as readonly ColumnValue<TSchema[Key]>[]).map(encode)
          : encode(filter.value as ColumnValue<TSchema[Key]>),
      };
    }

    this.validateFilterValue(
      definition.kind,
      filter.value,
      isMultiValue,
      String(filter.columnName),
    );
    return {
      columnName: String(filter.columnName),
      operator: filter.operator,
      value: filter.value as number | boolean | readonly (number | boolean)[],
    };
  }

  matchesFilter(rowIndex: number, filter: InternalFilter): boolean {
    const key = filter.columnName as keyof TSchema;
    const storage = this.storages[key];
    const left = storage instanceof DictionaryColumnStorage
      ? storage.getCode(rowIndex)
      : (storage.get(rowIndex) as number | boolean);
    const { operator, value } = filter;

    if (operator === "in" || operator === "not in") {
      const values = value as readonly (number | boolean)[];
      const found = values.includes(left);
      return operator === "in" ? found : !found;
    }

    const right = value as number | boolean;
    switch (operator) {
      case "=":
        return left === right;
      case "!=":
        return left !== right;
      case ">":
        return left > right;
      case ">=":
        return left >= right;
      case "<":
        return left < right;
      case "<=":
        return left <= right;
    }
  }

  query(): Query<TSchema, RowForSchema<TSchema>> {
    return new Query(this);
  }

  getIndexedCandidatePlan(
    filters: readonly IndexFilter[],
  ): IndexCandidatePlan | undefined {
    return this.indexManager.bestCandidate(
      filters,
      this.currentRowCount,
      (rowIndex, name) =>
        this.getNumericValue(rowIndex, name as NumericColumnKey<TSchema>),
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
  }

  getIndexDebugPlan(filters: readonly IndexFilter[]): IndexDebugPlan {
    return this.indexManager.debugPlan(
      filters,
      this.currentRowCount,
      (rowIndex, name) =>
        this.getNumericValue(rowIndex, name as NumericColumnKey<TSchema>),
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
  }

  static deserialize(input: ArrayBuffer | Uint8Array): Table<Schema> {
    if (!(input instanceof ArrayBuffer) && !(input instanceof Uint8Array)) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        "Invalid serialized ColQL data: expected ArrayBuffer or Uint8Array input.",
      );
    }
    const source = input instanceof Uint8Array ? input : new Uint8Array(input);
    const buffer = source.buffer.slice(
      source.byteOffset,
      source.byteOffset + source.byteLength,
    );
    const bytes = new Uint8Array(buffer);

    if (bytes.byteLength < SERIALIZATION_PREFIX_BYTES) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        "Invalid serialized ColQL data: input is too small.",
      );
    }

    const magic = new TextDecoder().decode(bytes.subarray(0, MAGIC_BYTES));
    if (magic !== SERIALIZATION_MAGIC) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        "Invalid serialized ColQL data: magic header mismatch.",
      );
    }

    const headerLength = new DataView(buffer).getUint32(MAGIC_BYTES, true);
    const headerStart = SERIALIZATION_PREFIX_BYTES;
    const headerEnd = headerStart + headerLength;
    if (headerEnd > bytes.byteLength) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        "Invalid serialized ColQL data: header length exceeds input size.",
      );
    }

    const meta = Table.parseSerializedMeta(
      new TextDecoder().decode(bytes.subarray(headerStart, headerEnd)),
    );
    if (meta.version !== SERIALIZATION_VERSION) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        `Unsupported ColQL serialization version "${meta.version}".`,
      );
    }

    const schemaEntries: [string, ColumnDefinition][] = [];
    const storageEntries: [string, ColumnStorage<unknown>][] = [];

    for (const columnMeta of meta.columns) {
      if (columnMeta.byteOffset + columnMeta.byteLength > bytes.byteLength) {
        throw new ColQLError(
          "COLQL_INVALID_SERIALIZED_DATA",
          `Invalid serialized ColQL data: column "${columnMeta.name}" exceeds input size.`,
        );
      }

      const view = bytes.subarray(
        columnMeta.byteOffset,
        columnMeta.byteOffset + columnMeta.byteLength,
      );
      const { definition, storage } = Table.restoreColumn(
        columnMeta,
        meta.capacity,
        meta.rowCount,
        view,
      );
      schemaEntries.push([columnMeta.name, definition]);
      storageEntries.push([columnMeta.name, storage]);
    }

    const restoredSchema = Object.fromEntries(schemaEntries) as Schema;
    const storages = Object.fromEntries(storageEntries) as StorageMap<Schema>;
    return new Table(restoredSchema, meta.capacity, {
      storages,
      rowCount: meta.rowCount,
    });
  }

  private createStorages(capacity: number): StorageMap<TSchema> {
    const entries = this.schemaKeys().map(
      (key) => [key, this.schema[key].createStorage(capacity)] as const,
    );
    return Object.fromEntries(entries) as StorageMap<TSchema>;
  }

  private validateRow(row: unknown): asserts row is RowForSchema<TSchema> {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        "Invalid row: expected a non-null object.",
      );
    }

    const record = row as Record<string, unknown>;
    const keys = this.schemaKeys().map(String);
    for (const key of keys) {
      if (!(key in record)) {
        throw new ColQLError(
          "COLQL_MISSING_VALUE",
          `Missing value for column "${key}".`,
          { columnName: key },
        );
      }
    }

    for (const key of Object.keys(record)) {
      if (!(key in this.schema)) {
        throw new ColQLError(
          "COLQL_INVALID_COLUMN",
          `Unknown column "${key}" in inserted row.`,
          { columnName: key },
        );
      }
    }

    for (const key of keys) {
      validateColumnValue(key, this.schema[key], record[key]);
    }
  }

  private validatePartialRow(
    row: unknown,
    context: "updated row",
  ): [keyof TSchema, ColumnValue<TSchema[keyof TSchema]>][] {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new ColQLError(
        "COLQL_TYPE_MISMATCH",
        `Invalid ${context}: expected a non-null object.`,
      );
    }

    const record = row as Record<string, unknown>;
    const keys = Object.keys(record);
    if (keys.length === 0) {
      throw new ColQLError(
        "COLQL_MISSING_VALUE",
        "Invalid updated row: expected at least one column value.",
      );
    }

    for (const key of keys) {
      if (!(key in this.schema)) {
        throw new ColQLError(
          "COLQL_INVALID_COLUMN",
          `Unknown column "${key}" in updated row.`,
          { columnName: key },
        );
      }
    }

    const values: [keyof TSchema, ColumnValue<TSchema[keyof TSchema]>][] = [];
    for (const key of keys) {
      const schemaKey = key as keyof TSchema;
      validateColumnValue(key, this.schema[schemaKey], record[key]);
      values.push([
        schemaKey,
        record[key] as ColumnValue<TSchema[keyof TSchema]>,
      ]);
    }

    return values;
  }

  private applyPartialRow(
    rowIndex: number,
    values: readonly [keyof TSchema, ColumnValue<TSchema[keyof TSchema]>][],
  ): void {
    for (const [key, value] of values) {
      this.storages[key].set(rowIndex, value);
    }
  }

  private appendRow(row: RowForSchema<TSchema>): void {
    for (const key of this.schemaKeys()) {
      this.storages[key].append(row[key]);
    }

    this.currentRowCount += 1;
  }

  private uniqueRowIndexes(rowIndexes: readonly number[]): number[] {
    return [...new Set(rowIndexes)];
  }

  private addRowToIndexes(rowIndex: number): void {
    this.indexManager.markSortedDirty();
    this.addRowToEqualityIndexes(rowIndex);
    this.addRowToUniqueIndexes(rowIndex);
  }

  private addRowToEqualityIndexes(rowIndex: number): void {
    for (const key of this.schemaKeys()) {
      const columnName = String(key);
      if (!this.indexManager.has(columnName)) {
        continue;
      }

      this.indexManager.addRow(
        columnName,
        this.getComparableValue(rowIndex, key),
        rowIndex,
      );
    }
  }

  private addRowToUniqueIndexes(rowIndex: number): void {
    for (const columnName of this.indexManager.listUnique()) {
      this.indexManager.addUniqueRow(
        columnName,
        this.getComparableValue(rowIndex, columnName as keyof TSchema),
        rowIndex,
      );
    }
  }

  private assertUniqueInsert(row: RowForSchema<TSchema>, operation: string): void {
    for (const columnName of this.indexManager.listUnique()) {
      const encodedValue = this.comparableValueFromRow(row, columnName as keyof TSchema);
      const existingRowIndex = this.indexManager.uniqueLookup(
        columnName,
        encodedValue,
        this.currentRowCount,
        (rowIndex, name) =>
          this.getComparableValue(rowIndex, name as keyof TSchema),
      );

      if (existingRowIndex !== undefined) {
        throw this.duplicateKeyError(columnName, encodedValue, {
          operation,
          existingRowIndex,
          rowIndex: this.currentRowCount,
        });
      }
    }
  }

  private assertUniqueInsertMany(rows: readonly RowForSchema<TSchema>[]): void {
    for (const columnName of this.indexManager.listUnique()) {
      const seen = new Map<number, number>();
      for (let index = 0; index < rows.length; index += 1) {
        const encodedValue = this.comparableValueFromRow(rows[index], columnName as keyof TSchema);
        const existingRowIndex = this.indexManager.uniqueLookup(
          columnName,
          encodedValue,
          this.currentRowCount,
          (rowIndex, name) =>
            this.getComparableValue(rowIndex, name as keyof TSchema),
        );

        if (existingRowIndex !== undefined) {
          throw this.duplicateKeyError(columnName, encodedValue, {
            operation: "insertMany",
            existingRowIndex,
            inputIndex: index,
          });
        }

        const conflictingInputIndex = seen.get(encodedValue);
        if (conflictingInputIndex !== undefined) {
          throw this.duplicateKeyError(columnName, encodedValue, {
            operation: "insertMany",
            inputIndex: index,
            conflictingInputIndex,
          });
        }

        seen.set(encodedValue, index);
      }
    }
  }

  private assertUniqueUpdate(
    rowIndexes: readonly number[],
    values: readonly [keyof TSchema, ColumnValue<TSchema[keyof TSchema]>][],
    operation: string,
  ): void {
    const updatesByColumn = new Map<keyof TSchema, ColumnValue<TSchema[keyof TSchema]>>();
    for (const [key, value] of values) {
      updatesByColumn.set(key, value);
    }

    const targetRows = new Set(rowIndexes);
    for (const columnName of this.indexManager.listUnique()) {
      const key = columnName as keyof TSchema;
      if (!updatesByColumn.has(key)) {
        continue;
      }

      const rowsByValue = new Map<number, number>();
      for (let rowIndex = 0; rowIndex < this.currentRowCount; rowIndex += 1) {
        const encodedValue = targetRows.has(rowIndex)
          ? this.comparableValueFromValue(key, updatesByColumn.get(key))
          : (this.getComparableValue(rowIndex, key) as number);
        const existingRowIndex = rowsByValue.get(encodedValue);
        if (existingRowIndex !== undefined) {
          throw this.duplicateKeyError(columnName, encodedValue, {
            operation,
            existingRowIndex,
            rowIndex,
          });
        }

        rowsByValue.set(encodedValue, rowIndex);
      }
    }
  }

  private markIndexesAfterUpdate(
    values: readonly [keyof TSchema, ColumnValue<TSchema[keyof TSchema]>][],
  ): void {
    this.indexManager.markPerformanceDirty();
    const uniqueColumns = values
      .map(([key]) => String(key))
      .filter((columnName) => this.indexManager.hasUnique(columnName));
    if (uniqueColumns.length > 0) {
      this.indexManager.markUniqueDirty(uniqueColumns);
    }
  }

  private uniqueRowIndexByValue<Key extends UniqueColumnKey<TSchema>>(
    columnName: Key,
    value: ColumnValue<TSchema[Key]>,
    context: string,
  ): number | undefined {
    assertColumnExists(this.schema, columnName, `${context}()`);
    if (!this.indexManager.hasUnique(String(columnName))) {
      throw new ColQLError(
        "COLQL_UNIQUE_INDEX_NOT_FOUND",
        `Unique index not found for column "${String(columnName)}".`,
        { columnName: String(columnName) },
      );
    }

    validateColumnValue(String(columnName), this.schema[columnName], value);
    return this.indexManager.uniqueLookup(
      String(columnName),
      this.comparableValueFromValue(columnName, value),
      this.currentRowCount,
      (rowIndex, name) =>
        this.getComparableValue(rowIndex, name as keyof TSchema),
    );
  }

  private comparableValueFromRow(
    row: RowForSchema<TSchema>,
    columnName: keyof TSchema,
  ): number {
    return this.comparableValueFromValue(columnName, row[columnName]);
  }

  private comparableValueFromValue(
    columnName: keyof TSchema,
    value: unknown,
  ): number {
    const storage = this.storages[columnName];
    if (storage instanceof DictionaryColumnStorage) {
      return storage.encode(value as string);
    }

    if (this.schema[columnName].kind === "boolean") {
      throw new ColQLError(
        "COLQL_UNIQUE_INDEX_UNSUPPORTED",
        `Unique indexing is not supported for boolean column "${String(columnName)}".`,
        { columnName: String(columnName), kind: "boolean" },
      );
    }

    return value as number;
  }

  private duplicateKeyError(
    columnName: string,
    encodedValue: number,
    details: Record<string, unknown>,
  ): ColQLError {
    return new ColQLError(
      "COLQL_DUPLICATE_KEY",
      `Duplicate key for unique index "${columnName}".`,
      {
        columnName,
        encodedValue,
        ...details,
      },
    );
  }

  private hasBulkDelete(storage: ColumnStorage<unknown>): storage is ColumnStorage<unknown> & BulkDeleteStorage {
    return typeof (storage as Partial<BulkDeleteStorage>).deleteMany === "function";
  }

  private createSerializedColumnMeta(
    name: string,
  ): Omit<SerializedColumnMeta, "byteOffset"> {
    const definition = this.schema[name as keyof TSchema];
    const bytes = this.getSerializedColumnBytes(name);

    if (definition.kind === "numeric") {
      return {
        name,
        kind: "numeric",
        type: definition.type,
        byteLength: bytes.byteLength,
        alignment: this.alignmentForNumericType(definition.type),
      };
    }

    if (definition.kind === "dictionary") {
      return {
        name,
        kind: "dictionary",
        values: definition.values,
        byteLength: bytes.byteLength,
        alignment: this.alignmentForDictionarySize(definition.values.length),
      };
    }

    return {
      name,
      kind: "boolean",
      byteLength: bytes.byteLength,
      alignment: 1,
    };
  }

  private getSerializedColumnBytes(name: string): Uint8Array {
    const storage = this.storages[name as keyof TSchema];
    if (
      storage instanceof NumericColumnStorage ||
      storage instanceof DictionaryColumnStorage ||
      storage instanceof BooleanColumnStorage
    ) {
      return storage.toBytes();
    }

    throw new ColQLError(
      "COLQL_UNSUPPORTED_OPERATION",
      `Column "${name}" cannot be serialized.`,
    );
  }

  private withPayloadOffsets(
    columns: readonly Omit<SerializedColumnMeta, "byteOffset">[],
  ): SerializedColumnMeta[] {
    let header = this.encodeHeader({
      version: SERIALIZATION_VERSION,
      rowCount: this.currentRowCount,
      capacity: this.currentCapacity,
      columns: columns.map((columnMeta) => ({ ...columnMeta, byteOffset: 0 })),
    });

    while (true) {
      let offset = SERIALIZATION_PREFIX_BYTES + header.byteLength;
      const columnsWithOffsets = columns.map((columnMeta) => {
        offset = this.alignOffset(offset, columnMeta.alignment);
        const next = { ...columnMeta, byteOffset: offset };
        offset += columnMeta.byteLength;
        return next;
      });

      const nextHeader = this.encodeHeader({
        version: SERIALIZATION_VERSION,
        rowCount: this.currentRowCount,
        capacity: this.currentCapacity,
        columns: columnsWithOffsets,
      });

      if (nextHeader.byteLength === header.byteLength) {
        return columnsWithOffsets;
      }

      header = nextHeader;
    }
  }

  private encodeHeader(meta: SerializedTableMeta): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(meta));
  }

  private totalSerializedBytes(
    headerLength: number,
    columns: readonly SerializedColumnMeta[],
  ): number {
    let total = SERIALIZATION_PREFIX_BYTES + headerLength;
    for (const columnMeta of columns) {
      total = Math.max(total, columnMeta.byteOffset + columnMeta.byteLength);
    }

    return total;
  }

  private alignOffset(offset: number, alignment: number): number {
    const remainder = offset % alignment;
    return remainder === 0 ? offset : offset + alignment - remainder;
  }

  private alignmentForNumericType(type: string): number {
    switch (type) {
      case "int16":
      case "uint16":
        return 2;
      case "int32":
      case "uint32":
      case "float32":
        return 4;
      case "float64":
        return 8;
      default:
        return 1;
    }
  }

  private alignmentForDictionarySize(size: number): number {
    if (size <= 255) {
      return 1;
    }

    if (size <= 65_535) {
      return 2;
    }

    return 4;
  }

  private ensureCapacity(requiredCapacity: number): void {
    if (requiredCapacity <= this.currentCapacity) {
      return;
    }

    let nextCapacity = this.currentCapacity;
    while (nextCapacity < requiredCapacity) {
      nextCapacity *= 2;
    }

    for (const storage of Object.values(
      this.storages,
    ) as ColumnStorage<unknown>[]) {
      storage.resize(nextCapacity);
    }

    this.currentCapacity = nextCapacity;
  }

  private schemaKeys(): (keyof TSchema)[] {
    return Object.keys(this.schema) as (keyof TSchema)[];
  }

  private assertReadableRow(rowIndex: number): void {
    assertRowIndex(rowIndex, this.currentRowCount);
  }

  private validateFilterValue(
    kind: "numeric" | "boolean",
    value: unknown,
    isMultiValue: boolean,
    columnName: string,
  ): void {
    const values = isMultiValue ? (value as readonly unknown[]) : [value];
    for (const item of values) {
      if (
        kind === "numeric" &&
        (typeof item !== "number" || Number.isNaN(item))
      ) {
        throw new ColQLError(
          "COLQL_TYPE_MISMATCH",
          `Column "${columnName}" expects numeric filter values.`,
        );
      }

      if (kind === "boolean" && typeof item !== "boolean") {
        throw new ColQLError(
          "COLQL_TYPE_MISMATCH",
          `Column "${columnName}" expects boolean filter values.`,
        );
      }
    }
  }

  private static parseSerializedMeta(json: string): SerializedTableMeta {
    try {
      const parsed = JSON.parse(json) as SerializedTableMeta;
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        !Number.isInteger(parsed.rowCount) ||
        !Number.isInteger(parsed.capacity) ||
        !Array.isArray(parsed.columns)
      ) {
        throw new ColQLError(
          "COLQL_INVALID_SERIALIZED_DATA",
          "Invalid serialized ColQL data: missing schema metadata.",
        );
      }

      return parsed;
    } catch (error) {
      if (error instanceof ColQLError) {
        throw error;
      }
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        `Invalid serialized ColQL data: ${error instanceof Error ? error.message : "bad metadata"}.`,
      );
    }
  }

  private static restoreColumn(
    meta: SerializedColumnMeta,
    capacity: number,
    rowCount: number,
    bytes: Uint8Array,
  ): { definition: ColumnDefinition; storage: ColumnStorage<unknown> } {
    if (meta.kind === "numeric") {
      return Table.restoreNumericColumn(meta, capacity, rowCount, bytes);
    }

    if (meta.kind === "dictionary") {
      return Table.restoreDictionaryColumn(meta, capacity, rowCount, bytes);
    }

    if (meta.kind === "boolean") {
      if (bytes.byteLength !== Math.ceil(capacity / 8)) {
        throw new ColQLError(
          "COLQL_INVALID_SERIALIZED_DATA",
          `Invalid serialized ColQL data: boolean column "${meta.name}" byte length is invalid.`,
        );
      }

      return {
        definition: column.boolean(),
        storage: new BooleanColumnStorage(capacity, bytes, rowCount),
      };
    }

    throw new ColQLError(
      "COLQL_INVALID_SERIALIZED_DATA",
      `Invalid serialized ColQL data: unknown column kind "${String(meta.kind)}".`,
    );
  }

  private static restoreNumericColumn(
    meta: SerializedColumnMeta,
    capacity: number,
    rowCount: number,
    bytes: Uint8Array,
  ): { definition: ColumnDefinition; storage: ColumnStorage<unknown> } {
    switch (meta.type) {
      case "int16":
        return {
          definition: column.int16(),
          storage: new NumericColumnStorage(
            "int16",
            capacity,
            new Int16Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "int32":
        return {
          definition: column.int32(),
          storage: new NumericColumnStorage(
            "int32",
            capacity,
            new Int32Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "uint8":
        return {
          definition: column.uint8(),
          storage: new NumericColumnStorage(
            "uint8",
            capacity,
            new Uint8Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "uint16":
        return {
          definition: column.uint16(),
          storage: new NumericColumnStorage(
            "uint16",
            capacity,
            new Uint16Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "uint32":
        return {
          definition: column.uint32(),
          storage: new NumericColumnStorage(
            "uint32",
            capacity,
            new Uint32Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "float32":
        return {
          definition: column.float32(),
          storage: new NumericColumnStorage(
            "float32",
            capacity,
            new Float32Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      case "float64":
        return {
          definition: column.float64(),
          storage: new NumericColumnStorage(
            "float64",
            capacity,
            new Float64Array(bytes.buffer, bytes.byteOffset, capacity),
            rowCount,
          ),
        };
      default:
        throw new ColQLError(
          "COLQL_INVALID_SERIALIZED_DATA",
          `Invalid serialized ColQL data: invalid numeric column "${meta.name}" type "${String(meta.type)}".`,
        );
    }
  }

  private static restoreDictionaryColumn(
    meta: SerializedColumnMeta,
    capacity: number,
    rowCount: number,
    bytes: Uint8Array,
  ): { definition: ColumnDefinition; storage: ColumnStorage<unknown> } {
    if (!Array.isArray(meta.values) || meta.values.length === 0) {
      throw new ColQLError(
        "COLQL_INVALID_SERIALIZED_DATA",
        `Invalid serialized ColQL data: invalid dictionary column "${meta.name}" values.`,
      );
    }

    const values = meta.values as unknown as readonly [string, ...string[]];
    const definition = column.dictionary(values);
    if (values.length <= 255) {
      return {
        definition,
        storage: new DictionaryColumnStorage(
          values,
          capacity,
          new Uint8Array(bytes.buffer, bytes.byteOffset, capacity),
          rowCount,
        ),
      };
    }

    if (values.length <= 65_535) {
      return {
        definition,
        storage: new DictionaryColumnStorage(
          values,
          capacity,
          new Uint16Array(bytes.buffer, bytes.byteOffset, capacity),
          rowCount,
        ),
      };
    }

    return {
      definition,
      storage: new DictionaryColumnStorage(
        values,
        capacity,
        new Uint32Array(bytes.buffer, bytes.byteOffset, capacity),
        rowCount,
      ),
    };
  }
}

export function table<const TSchema extends Schema>(
  schema: TSchema,
  options?: TableOptions,
): Table<TSchema> {
  return new Table(schema, options);
}

export function fromRows<const TSchema extends Schema>(
  schema: TSchema,
  rows: readonly RowForSchema<TSchema>[],
  options?: TableOptions,
): Table<TSchema> {
  return table(schema, options).insertMany(rows);
}

export namespace table {
  export function deserialize(input: ArrayBuffer | Uint8Array): Table<Schema> {
    return Table.deserialize(input);
  }
}
