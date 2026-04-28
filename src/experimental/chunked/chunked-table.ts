import { ColQLError } from "../../errors";
import { assertColumnExists, assertOperator, assertOperatorAllowed, validateColumnValue } from "../../validation";
import type { Operator } from "../../types";
import { ExperimentalChunkedBooleanColumn } from "./chunked-boolean-column";
import { ExperimentalChunkedDictionaryColumn } from "./chunked-dictionary-column";
import { ExperimentalChunkedNumericColumn } from "./chunked-numeric-column";
import type { ChunkedColumnDefinition, ChunkedColumnValue, ChunkedRow, ChunkedSchema, ExperimentalChunkedColumn } from "./chunked-types";
import { assertChunkSize, assertRowIndex } from "./chunked-column";

type StorageMap<TSchema extends ChunkedSchema> = {
  [Key in keyof TSchema]: ExperimentalChunkedColumn<ChunkedColumnValue<TSchema[Key]>>;
};

type Filter = {
  readonly columnName: string;
  readonly operator: Operator;
  readonly value: number | string | boolean | readonly (number | string | boolean)[];
};

export class ExperimentalChunkedTable<TSchema extends ChunkedSchema> {
  private readonly storages: StorageMap<TSchema>;
  private currentRowCount = 0;
  private indexesDirty = false;

  constructor(readonly schema: TSchema, readonly chunkSize = 65_536) {
    assertChunkSize(chunkSize);
    this.storages = this.createStorages();
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  get dirtyIndexes(): boolean {
    return this.indexesDirty;
  }

  insert(row: ChunkedRow<TSchema>): this {
    this.validateRow(row);
    for (const key of this.schemaKeys()) {
      this.storages[key].append(row[key]);
    }
    this.currentRowCount += 1;
    this.indexesDirty = true;
    return this;
  }

  insertMany(rows: readonly ChunkedRow<TSchema>[]): this {
    if (!Array.isArray(rows)) {
      throw new ColQLError("COLQL_TYPE_MISMATCH", "insertMany() expected an array of rows.");
    }
    rows.forEach((row) => this.validateRow(row));
    rows.forEach((row) => this.insert(row));
    return this;
  }

  get(rowIndex: number): ChunkedRow<TSchema> {
    assertRowIndex(rowIndex, this.currentRowCount);
    return this.materializeRow(rowIndex);
  }

  delete(rowIndex: number): this {
    assertRowIndex(rowIndex, this.currentRowCount);
    for (const key of this.schemaKeys()) {
      this.storages[key].deleteAt(rowIndex);
    }
    this.currentRowCount -= 1;
    this.indexesDirty = true;
    return this;
  }

  count(): number {
    return this.currentRowCount;
  }

  toArray(): ChunkedRow<TSchema>[] {
    const rows: ChunkedRow<TSchema>[] = [];
    for (let rowIndex = 0; rowIndex < this.currentRowCount; rowIndex += 1) {
      rows.push(this.materializeRow(rowIndex));
    }
    return rows;
  }

  where<Key extends keyof TSchema>(columnName: Key, operator: Operator, value: ChunkedColumnValue<TSchema[Key]>): ExperimentalChunkedQuery<TSchema> {
    return new ExperimentalChunkedQuery(this).where(columnName, operator, value);
  }

  memoryBytesApprox(): number {
    return Object.values(this.storages).reduce((total, storage) => total + (storage as ExperimentalChunkedColumn<unknown>).memoryBytesApprox(), 0);
  }

  getValue<Key extends keyof TSchema>(rowIndex: number, columnName: Key): ChunkedColumnValue<TSchema[Key]> {
    assertRowIndex(rowIndex, this.currentRowCount);
    return this.storages[columnName].get(rowIndex);
  }

  matchesFilter(rowIndex: number, filter: Filter): boolean {
    const left = this.getValue(rowIndex, filter.columnName as keyof TSchema) as number | string | boolean;
    if (filter.operator === "in" || filter.operator === "not in") {
      const found = (filter.value as readonly (number | string | boolean)[]).includes(left);
      return filter.operator === "in" ? found : !found;
    }

    const right = filter.value as number | string | boolean;
    switch (filter.operator) {
      case "=": return left === right;
      case "!=": return left !== right;
      case ">": return left > right;
      case ">=": return left >= right;
      case "<": return left < right;
      case "<=": return left <= right;
    }
  }

  createFilter<Key extends keyof TSchema>(columnName: Key, operator: Operator, value: unknown): Filter {
    assertColumnExists(this.schema, columnName, "experimental chunked where()");
    assertOperator(operator);
    const definition = this.schema[columnName];
    assertOperatorAllowed(String(columnName), this.toColumnDefinition(definition), operator);
    validateColumnValue(String(columnName), this.toColumnDefinition(definition), value);
    return { columnName: String(columnName), operator, value: value as number | string | boolean };
  }

  private createStorages(): StorageMap<TSchema> {
    const entries = this.schemaKeys().map((key) => [key, this.createStorage(this.schema[key])] as const);
    return Object.fromEntries(entries) as StorageMap<TSchema>;
  }

  private createStorage(definition: ChunkedColumnDefinition): ExperimentalChunkedColumn<unknown> {
    if (definition.kind === "numeric") {
      return new ExperimentalChunkedNumericColumn(definition.type, this.chunkSize);
    }
    if (definition.kind === "dictionary") {
      return new ExperimentalChunkedDictionaryColumn(definition.values, this.chunkSize);
    }
    return new ExperimentalChunkedBooleanColumn(this.chunkSize);
  }

  private materializeRow(rowIndex: number): ChunkedRow<TSchema> {
    const row: Partial<ChunkedRow<TSchema>> = {};
    for (const key of this.schemaKeys()) {
      row[key] = this.getValue(rowIndex, key);
    }
    return row as ChunkedRow<TSchema>;
  }

  private validateRow(row: unknown): asserts row is ChunkedRow<TSchema> {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new ColQLError("COLQL_TYPE_MISMATCH", "Invalid row: expected a non-null object.");
    }
    const record = row as Record<string, unknown>;
    for (const key of this.schemaKeys()) {
      if (!(String(key) in record)) {
        throw new ColQLError("COLQL_MISSING_VALUE", `Missing value for column "${String(key)}".`);
      }
      validateColumnValue(String(key), this.toColumnDefinition(this.schema[key]), record[String(key)]);
    }
    for (const key of Object.keys(record)) {
      if (!(key in this.schema)) {
        throw new ColQLError("COLQL_INVALID_COLUMN", `Unknown column "${key}" in inserted row.`);
      }
    }
  }

  private toColumnDefinition(definition: ChunkedColumnDefinition): import("../../types").ColumnDefinition {
    if (definition.kind === "numeric") {
      return { kind: "numeric", type: definition.type, createStorage: () => { throw new Error("experimental"); } };
    }
    if (definition.kind === "dictionary") {
      return { kind: "dictionary", type: "dictionary", values: definition.values, createStorage: () => { throw new Error("experimental"); } };
    }
    return { kind: "boolean", type: "boolean", createStorage: () => { throw new Error("experimental"); } };
  }

  private schemaKeys(): (keyof TSchema)[] {
    return Object.keys(this.schema) as (keyof TSchema)[];
  }
}

export class ExperimentalChunkedQuery<TSchema extends ChunkedSchema> {
  private readonly filters: Filter[] = [];

  constructor(private readonly source: ExperimentalChunkedTable<TSchema>) {}

  where<Key extends keyof TSchema>(columnName: Key, operator: Operator, value: ChunkedColumnValue<TSchema[Key]>): this {
    this.filters.push(this.source.createFilter(columnName, operator, value));
    return this;
  }

  toArray(): ChunkedRow<TSchema>[] {
    const rows: ChunkedRow<TSchema>[] = [];
    for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
      if (this.filters.every((filter) => this.source.matchesFilter(rowIndex, filter))) {
        rows.push(this.source.get(rowIndex));
      }
    }
    return rows;
  }

  count(): number {
    let count = 0;
    for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
      if (this.filters.every((filter) => this.source.matchesFilter(rowIndex, filter))) {
        count += 1;
      }
    }
    return count;
  }
}

export function experimentalChunkedTable<const TSchema extends ChunkedSchema>(schema: TSchema, chunkSize?: number): ExperimentalChunkedTable<TSchema> {
  return new ExperimentalChunkedTable(schema, chunkSize);
}
