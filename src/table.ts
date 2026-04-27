import { Query } from "./query";
import { DictionaryColumnStorage } from "./storage/dictionary-column";
import type {
  ColumnStorage,
  ColumnValue,
  Filter,
  Operator,
  RowForSchema,
  Schema,
  SelectedRow,
} from "./types";

const DEFAULT_CAPACITY = 1024;

type StorageMap<TSchema extends Schema> = {
  [Key in keyof TSchema]: ColumnStorage<ColumnValue<TSchema[Key]>>;
};

type InternalFilter = {
  readonly columnName: string;
  readonly operator: Operator;
  readonly value: number | boolean | readonly (number | boolean)[];
};

type ValueForOperator<TValue, TOperator extends Operator> = TOperator extends "in" | "not in"
  ? readonly TValue[]
  : TValue;

export class Table<TSchema extends Schema> {
  readonly schema: TSchema;
  private readonly storages: StorageMap<TSchema>;
  private currentRowCount = 0;
  private currentCapacity: number;
  private materializedRows = 0;

  constructor(schema: TSchema, initialCapacity = DEFAULT_CAPACITY) {
    if (!Number.isInteger(initialCapacity) || initialCapacity < 1) {
      throw new Error(`Initial capacity must be a positive integer. Received ${initialCapacity}.`);
    }

    this.schema = schema;
    this.currentCapacity = initialCapacity;
    this.storages = this.createStorages(initialCapacity);
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

  insert(row: RowForSchema<TSchema>): this {
    this.ensureCapacity(this.currentRowCount + 1);
    for (const key of this.schemaKeys()) {
      const value = row[key];
      this.storages[key].set(this.currentRowCount, value);
    }

    this.currentRowCount += 1;
    return this;
  }

  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, RowForSchema<TSchema>> {
    return this.query().where(columnName, operator, value);
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

  forEach(callback: (row: RowForSchema<TSchema>, index: number) => void): void {
    this.query().forEach(callback);
  }

  getValue<Key extends keyof TSchema>(rowIndex: number, columnName: Key): ColumnValue<TSchema[Key]> {
    this.assertReadableRow(rowIndex);
    return this.storages[columnName].get(rowIndex);
  }

  getComparableValue(rowIndex: number, columnName: keyof TSchema): number | boolean {
    this.assertReadableRow(rowIndex);
    const storage = this.storages[columnName];
    if (storage instanceof DictionaryColumnStorage) {
      return storage.getCode(rowIndex);
    }

    return storage.get(rowIndex) as number | boolean;
  }

  materializeRow<Keys extends readonly (keyof TSchema)[] | undefined>(
    rowIndex: number,
    selectedColumns?: Keys,
  ): Keys extends readonly (keyof TSchema)[] ? SelectedRow<TSchema, Keys> : RowForSchema<TSchema> {
    this.assertReadableRow(rowIndex);
    this.materializedRows += 1;

    const columns = selectedColumns ?? this.schemaKeys();
    const row: Partial<RowForSchema<TSchema>> = {};
    for (const key of columns) {
      row[key] = this.getValue(rowIndex, key);
    }

    return row as Keys extends readonly (keyof TSchema)[] ? SelectedRow<TSchema, Keys> : RowForSchema<TSchema>;
  }

  createFilter<Key extends keyof TSchema>(filter: Filter<TSchema, Key>): InternalFilter {
    if (!(filter.columnName in this.schema)) {
      throw new Error(`Unknown column "${String(filter.columnName)}".`);
    }

    const definition = this.schema[filter.columnName];
    const isMultiValue = filter.operator === "in" || filter.operator === "not in";

    if (isMultiValue && !Array.isArray(filter.value)) {
      throw new Error(`Operator "${filter.operator}" expects an array value.`);
    }

    if (!isMultiValue && Array.isArray(filter.value)) {
      throw new Error(`Operator "${filter.operator}" expects a single value.`);
    }

    if (definition.kind === "dictionary") {
      const storage = this.storages[filter.columnName];
      if (!(storage instanceof DictionaryColumnStorage)) {
        throw new Error(`Column "${String(filter.columnName)}" is not backed by dictionary storage.`);
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

    this.validateFilterValue(definition.kind, filter.value, isMultiValue, String(filter.columnName));
    return {
      columnName: String(filter.columnName),
      operator: filter.operator,
      value: filter.value as number | boolean | readonly (number | boolean)[],
    };
  }

  matchesFilter(rowIndex: number, filter: InternalFilter): boolean {
    const left = this.getComparableValue(rowIndex, filter.columnName as keyof TSchema);
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

  private createStorages(capacity: number): StorageMap<TSchema> {
    const entries = this.schemaKeys().map((key) => [key, this.schema[key].createStorage(capacity)] as const);
    return Object.fromEntries(entries) as StorageMap<TSchema>;
  }

  private ensureCapacity(requiredCapacity: number): void {
    if (requiredCapacity <= this.currentCapacity) {
      return;
    }

    let nextCapacity = this.currentCapacity;
    while (nextCapacity < requiredCapacity) {
      nextCapacity *= 2;
    }

    for (const storage of Object.values(this.storages) as ColumnStorage<unknown>[]) {
      storage.resize(nextCapacity);
    }

    this.currentCapacity = nextCapacity;
  }

  private schemaKeys(): (keyof TSchema)[] {
    return Object.keys(this.schema) as (keyof TSchema)[];
  }

  private assertReadableRow(rowIndex: number): void {
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.currentRowCount) {
      throw new Error(`Row index ${rowIndex} is outside row count ${this.currentRowCount}.`);
    }
  }

  private validateFilterValue(
    kind: "numeric" | "boolean",
    value: unknown,
    isMultiValue: boolean,
    columnName: string,
  ): void {
    const values = isMultiValue ? (value as readonly unknown[]) : [value];
    for (const item of values) {
      if (kind === "numeric" && (typeof item !== "number" || Number.isNaN(item))) {
        throw new Error(`Column "${columnName}" expects numeric filter values.`);
      }

      if (kind === "boolean" && typeof item !== "boolean") {
        throw new Error(`Column "${columnName}" expects boolean filter values.`);
      }
    }
  }
}

export function table<const TSchema extends Schema>(schema: TSchema): Table<TSchema> {
  return new Table(schema);
}
