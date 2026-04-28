export type NumericColumnType =
  | "int16"
  | "int32"
  | "uint8"
  | "uint16"
  | "uint32"
  | "float32"
  | "float64";

export type Operator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "not in";

export interface ColumnStorage<T> {
  readonly capacity: number;
  readonly rowCount: number;
  append(value: T): void;
  get(rowIndex: number): T;
  set(rowIndex: number, value: T): void;
  deleteAt(rowIndex: number): void;
  resize(capacity: number): void;
}

export interface NumericColumnDefinition<T extends NumericColumnType = NumericColumnType> {
  readonly kind: "numeric";
  readonly type: T;
  createStorage(capacity: number): ColumnStorage<number>;
}

export interface BooleanColumnDefinition {
  readonly kind: "boolean";
  readonly type: "boolean";
  createStorage(capacity: number): ColumnStorage<boolean>;
}

export interface DictionaryColumnDefinition<Values extends readonly string[] = readonly string[]> {
  readonly kind: "dictionary";
  readonly type: "dictionary";
  readonly values: Values;
  createStorage(capacity: number): ColumnStorage<Values[number]>;
}

export type ColumnDefinition =
  | NumericColumnDefinition
  | BooleanColumnDefinition
  | DictionaryColumnDefinition;

export type Schema = Record<string, ColumnDefinition>;

export type ColumnValue<Definition extends ColumnDefinition> =
  Definition extends DictionaryColumnDefinition<infer Values>
    ? Values[number]
    : Definition extends BooleanColumnDefinition
      ? boolean
      : number;

export type RowForSchema<TSchema extends Schema> = {
  [Key in keyof TSchema]: ColumnValue<TSchema[Key]>;
};

export type MutationResult = {
  readonly affectedRows: number;
};

export type SelectedRow<
  TSchema extends Schema,
  Keys extends readonly (keyof TSchema)[],
> = Pick<RowForSchema<TSchema>, Keys[number]>;

export type NumericColumnKey<TSchema extends Schema> = {
  [Key in keyof TSchema]: ColumnValue<TSchema[Key]> extends number ? Key : never;
}[keyof TSchema];

export type WhereValue<T> = T | readonly T[];

export interface Filter<TSchema extends Schema, Key extends keyof TSchema = keyof TSchema> {
  readonly columnName: Key;
  readonly operator: Operator;
  readonly value: WhereValue<ColumnValue<TSchema[Key]>>;
}
