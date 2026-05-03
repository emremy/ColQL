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

export type RowPredicate<TSchema extends Schema> = (
  row: RowForSchema<TSchema>,
) => boolean;

export type QueryInfo = {
  readonly duration: number;
  readonly rowsScanned: number;
  readonly indexUsed: boolean;
};

export type QueryHook = (info: QueryInfo) => void;

export type TableOptions = {
  readonly onQuery?: QueryHook;
};

export type QueryExplainReasonCode =
  | "NO_PREDICATES"
  | "NO_INDEX_FOR_COLUMN"
  | "RANGE_QUERY_WITHOUT_SORTED_INDEX"
  | "INDEX_CANDIDATE_SET_TOO_LARGE"
  | "CALLBACK_PREDICATE_REQUIRES_FULL_SCAN"
  | "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION"
  | "UNSUPPORTED_INDEX_OPERATOR";

export type QueryExplainPlan = {
  readonly scanType: "index" | "full";
  readonly indexesUsed: readonly string[];
  readonly predicates: number;
  readonly predicateOrder: readonly string[];
  readonly projectionPushdown: boolean;
  readonly candidateRows?: number;
  readonly indexState?: "fresh" | "dirty";
  readonly reasonCode?: QueryExplainReasonCode;
  readonly reason?: string;
};

export type SelectedRow<
  TSchema extends Schema,
  Keys extends readonly (keyof TSchema)[],
> = Pick<RowForSchema<TSchema>, Keys[number]>;

export type NumericColumnKey<TSchema extends Schema> = {
  [Key in keyof TSchema]: ColumnValue<TSchema[Key]> extends number ? Key : never;
}[keyof TSchema];

export type UniqueColumnKey<TSchema extends Schema> = {
  [Key in keyof TSchema]: TSchema[Key] extends BooleanColumnDefinition ? never : Key;
}[keyof TSchema];

export type WhereValue<T> = T | readonly T[];

export interface Filter<TSchema extends Schema, Key extends keyof TSchema = keyof TSchema> {
  readonly columnName: Key;
  readonly operator: Operator;
  readonly value: WhereValue<ColumnValue<TSchema[Key]>>;
}

export type NumericWherePredicate =
  | number
  | {
      readonly eq?: number;
      readonly gt?: number;
      readonly gte?: number;
      readonly lt?: number;
      readonly lte?: number;
      readonly in?: readonly number[];
    };

export type BooleanWherePredicate =
  | boolean
  | {
      readonly eq?: boolean;
      readonly in?: readonly boolean[];
    };

export type DictionaryWherePredicate<TValue extends string> =
  | TValue
  | {
      readonly eq?: TValue;
      readonly in?: readonly TValue[];
    };

export type ObjectWherePredicate<TSchema extends Schema> = {
  readonly [Key in keyof TSchema]?: TSchema[Key] extends NumericColumnDefinition
    ? NumericWherePredicate
    : TSchema[Key] extends BooleanColumnDefinition
      ? BooleanWherePredicate
      : TSchema[Key] extends DictionaryColumnDefinition<infer Values>
        ? DictionaryWherePredicate<Values[number]>
        : never;
};
