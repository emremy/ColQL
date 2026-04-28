export type ChunkedNumericType = "uint8" | "uint32" | "float64";

export type ChunkedColumnKind = "numeric" | "dictionary" | "boolean";

export type ChunkedColumnDefinition =
  | { readonly kind: "numeric"; readonly type: ChunkedNumericType }
  | { readonly kind: "dictionary"; readonly values: readonly string[] }
  | { readonly kind: "boolean" };

export type ChunkedSchema = Record<string, ChunkedColumnDefinition>;

export type ChunkedColumnValue<TDefinition extends ChunkedColumnDefinition> =
  TDefinition extends { readonly kind: "dictionary"; readonly values: infer Values extends readonly string[] }
    ? Values[number]
    : TDefinition extends { readonly kind: "boolean" }
      ? boolean
      : number;

export type ChunkedRow<TSchema extends ChunkedSchema> = {
  [Key in keyof TSchema]: ChunkedColumnValue<TSchema[Key]>;
};

export interface ExperimentalChunkedColumn<T> {
  readonly chunkSize: number;
  readonly rowCount: number;
  append(value: T): void;
  get(rowIndex: number): T;
  set(rowIndex: number, value: T): void;
  deleteAt(rowIndex: number): void;
  toArray(): T[];
  memoryBytesApprox(): number;
}
