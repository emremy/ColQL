import { BooleanColumnStorage } from "./storage/boolean-column";
import { DictionaryColumnStorage } from "./storage/dictionary-column";
import { NumericColumnStorage } from "./storage/numeric-column";
import { assertDictionaryValues } from "./validation";
import type {
  BooleanColumnDefinition,
  DictionaryColumnDefinition,
  NumericColumnDefinition,
  NumericColumnType,
} from "./types";

function numeric(type: NumericColumnType): NumericColumnDefinition {
  return {
    kind: "numeric",
    type,
    createStorage(capacity: number) {
      return new NumericColumnStorage(type, capacity);
    },
  };
}

export const column = {
  int16(): NumericColumnDefinition<"int16"> {
    return numeric("int16") as NumericColumnDefinition<"int16">;
  },

  int32(): NumericColumnDefinition<"int32"> {
    return numeric("int32") as NumericColumnDefinition<"int32">;
  },

  uint8(): NumericColumnDefinition<"uint8"> {
    return numeric("uint8") as NumericColumnDefinition<"uint8">;
  },

  uint16(): NumericColumnDefinition<"uint16"> {
    return numeric("uint16") as NumericColumnDefinition<"uint16">;
  },

  uint32(): NumericColumnDefinition<"uint32"> {
    return numeric("uint32") as NumericColumnDefinition<"uint32">;
  },

  float32(): NumericColumnDefinition<"float32"> {
    return numeric("float32") as NumericColumnDefinition<"float32">;
  },

  float64(): NumericColumnDefinition<"float64"> {
    return numeric("float64") as NumericColumnDefinition<"float64">;
  },

  boolean(): BooleanColumnDefinition {
    return {
      kind: "boolean",
      type: "boolean",
      createStorage(capacity: number) {
        return new BooleanColumnStorage(capacity);
      },
    };
  },

  dictionary<const Values extends readonly [string, ...string[]]>(
    values: Values,
  ): DictionaryColumnDefinition<Values> {
    assertDictionaryValues(values);
    return {
      kind: "dictionary",
      type: "dictionary",
      values,
      createStorage(capacity: number) {
        return new DictionaryColumnStorage(values, capacity);
      },
    };
  },

  smallint(): NumericColumnDefinition<"int16"> {
    return column.int16();
  },

  integer(): NumericColumnDefinition<"int32"> {
    return column.int32();
  },

  real(): NumericColumnDefinition<"float32"> {
    return column.float32();
  },

  doublePrecision(): NumericColumnDefinition<"float64"> {
    return column.float64();
  },
};
