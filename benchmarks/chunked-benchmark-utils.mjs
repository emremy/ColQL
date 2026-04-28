import { column, table } from "../dist/index.mjs";

export const CHUNK_SIZES = [16_384, 65_536, 262_144];
export const DEFAULT_ROWS = 250_000;
export const LARGE_ROWS = 1_000_000;
export const GET_OPS = 100_000;

export function rowCountsFromEnv() {
  return process.argv[2]
    ? [Number.parseInt(process.argv[2], 10)]
    : process.env.COLQL_BENCH_LARGE === "1"
      ? [DEFAULT_ROWS, LARGE_ROWS]
      : [DEFAULT_ROWS];
}

export function formatMs(value) {
  return `${value.toFixed(3)}ms`;
}

export function formatOps(ops, ms) {
  return `${Math.round((ops / ms) * 1000).toLocaleString()} ops/sec`;
}

export function formatRows(rows, ms) {
  return `${Math.round((rows / ms) * 1000).toLocaleString()} rows/sec`;
}

export function formatMB(value) {
  return `${value.toFixed(2)} MB`;
}

export function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

export function gc() {
  if (typeof global.gc === "function") global.gc();
}

export function memoryMB() {
  gc();
  const usage = process.memoryUsage();
  const heapUsed = usage.heapUsed / 1024 / 1024;
  const arrayBuffers = usage.arrayBuffers / 1024 / 1024;
  return { heapUsed, arrayBuffers, total: heapUsed + arrayBuffers };
}

export function makeRow(i) {
  return {
    id: i,
    age: i % 100,
    score: i / 10,
    status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
    is_active: i % 2 === 0,
  };
}

export function createProductionTable(rowCount) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive", "archived"]),
    is_active: column.boolean(),
  });

  for (let i = 0; i < rowCount; i += 1) users.insert(makeRow(i));
  return users;
}

export function productionRows(rowCount) {
  return Array.from({ length: rowCount }, (_, index) => makeRow(index));
}

function getBit(bytes, index) {
  return (bytes[Math.floor(index / 8)] & (1 << (index % 8))) !== 0;
}

function setBit(bytes, index, value) {
  const byteIndex = Math.floor(index / 8);
  const mask = 1 << (index % 8);
  if (value) bytes[byteIndex] |= mask;
  else bytes[byteIndex] &= ~mask;
}

class ChunkedNumericColumn {
  constructor(ArrayType, chunkSize) {
    this.ArrayType = ArrayType;
    this.chunkSize = chunkSize;
    this.chunks = [];
    this.lengths = [];
    this.rowCount = 0;
  }

  append(value) {
    const chunkIndex = this.ensureWritableChunk();
    this.chunks[chunkIndex][this.lengths[chunkIndex]] = value;
    this.lengths[chunkIndex] += 1;
    this.rowCount += 1;
  }

  get(rowIndex) {
    const { chunkIndex, offset } = this.locate(rowIndex);
    return this.chunks[chunkIndex][offset];
  }

  deleteAt(rowIndex) {
    const { chunkIndex, offset } = this.locate(rowIndex);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];
    if (offset < length - 1) chunk.copyWithin(offset, offset + 1, length);
    this.lengths[chunkIndex] -= 1;
    this.rowCount -= 1;
    if (this.lengths[chunkIndex] === 0) {
      this.chunks.splice(chunkIndex, 1);
      this.lengths.splice(chunkIndex, 1);
    }
  }

  locate(rowIndex) {
    let remaining = rowIndex;
    for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) {
      const length = this.lengths[chunkIndex];
      if (remaining < length) return { chunkIndex, offset: remaining };
      remaining -= length;
    }
    throw new Error(`Bad row index ${rowIndex}`);
  }

  scanSum() {
    let total = 0;
    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      const chunk = this.chunks[chunkIndex];
      const length = this.lengths[chunkIndex];
      for (let offset = 0; offset < length; offset += 1) total += chunk[offset];
    }
    return total;
  }

  ensureWritableChunk() {
    const last = this.chunks.length - 1;
    if (last >= 0 && this.lengths[last] < this.chunkSize) return last;
    this.chunks.push(new this.ArrayType(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  memoryBytesApprox() {
    return this.chunks.reduce((total, chunk) => total + chunk.byteLength, 0) + this.lengths.length * 8;
  }
}

class ChunkedDictionaryColumn extends ChunkedNumericColumn {
  constructor(values, chunkSize) {
    super(values.length <= 255 ? Uint8Array : values.length <= 65_535 ? Uint16Array : Uint32Array, chunkSize);
    this.values = values;
    this.codeByValue = new Map(values.map((value, index) => [value, index]));
  }

  append(value) {
    super.append(this.codeByValue.get(value));
  }

  get(rowIndex) {
    return this.values[super.get(rowIndex)];
  }
}

class ChunkedBooleanColumn {
  constructor(chunkSize) {
    this.chunkSize = chunkSize;
    this.chunks = [];
    this.lengths = [];
    this.rowCount = 0;
  }

  append(value) {
    const chunkIndex = this.ensureWritableChunk();
    setBit(this.chunks[chunkIndex], this.lengths[chunkIndex], value);
    this.lengths[chunkIndex] += 1;
    this.rowCount += 1;
  }

  get(rowIndex) {
    const { chunkIndex, offset } = this.locate(rowIndex);
    return getBit(this.chunks[chunkIndex], offset);
  }

  deleteAt(rowIndex) {
    const { chunkIndex, offset } = this.locate(rowIndex);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];
    for (let index = offset; index < length - 1; index += 1) setBit(chunk, index, getBit(chunk, index + 1));
    this.lengths[chunkIndex] -= 1;
    this.rowCount -= 1;
    if (this.lengths[chunkIndex] === 0) {
      this.chunks.splice(chunkIndex, 1);
      this.lengths.splice(chunkIndex, 1);
    }
  }

  locate(rowIndex) {
    let remaining = rowIndex;
    for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) {
      const length = this.lengths[chunkIndex];
      if (remaining < length) return { chunkIndex, offset: remaining };
      remaining -= length;
    }
    throw new Error(`Bad row index ${rowIndex}`);
  }

  ensureWritableChunk() {
    const last = this.chunks.length - 1;
    if (last >= 0 && this.lengths[last] < this.chunkSize) return last;
    this.chunks.push(new Uint8Array(Math.ceil(this.chunkSize / 8)));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  memoryBytesApprox() {
    return this.chunks.reduce((total, chunk) => total + chunk.byteLength, 0) + this.lengths.length * 8;
  }
}

export class BenchmarkChunkedTable {
  constructor(chunkSize) {
    this.chunkSize = chunkSize;
    this.id = new ChunkedNumericColumn(Uint32Array, chunkSize);
    this.age = new ChunkedNumericColumn(Uint8Array, chunkSize);
    this.score = new ChunkedNumericColumn(Float64Array, chunkSize);
    this.status = new ChunkedDictionaryColumn(["active", "passive", "archived"], chunkSize);
    this.isActive = new ChunkedBooleanColumn(chunkSize);
    this.rowCount = 0;
  }

  insert(row) {
    this.id.append(row.id);
    this.age.append(row.age);
    this.score.append(row.score);
    this.status.append(row.status);
    this.isActive.append(row.is_active);
    this.rowCount += 1;
  }

  insertMany(rows) {
    for (const row of rows) this.insert(row);
  }

  get(rowIndex) {
    return {
      id: this.id.get(rowIndex),
      age: this.age.get(rowIndex),
      score: this.score.get(rowIndex),
      status: this.status.get(rowIndex),
      is_active: this.isActive.get(rowIndex),
    };
  }

  getValue(rowIndex, columnName) {
    switch (columnName) {
      case "id": return this.id.get(rowIndex);
      case "age": return this.age.get(rowIndex);
      case "score": return this.score.get(rowIndex);
      case "status": return this.status.get(rowIndex);
      case "is_active": return this.isActive.get(rowIndex);
      default: throw new Error(`Unknown column ${columnName}`);
    }
  }

  delete(rowIndex) {
    this.id.deleteAt(rowIndex);
    this.age.deleteAt(rowIndex);
    this.score.deleteAt(rowIndex);
    this.status.deleteAt(rowIndex);
    this.isActive.deleteAt(rowIndex);
    this.rowCount -= 1;
  }

  scanAgeSumByRowGet() {
    let total = 0;
    for (let rowIndex = 0; rowIndex < this.rowCount; rowIndex += 1) total += this.age.get(rowIndex);
    return total;
  }

  scanAgeSumByChunks() {
    return this.age.scanSum();
  }

  countWhere(columnName, operator, value) {
    let count = 0;
    for (let rowIndex = 0; rowIndex < this.rowCount; rowIndex += 1) {
      const left = this.getValue(rowIndex, columnName);
      let matched = false;
      switch (operator) {
        case "=": matched = left === value; break;
        case ">": matched = left > value; break;
        default: throw new Error(`Unsupported benchmark operator ${operator}`);
      }
      if (matched) count += 1;
    }
    return count;
  }

  memoryBytesApprox() {
    return this.id.memoryBytesApprox() + this.age.memoryBytesApprox() + this.score.memoryBytesApprox() + this.status.memoryBytesApprox() + this.isActive.memoryBytesApprox();
  }
}

export function createChunkedTable(rowCount, chunkSize) {
  const users = new BenchmarkChunkedTable(chunkSize);
  for (let i = 0; i < rowCount; i += 1) users.insert(makeRow(i));
  return users;
}

export function randomIndexes(count, rowCount) {
  let seed = 42;
  const indexes = [];
  for (let i = 0; i < count; i += 1) {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    indexes.push(seed % rowCount);
  }
  return indexes;
}
