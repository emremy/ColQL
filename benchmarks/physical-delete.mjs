const DEFAULT_ROWS = 250_000;
const LARGE_ROWS = 1_000_000;
const HUGE_ROWS = 4_000_000;
const CHUNK_SIZES = [16_384, 65_536, 262_144];
const RANDOM_DELETES = 1_000;

const rowCounts = process.argv[2]
  ? [Number.parseInt(process.argv[2], 10)]
  : process.env.COLQL_BENCH_HUGE === "1"
    ? [DEFAULT_ROWS, LARGE_ROWS, HUGE_ROWS]
    : process.env.COLQL_BENCH_LARGE === "1"
      ? [DEFAULT_ROWS, LARGE_ROWS]
      : [DEFAULT_ROWS];

function gc() {
  if (typeof global.gc === "function") global.gc();
}

function memoryMB() {
  gc();
  const usage = process.memoryUsage();
  const heapUsed = usage.heapUsed / 1024 / 1024;
  const arrayBuffers = usage.arrayBuffers / 1024 / 1024;
  return { heapUsed, arrayBuffers, total: heapUsed + arrayBuffers };
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function formatMs(value) {
  return `${value.toFixed(3)}ms`;
}

function formatMB(value) {
  return `${value.toFixed(2)} MB`;
}

function speedup(single, chunked) {
  if (chunked === 0) return "n/a";
  return `${(single / chunked).toFixed(2)}x`;
}

function createSingleBuffer(rowCount) {
  const id = new Uint32Array(rowCount);
  const age = new Uint8Array(rowCount);
  const score = new Float64Array(rowCount);
  const status = new Uint8Array(rowCount);
  const isActive = new Uint8Array(Math.ceil(rowCount / 8));
  for (let i = 0; i < rowCount; i += 1) {
    id[i] = i;
    age[i] = i % 100;
    score[i] = i / 10;
    status[i] = i % 3;
    if (i % 2 === 0) isActive[Math.floor(i / 8)] |= 1 << (i % 8);
  }
  return { id, age, score, status, isActive, rowCount };
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

function singleBufferDelete(table, rowIndex) {
  const nextLength = table.rowCount - 1;
  if (rowIndex < nextLength) {
    table.id.copyWithin(rowIndex, rowIndex + 1, table.rowCount);
    table.age.copyWithin(rowIndex, rowIndex + 1, table.rowCount);
    table.score.copyWithin(rowIndex, rowIndex + 1, table.rowCount);
    table.status.copyWithin(rowIndex, rowIndex + 1, table.rowCount);
    for (let i = rowIndex; i < nextLength; i += 1) {
      setBit(table.isActive, i, getBit(table.isActive, i + 1));
    }
  }
  setBit(table.isActive, nextLength, false);
  table.rowCount = nextLength;
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

  deleteAt(rowIndex) {
    const { chunkIndex, offset } = this.locate(rowIndex);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];
    for (let i = offset; i < length - 1; i += 1) setBit(chunk, i, getBit(chunk, i + 1));
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

function createChunked(rowCount, chunkSize) {
  const table = {
    id: new ChunkedNumericColumn(Uint32Array, chunkSize),
    age: new ChunkedNumericColumn(Uint8Array, chunkSize),
    score: new ChunkedNumericColumn(Float64Array, chunkSize),
    status: new ChunkedNumericColumn(Uint8Array, chunkSize),
    isActive: new ChunkedBooleanColumn(chunkSize),
    rowCount: 0,
  };
  for (let i = 0; i < rowCount; i += 1) {
    table.id.append(i);
    table.age.append(i % 100);
    table.score.append(i / 10);
    table.status.append(i % 3);
    table.isActive.append(i % 2 === 0);
    table.rowCount += 1;
  }
  return table;
}

function chunkedDelete(table, rowIndex) {
  table.id.deleteAt(rowIndex);
  table.age.deleteAt(rowIndex);
  table.score.deleteAt(rowIndex);
  table.status.deleteAt(rowIndex);
  table.isActive.deleteAt(rowIndex);
  table.rowCount -= 1;
}

function chunkedMemoryBytes(table) {
  return table.id.memoryBytesApprox() + table.age.memoryBytesApprox() + table.score.memoryBytesApprox() + table.status.memoryBytesApprox() + table.isActive.memoryBytesApprox();
}

function randomIndexes(count, rowCount) {
  let seed = 42;
  const indexes = [];
  let remaining = rowCount;
  for (let i = 0; i < count; i += 1) {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    indexes.push(seed % remaining);
    remaining -= 1;
  }
  return indexes;
}

function runSingle(rowCount) {
  let table = createSingleBuffer(rowCount);
  const first = time(() => singleBufferDelete(table, 0)).duration;
  table = createSingleBuffer(rowCount);
  const middle = time(() => singleBufferDelete(table, Math.floor(table.rowCount / 2))).duration;
  table = createSingleBuffer(rowCount);
  const last = time(() => singleBufferDelete(table, table.rowCount - 1)).duration;
  table = createSingleBuffer(rowCount);
  const indexes = randomIndexes(RANDOM_DELETES, rowCount);
  const random = time(() => {
    for (const index of indexes) singleBufferDelete(table, index);
  }).duration;
  const mem = memoryMB();
  table = undefined;
  gc();
  return { first, middle, last, random, mem };
}

function runChunked(rowCount, chunkSize) {
  let table = createChunked(rowCount, chunkSize);
  const first = time(() => chunkedDelete(table, 0)).duration;
  table = createChunked(rowCount, chunkSize);
  const middle = time(() => chunkedDelete(table, Math.floor(table.rowCount / 2))).duration;
  table = createChunked(rowCount, chunkSize);
  const last = time(() => chunkedDelete(table, table.rowCount - 1)).duration;
  table = createChunked(rowCount, chunkSize);
  const trackedBytes = chunkedMemoryBytes(table);
  const indexes = randomIndexes(RANDOM_DELETES, rowCount);
  const random = time(() => {
    for (const index of indexes) chunkedDelete(table, index);
  }).duration;
  const mem = memoryMB();
  table = undefined;
  gc();
  return { first, middle, last, random, mem, trackedBytes };
}

console.log("ColQL physical delete benchmark (experimental)");
console.log("Strategy A: variable chunk lengths + local in-chunk physical delete.");
console.log("Tip: use COLQL_BENCH_LARGE=1 or COLQL_BENCH_HUGE=1 for larger datasets.\n");

for (const rowCount of rowCounts) {
  console.log(`${rowCount.toLocaleString()} rows`);
  const single = runSingle(rowCount);
  console.log("\nSingle-buffer physical delete:");
  console.log(`delete first row:      ${formatMs(single.first)}`);
  console.log(`delete middle row:     ${formatMs(single.middle)}`);
  console.log(`delete last row:       ${formatMs(single.last)}`);
  console.log(`delete 1k random rows: ${formatMs(single.random)}`);
  console.log(`heapUsed:              ${formatMB(single.mem.heapUsed)}`);
  console.log(`arrayBuffers:          ${formatMB(single.mem.arrayBuffers)}`);
  console.log(`tracked total memory:  ${formatMB(single.mem.total)}`);

  for (const chunkSize of CHUNK_SIZES) {
    const chunked = runChunked(rowCount, chunkSize);
    console.log(`\nChunked physical delete (chunkSize: ${chunkSize.toLocaleString()}):`);
    console.log(`delete first row:      ${formatMs(chunked.first)}`);
    console.log(`delete middle row:     ${formatMs(chunked.middle)}`);
    console.log(`delete last row:       ${formatMs(chunked.last)}`);
    console.log(`delete 1k random rows: ${formatMs(chunked.random)}`);
    console.log(`heapUsed:              ${formatMB(chunked.mem.heapUsed)}`);
    console.log(`arrayBuffers:          ${formatMB(chunked.mem.arrayBuffers)}`);
    console.log(`tracked total memory:  ${formatMB(chunked.mem.total)}`);
    console.log(`storage bytes approx:  ${formatMB(chunked.trackedBytes / 1024 / 1024)}`);
    console.log("Result:");
    console.log(`first-row delete speedup:  ${speedup(single.first, chunked.first)}`);
    console.log(`middle-row delete speedup: ${speedup(single.middle, chunked.middle)}`);
    console.log(`random-delete speedup:     ${speedup(single.random, chunked.random)}`);
  }
  console.log("\n---\n");
}
