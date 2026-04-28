const DEFAULT_ROWS = 250_000;
const CHUNK_SIZES = [16_384, 65_536, 262_144];
const rowCount = process.argv[2] ? Number.parseInt(process.argv[2], 10) : DEFAULT_ROWS;

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

class ChunkedUint32 {
  constructor(chunkSize) {
    this.chunkSize = chunkSize;
    this.chunks = [];
    this.lengths = [];
    this.rowCount = 0;
  }

  append(value) {
    const last = this.chunks.length - 1;
    const chunkIndex = last >= 0 && this.lengths[last] < this.chunkSize ? last : this.addChunk();
    this.chunks[chunkIndex][this.lengths[chunkIndex]] = value;
    this.lengths[chunkIndex] += 1;
    this.rowCount += 1;
  }

  get(rowIndex) {
    let remaining = rowIndex;
    for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) {
      if (remaining < this.lengths[chunkIndex]) return this.chunks[chunkIndex][remaining];
      remaining -= this.lengths[chunkIndex];
    }
    throw new Error("row out of range");
  }

  addChunk() {
    this.chunks.push(new Uint32Array(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  memoryBytesApprox() {
    return this.chunks.reduce((total, chunk) => total + chunk.byteLength, 0) + this.lengths.length * 8;
  }
}

console.log("ColQL chunked storage benchmark (experimental)");
console.log(`${rowCount.toLocaleString()} rows\n`);

for (const chunkSize of CHUNK_SIZES) {
  const column = new ChunkedUint32(chunkSize);
  const append = time(() => {
    for (let i = 0; i < rowCount; i += 1) column.append(i);
  });
  const locate = time(() => {
    let total = 0;
    for (let i = 0; i < 10_000; i += 1) total += column.get((i * 7919) % rowCount);
    return total;
  });

  console.log(`chunkSize ${chunkSize.toLocaleString()}:`);
  console.log(`append:              ${append.duration.toFixed(3)}ms`);
  console.log(`10k random get:      ${locate.duration.toFixed(3)}ms`);
  console.log(`memory approx:       ${(column.memoryBytesApprox() / 1024 / 1024).toFixed(2)} MB`);
  console.log("");
}
