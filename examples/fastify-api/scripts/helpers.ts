import type { FastifyInstance } from "fastify";

export type TimedResult<T> = {
  readonly duration: number;
  readonly value: T;
};

export type LatencySummary = {
  readonly min: number;
  readonly avg: number;
  readonly max: number;
  readonly p95: number;
};

export function time<T>(fn: () => Promise<T>): Promise<TimedResult<T>> {
  const start = performance.now();
  return fn().then((value) => ({ duration: performance.now() - start, value }));
}

export async function injectJson<T>(
  app: FastifyInstance,
  options: Parameters<FastifyInstance["inject"]>[0],
): Promise<T> {
  const response = await app.inject(options);
  if (response.statusCode < 200 || response.statusCode >= 300) {
    throw new Error(`${options.method ?? "GET"} ${String(options.url)} failed with ${response.statusCode}: ${response.body}`);
  }

  return response.json() as T;
}

export function summarize(values: readonly number[]): LatencySummary {
  if (values.length === 0) {
    throw new Error("Cannot summarize empty latency set.");
  }

  const sorted = [...values].sort((left, right) => left - right);
  const total = values.reduce((sum, value) => sum + value, 0);
  const p95Index = Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1);

  return {
    min: sorted[0],
    avg: total / values.length,
    max: sorted[sorted.length - 1],
    p95: sorted[p95Index],
  };
}

export function formatMs(value: number): string {
  return `${value.toFixed(2)}ms`;
}

export function printLatency(label: string, values: readonly number[]): void {
  const summary = summarize(values);
  console.log(
    `${label}: min=${formatMs(summary.min)} avg=${formatMs(summary.avg)} p95=${formatMs(summary.p95)} max=${formatMs(summary.max)}`,
  );
}

export function forceGc(): void {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (typeof gc === "function") {
    gc();
  }
}

export function printMemory(label: string): void {
  forceGc();
  const memory = process.memoryUsage();
  const mb = (value: number) => `${(value / 1024 / 1024).toFixed(2)} MB`;
  console.log(`${label}: heapUsed=${mb(memory.heapUsed)} rss=${mb(memory.rss)} arrayBuffers=${mb(memory.arrayBuffers)}`);
}
