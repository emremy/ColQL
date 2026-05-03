import { expect } from "vitest";
import type { QueryExplainReasonCode } from "../../../src";

type Explainable = {
  explain(): {
    readonly scanType: "index" | "full";
    readonly indexesUsed: readonly string[];
    readonly indexState?: "fresh" | "dirty";
    readonly reasonCode?: QueryExplainReasonCode;
    readonly projectionPushdown: boolean;
    readonly candidateRows?: number;
  };
};

export function expectUsesIndex(
  query: Explainable,
  indexName: string,
): void {
  expect(query.explain()).toEqual(
    expect.objectContaining({
      scanType: "index",
      indexesUsed: expect.arrayContaining([indexName]),
      indexState: expect.any(String),
    }),
  );
}

export function expectFreshIndex(
  query: Explainable,
  indexName: string,
): void {
  expect(query.explain()).toEqual(
    expect.objectContaining({
      scanType: "index",
      indexesUsed: expect.arrayContaining([indexName]),
      indexState: "fresh",
    }),
  );
}

export function expectDirtyIndex(
  query: Explainable,
  indexName: string,
): void {
  const explain = query.explain();
  expect(explain).toEqual(
    expect.objectContaining({
      scanType: "index",
      indexesUsed: expect.arrayContaining([indexName]),
      indexState: "dirty",
      reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
    }),
  );
  expect(explain).not.toHaveProperty("candidateRows");
}

export function expectFullScanReason(
  query: Explainable,
  reasonCode: QueryExplainReasonCode,
): void {
  expect(query.explain()).toEqual(
    expect.objectContaining({
      scanType: "full",
      reasonCode,
    }),
  );
}

export function expectProjectionPushdown(query: Explainable): void {
  expect(query.explain()).toEqual(
    expect.objectContaining({
      projectionPushdown: true,
    }),
  );
}
