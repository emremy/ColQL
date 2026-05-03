export function projectRows<TRow, const Key extends keyof TRow>(
  rows: readonly TRow[],
  keys: readonly Key[],
): Pick<TRow, Key>[] {
  return rows.map((row) => {
    const projected: Partial<Pick<TRow, Key>> = {};
    for (const key of keys) {
      projected[key] = row[key];
    }
    return projected as Pick<TRow, Key>;
  });
}

export function updateOracle<TRow>(
  rows: TRow[],
  predicate: (row: TRow) => boolean,
  patch: Partial<TRow>,
): number {
  let affectedRows = 0;
  for (const row of rows) {
    if (!predicate(row)) {
      continue;
    }
    Object.assign(row as object, patch);
    affectedRows += 1;
  }
  return affectedRows;
}

export function deleteFromOracle<TRow>(
  rows: TRow[],
  predicate: (row: TRow) => boolean,
): number {
  let affectedRows = 0;
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    if (!predicate(rows[index])) {
      continue;
    }
    rows.splice(index, 1);
    affectedRows += 1;
  }
  return affectedRows;
}

export function topBy<TRow>(
  rows: readonly TRow[],
  column: keyof TRow,
  count: number,
): TRow[] {
  return [...rows]
    .sort((left, right) => Number(right[column]) - Number(left[column]))
    .slice(0, count);
}

export function bottomBy<TRow>(
  rows: readonly TRow[],
  column: keyof TRow,
  count: number,
): TRow[] {
  return [...rows]
    .sort((left, right) => Number(left[column]) - Number(right[column]))
    .slice(0, count);
}

export function sumBy<TRow>(
  rows: readonly TRow[],
  column: keyof TRow,
): number {
  return rows.reduce((total, row) => total + Number(row[column]), 0);
}

export function avgBy<TRow>(
  rows: readonly TRow[],
  column: keyof TRow,
): number | undefined {
  return rows.length === 0 ? undefined : sumBy(rows, column) / rows.length;
}
