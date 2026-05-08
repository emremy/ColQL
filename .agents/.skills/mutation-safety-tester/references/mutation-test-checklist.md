# Mutation Test Checklist

## Atomicity

- [ ] invalid update does not partially mutate
- [ ] invalid predicate update does not partially mutate
- [ ] duplicate unique-key update does not partially mutate
- [ ] failed validation happens before write
- [ ] state remains queryable after failed mutation

## Delete

- [ ] delete first row
- [ ] delete last row
- [ ] delete middle row
- [ ] delete consecutive rows
- [ ] delete across chunk boundary
- [ ] delete empty result set
- [ ] delete broad result set

## Update

- [ ] update indexed column
- [ ] update non-indexed column
- [ ] update unique-indexed column
- [ ] update dictionary column
- [ ] update numeric column
- [ ] update boolean column

## By-Key

- [ ] findBy requires a unique index
- [ ] updateBy updates one matching row
- [ ] deleteBy physically removes one matching row
- [ ] missing by-key lookup returns no mutation effect
- [ ] dirty unique index rebuilds before by-key mutation

## Invariants

- [ ] rowCount is correct
- [ ] column lengths are aligned
- [ ] indexes are dirty
- [ ] query results match JS array ground truth
