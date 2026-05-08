# Mutation Invariants

- row order must remain deterministic.
- physical delete must remove rows, not mark tombstones.
- all columns must remain the same length.
- all mutation targets must be snapshotted before destructive writes.
- index state must be invalidated after mutation.
- unique-index conflicts must be detected before any write.
- by-key mutations must require an existing unique index.
