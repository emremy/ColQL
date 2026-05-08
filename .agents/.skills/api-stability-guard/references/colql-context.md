# ColQL Context Reference

ColQL is an in-memory columnar query engine for TypeScript.

## Current Capabilities

- chunked columnar storage
- typed schema: numeric, boolean, dictionary
- lazy query engine
- aggregations: count, sum, avg, min, max, top, bottom
- equality indexes: =, in
- sorted range indexes: >, <, >=, <=
- unique indexes for numeric and dictionary keys
- by-key helpers: findBy, updateBy, deleteBy
- cost-aware planner
- predicate reordering
- projection pushdown
- public query.explain() diagnostics
- physical delete without tombstones
- row update
- predicate mutations:
  - where().update()
  - where().delete()
  - updateMany()
  - deleteMany()
  - updateWhere()
  - deleteWhere()
- table options with onQuery instrumentation
- runtime validation
- structured ColQLError
- serialization without persisted equality, sorted, or unique indexes
- benchmark and documentation focus

## Critical Design Rules

- rowIndex is not stable and must not be treated as an ID.
- mutations must be all-or-nothing.
- predicate mutations must snapshot target rows before applying writes.
- physical deletes must run in descending row index order.
- indexes are marked dirty after mutation and rebuilt lazily before use.
- unique indexes enforce duplicate-key protection while present and are rebuilt before by-key lookups when dirty.
- memory usage equals storage + indexes + temporary materialization.
- ColQL is process-local only.
- multi-pod deployments each have their own table instance.
- ColQL is not a database replacement.
- ColQL is not a persistence layer.

## Project Direction

- stabilize API before v1.0.0
- avoid major features before stability
- prioritize correctness, predictability, and documentation
- keep runtime dependencies near zero unless strongly justified
