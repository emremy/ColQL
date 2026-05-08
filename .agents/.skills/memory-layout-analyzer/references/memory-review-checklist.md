# Memory Review Checklist

## General

- [ ] no accidental full materialization
- [ ] no duplicate row-oriented storage
- [ ] no unnecessary cloning
- [ ] no object creation in hot loops
- [ ] no repeated typed-array conversion

## Query

- [ ] projection pushdown preserved
- [ ] aggregation avoids rows when possible
- [ ] filter(fn) cost remains explicit
- [ ] planner does not allocate large temporaries

## Mutation

- [ ] physical delete does not clone entire table unnecessarily
- [ ] update does not materialize unrelated columns
- [ ] indexes dirty correctly
- [ ] temporary memory is bounded

## Measurement

- [ ] heapUsed measured
- [ ] rss measured
- [ ] arrayBuffers measured
- [ ] dataset size stated
