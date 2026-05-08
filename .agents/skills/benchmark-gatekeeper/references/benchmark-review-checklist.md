# Benchmark Review Checklist

## Required

- [ ] Before/after benchmark exists
- [ ] Dataset size is stated
- [ ] Runtime version is stated
- [ ] Benchmark command is stated
- [ ] Result includes latency or throughput
- [ ] Result includes memory if memory behavior changed
- [ ] Regression threshold is evaluated
- [ ] Query correctness is not compromised

## Hot Path Risk

- [ ] No unnecessary Array.map/filter/reduce chains
- [ ] No accidental full materialization
- [ ] No repeated index rebuild inside loop
- [ ] No avoidable object wrapping
- [ ] No closure allocation inside tight loop unless justified

## Required Scenarios

- [ ] equality index lookup
- [ ] sorted range lookup
- [ ] unique index lookup or by-key helper if touched
- [ ] broad scan fallback
- [ ] aggregation
- [ ] updateMany/deleteMany or updateWhere/deleteWhere if mutation path changed
- [ ] updateBy/deleteBy if unique-index mutation path changed
