# JavaScript Memory Pitfalls

Avoid these in ColQL hot paths:

- object allocation inside loops
- closure creation inside loops
- spread syntax on large arrays
- JSON-based cloning
- converting typed arrays to arrays
- eager row materialization
- storing both source rows and encoded columns
