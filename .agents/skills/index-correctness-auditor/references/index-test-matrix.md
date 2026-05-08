# Index Test Matrix

## Equality

- [ ] one matching value
- [ ] many matching values
- [ ] no matching value
- [ ] duplicate values
- [ ] in() with one value
- [ ] in() with many values
- [ ] in() with missing values
- [ ] update indexed value
- [ ] delete indexed value

## Sorted Range

- [ ] greater than
- [ ] greater than or equal
- [ ] less than
- [ ] less than or equal
- [ ] duplicate boundary values
- [ ] negative numbers
- [ ] zero
- [ ] large numbers
- [ ] update across boundary
- [ ] delete across boundary

## Unique

- [ ] create unique index on numeric column
- [ ] create unique index on dictionary column
- [ ] reject boolean unique index
- [ ] reject duplicate insert
- [ ] reject duplicate update before writes
- [ ] allow unchanged unique-key update
- [ ] findBy existing key
- [ ] findBy missing key
- [ ] updateBy existing key
- [ ] deleteBy existing key
- [ ] missing unique index throws
- [ ] delete frees key for reuse

## Lifecycle

- [ ] clean index reused
- [ ] dirty index rebuilt before use
- [ ] mutation marks dirty
- [ ] serialization excludes trusted index state
