# API Review Checklist

## Public Exports

- [ ] no accidental removed export
- [ ] no accidental renamed export
- [ ] no accidental changed method signature
- [ ] no undocumented return shape change

## TypeScript DX

- [ ] schema inference remains good
- [ ] query result inference remains good
- [ ] mutation typing remains clear
- [ ] errors are typed or documented

## Runtime

- [ ] query behavior stable
- [ ] mutation behavior stable
- [ ] error behavior stable
- [ ] ordering behavior stable

## Docs

- [ ] README examples still accurate
- [ ] limitations still accurate
- [ ] migration notes added if needed
