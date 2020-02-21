# API tests design proposal

This document contains the refactoring proposal for `api/tests` package.

## Targeted problems

1. Actual tests coverage is unclear.
1. Some tests are duplicates.
1. Serialization is tested in a lot of places — in case of serialization update,
a lot of tests should be updated as well.

## Tests outline

* [ ] Empty database
* [ ] Serialization
* [ ] API errors
    * [ ] 400s
    * [ ] 404s
* [ ] Blockchain tip
* [ ] Data consistency
* [ ] Paging
    * [ ] By block number
    * [ ] By timestamp
* [ ] Endpoint-specific tests
