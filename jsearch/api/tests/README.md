# API tests design proposal

This document contains the refactoring proposal for `api/tests` package.

## Targeted problems

1. Actual tests coverage is unclear.
1. Some tests are duplicates.
1. Serialization is tested in a lot of places — in case of serialization update,
a lot of tests should be updated as well.
1. Multiple sources of testing data — some tests use old JSON-based fixtures,
some uses new `factoryboy-based` fixtures.
1. Factories are hard to operate in case of related items generation (e.g.
`TransferFactory.create` generates a single `token_transfer` entry, but,
actually, in the real system, Blocks Syncer writes `chain_event`, `block`,
2x `transaction`, 2x `log`, 2x `token_transfer` as well and API might rightfully
depend on those values to handle a request).

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
    * [ ] Keyset links forming
    * [ ] Limits
        * [ ] Default
        * [ ] Max
    * [ ] Order
        * [ ] Ascending
        * [ ] Descending
* [ ] Endpoint-specific tests
* [ ] Resources filtration by query params
* [ ] `jsearch-v1.swagger.yaml` conformity
