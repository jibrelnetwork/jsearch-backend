# /v1/tokens/<token_address>/holders

## Test plan.

* [x] Serialization
    * [x] enormous balance
* [x] API responses
    * [x] 200s
* [ ] API errors
    * [ ] 400s
    * [ ] 404s
* [ ] Blockchain tip
* [ ] Data consistency
* [x] Pagination
    * [x] Cursor pagination
        * [x] By balance
        * [x] By balance and id
    * [ ] Keyset links forming
        * [ ] next
        * [ ] link
    * [x] Limits
        * [x] Default
        * [x] Max
* [ ] Resources filtration by query params
    * [x] by balance
        * [x] big values
* [x] Filtration by token threshold
* [ ] `jsearch-v1.swagger.yaml` conformity
