# /v1/dex/history/<token_address>

## Test plan.

* [x] Empty database
* [x] Serialization
* [x] API responses
    * [x] 200s
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
* [ ] Resources filtration by query params
    * [ ] by asset 
    * [ ] by creator 
    * [ ] by block number
    * [ ] by timestamp 
* [ ] `jsearch-v1.swagger.yaml` conformity
