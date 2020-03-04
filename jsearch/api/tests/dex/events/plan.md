# /v1/dex/history/<token_address>

## Test plan.

* [x] Serialization
* [x] API responses
    * [x] 200s
* [x] API errors
    * [x] 400s
        * [x] event type is invalid
    * [x] 404s
        * [x] asset does not exist
* [x] Blockchain tip
* [ ] Data consistency
* [x] Pagination
    * [x] Cursor pagination
        * [x] By block number - desc
        * [x] By block number - asc
        * [x] By timestamp - desc
        * [x] By timestamp - asc
        * [x] by event index - desc
        * [x] by event index - asc
    * [x] Keyset links forming
        * [x] next
        * [x] link
    * [x] Limits
        * [x] Default
        * [x] Max
    * [x] Order
        * [x] Ascending
        * [x] Descending
        * [x] Empty 
* [x] Resources filtration by query params
    * [x] by asset 
    * [x] by event type
* [ ] `jsearch-v1.swagger.yaml` conformity
