# /v1/dex/orders/<token_address>

## Test plan.

* [x] Serialization
* [x] API responses
    * [x] 200s
* [x] API errors
    * [x] 400s
        * [x] order status type is invalid
        * [x] order creator does not exists
    * [x] 404s
        * [x] asset does not exist
* [ ] Blockchain tip
* [ ] Data consistency
* [x] Resources filtratiob
    * [x] by asset
    * [x] by order status
    * [x] by order creator
* [x] `jsearch-v1.swagger.yaml` conformity
