# /v1/dex/orders/<token_address>

## Test plan.

* [x] Serialization
* [x] API responses
    * [x] 200s
* [ ] API errors
    * [ ] 400s
        * [ ] order status type is invalid
        * [ ] order creator does not exists
    * [ ] 404s
        * [ ] asset does not exist
* [ ] Blockchain tip
* [ ] Data consistency
* [x] Resources filtratiob
    * [x] by asset
    * [x] by order status
    * [x] by order creator
* [ ] `jsearch-v1.swagger.yaml` conformity
