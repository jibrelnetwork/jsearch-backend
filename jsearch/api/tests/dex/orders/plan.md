# /v1/dex/orders/<token_address>

## Test plan.

* [x] Serialization
* [ ] API responses
    * [ ] 200s
* [ ] API errors
    * [ ] 400s
        * [ ] order status type is invalid
        * [ ] order creator does not exists
    * [ ] 404s
        * [ ] asset does not exist
* [ ] Blockchain tip
* [ ] Data consistency
* [ ] Resources filtration by query params
    * [ ] by order status
    * [ ] by order creator
* [ ] `jsearch-v1.swagger.yaml` conformity
