# /v1/accounts/balances?addresses=0x1,0x2

## Test plan.

* [x] Serialization
* [x] API responses
    * [x] 200s
        * [x] smoke
        * [x] Empty address returns with zero balance.
* [x] Input Validation
    * [x] addresses
        * [x] is required
        * [x] have spaces        
        * [x] beyond limits
* [ ] Blockchain tip
* [ ] Data consistency
* [x] Resources filtration
    * [x] by addresses 
* [x] `jsearch-v1.swagger.yaml` conformity
