# Changelog

## [0.4.0](https://github.com/openkcm/orbital/compare/v0.3.1...v0.4.0) (2026-02-04)


### Features

* add JWT signing and verification handlers  ([cc21a9a](https://github.com/openkcm/orbital/commit/cc21a9ae99622f83e072fe9faee211b63175169a))
* implemented  stop method and graceful shutdown for manager ([#126](https://github.com/openkcm/orbital/issues/126)) ([56570ac](https://github.com/openkcm/orbital/commit/56570acc5dd7db58447c9f22db74be8ff414cddb))
* provide working state with a structure ([da0fb12](https://github.com/openkcm/orbital/commit/da0fb121325df3909d7151b989c13e3f806510d4)), closes [#127](https://github.com/openkcm/orbital/issues/127)
* **signature:** support nil signer or verifier in handlers  ([2142292](https://github.com/openkcm/orbital/commit/2142292f9952f18b5457514ad2b0fe0b4fedebbc))


### Bug Fixes

* changed type of non negative fields  from int64 to uint64 ([#133](https://github.com/openkcm/orbital/issues/133)) ([7060a1e](https://github.com/openkcm/orbital/commit/7060a1ed06b507d5e1387c69addb57b3abc65173))
* **test:** update handler signature in signing test  ([847ee85](https://github.com/openkcm/orbital/commit/847ee85c2a5a67ab8137892ab2382b73257990eb))

## [0.3.1](https://github.com/openkcm/orbital/compare/v0.3.0...v0.3.1) (2025-11-28)


### Bug Fixes

* change the job and tasks error message  ([503eab5](https://github.com/openkcm/orbital/commit/503eab51ad49d214e040d90aeb4ffa299c08cf26))
* solace testcontainers  ([b7dc7db](https://github.com/openkcm/orbital/commit/b7dc7db4ce5f8d72dd6a9dcd332c3d00375d3975))

## [0.3.0](https://github.com/openkcm/orbital/compare/v0.2.0...v0.3.0) (2025-11-06)


### Features

* add ExternalID to TaskRequest and TaskResponse  ([0daaf1c](https://github.com/openkcm/orbital/commit/0daaf1cbf9c89760c01eaf82af88f42a5665259a))
* add regression test framework and docs  ([f4215f0](https://github.com/openkcm/orbital/commit/f4215f002322aadb6cd42c2619864fe015af003f))
* operator improve context-aware logging  ([f10e366](https://github.com/openkcm/orbital/commit/f10e36617945d59a43b2f746c44c94025313f250))
* reconnect AMQP client ([b4da357](https://github.com/openkcm/orbital/commit/b4da357090f81b7983771a2305663908355ae9ed)), closes [#72](https://github.com/openkcm/orbital/issues/72)


### Bug Fixes

* cancel AMQP receive operation when client closes ([4e9c615](https://github.com/openkcm/orbital/commit/4e9c6157f3cc8b2d0c153886584a2a13be959fb8)), closes [#94](https://github.com/openkcm/orbital/issues/94)
* getEntity returns pointer to an entity and an error ([#79](https://github.com/openkcm/orbital/issues/79)) ([d76677b](https://github.com/openkcm/orbital/commit/d76677be99ef2ccabb8dd3ef1e28cd33de50bf63))
