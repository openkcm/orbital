# Changelog

## [0.6.1](https://github.com/openkcm/orbital/compare/v0.6.0...v0.6.1) (2026-08-04)


### Bug Fixes

* convert labels into string before storing as JSON ([#193](https://github.com/openkcm/orbital/issues/193)) ([488f924](https://github.com/openkcm/orbital/commit/488f924b6e566c0e22d32b2c21e5661f00bddc10))

## [0.6.0](https://github.com/openkcm/orbital/compare/v0.5.1...v0.6.0) (2026-07-28)


### Features

* add job group scheduler ([b38bdc4](https://github.com/openkcm/orbital/commit/b38bdc42d86e60e8a85352ec53980c8e2cf1bbde)), closes [#176](https://github.com/openkcm/orbital/issues/176)
* add labels to job ([0cf7a39](https://github.com/openkcm/orbital/commit/0cf7a39429497566bf43b2af6c27cbffb7b0f4d6)), closes [#164](https://github.com/openkcm/orbital/issues/164)
* added grpc client ([#168](https://github.com/openkcm/orbital/issues/168)) ([e5d0c57](https://github.com/openkcm/orbital/commit/e5d0c5715bba63a06d9d254958cde66bff406f4e))
* added sync runner and refactored operator creation ([#171](https://github.com/openkcm/orbital/issues/171)) ([51fcc92](https://github.com/openkcm/orbital/commit/51fcc929c929ac54ae12187dacaf2014a75b4c47))
* implement group terminated events for JobGroup lifecycle ([605db17](https://github.com/openkcm/orbital/commit/605db17c1c482f850e4fbc1a08fb6b6545ef6eda)), closes [#178](https://github.com/openkcm/orbital/issues/178)
* introduce job group structure ([6586d43](https://github.com/openkcm/orbital/commit/6586d43711c34b1d2b4063adab679ece758fa21c)), closes [#163](https://github.com/openkcm/orbital/issues/163)
* **manager:** add CancelJobGroup to cancel a job group ([03213e8](https://github.com/openkcm/orbital/commit/03213e886a6be5ebfd8564cc66e28e400c011375)), closes [#177](https://github.com/openkcm/orbital/issues/177)
* prepare and get job groups ([506ef8d](https://github.com/openkcm/orbital/commit/506ef8d150466e039b4da8fc9c28c63639234c3c)), closes [#167](https://github.com/openkcm/orbital/issues/167)
* refactored operator to have a runner interface ([#169](https://github.com/openkcm/orbital/issues/169)) ([6d28174](https://github.com/openkcm/orbital/commit/6d28174f006bb31ae82cebc54a3e879680c7e0e7))


### Bug Fixes

* add job group integration tests ([b948d37](https://github.com/openkcm/orbital/commit/b948d37c213fe1bd27327c573a4c08c9d52ca272)), closes [#179](https://github.com/openkcm/orbital/issues/179)
* **deps:** bump actions/setup-go from 6.3.0 to 6.4.0 in the actions-group group across 1 directory ([#162](https://github.com/openkcm/orbital/issues/162)) ([2f653be](https://github.com/openkcm/orbital/commit/2f653bea06637eb5cbc2ab2e8adfd8f1ab6791dc))
* **deps:** bump the gomod-group group across 1 directory with 6 updates ([#159](https://github.com/openkcm/orbital/issues/159)) ([09e0ec0](https://github.com/openkcm/orbital/commit/09e0ec069615855aebaee59bb491b62d3c530294))
* remove implicit time-based filters from listTasks to prevent clock skew issues  ([13e4804](https://github.com/openkcm/orbital/commit/13e4804ac3d53a6ac7a79429d7bcef136533f87a))
* **test:** pin rabbitmq image version ([170528a](https://github.com/openkcm/orbital/commit/170528ac942a70d1e55b379201ca7284196932a4)), closes [#173](https://github.com/openkcm/orbital/issues/173)

## [0.5.1](https://github.com/openkcm/orbital/compare/v0.5.0...v0.5.1) (2026-03-20)


### Bug Fixes

* **amqp:** remove reference to RemoteErr in log ([eb9fcd2](https://github.com/openkcm/orbital/commit/eb9fcd2d7238a198b09d0a7b17d7af1df18a9098)), closes [#158](https://github.com/openkcm/orbital/issues/158)
* improve dependabot config ([#154](https://github.com/openkcm/orbital/issues/154)) ([f7aba09](https://github.com/openkcm/orbital/commit/f7aba092d1d33aa1977e146ff15451201a1e7528))

## [0.5.0](https://github.com/openkcm/orbital/compare/v0.4.0...v0.5.0) (2026-02-19)


### Features

* pass task timestamps to operator handler ([477167f](https://github.com/openkcm/orbital/commit/477167f77e6f3e24b1db4986a0747be14bdb6f76)), closes [#140](https://github.com/openkcm/orbital/issues/140)


### Bug Fixes

* handler signature and handler response methods ([997d139](https://github.com/openkcm/orbital/commit/997d139633fa8d6e69a83ec9bbaf81d9f55176c8)), closes [#143](https://github.com/openkcm/orbital/issues/143)
* job confirmer result ([d639a0d](https://github.com/openkcm/orbital/commit/d639a0dcb783b54faa26b03466b2d3af59702ba8)), closes [#139](https://github.com/openkcm/orbital/issues/139)
* query builder lint issue ([#147](https://github.com/openkcm/orbital/issues/147)) ([0d7b920](https://github.com/openkcm/orbital/commit/0d7b920fba48eedb2a586e7211ef0f576c020909))
* rename MaxReconcileCount ([fa3953b](https://github.com/openkcm/orbital/commit/fa3953ba56c8700223109f8a8f1404762196b780)), closes [#148](https://github.com/openkcm/orbital/issues/148)
* unexport status types and structs ([b77a738](https://github.com/openkcm/orbital/commit/b77a7380eebadc0aa962a0b062503aa72b10e168)), closes [#146](https://github.com/openkcm/orbital/issues/146)
* use handler in embedded client ([e0dcac5](https://github.com/openkcm/orbital/commit/e0dcac51780e2c49ed328e761acf90f382bcd0dd)), closes [#144](https://github.com/openkcm/orbital/issues/144)

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
