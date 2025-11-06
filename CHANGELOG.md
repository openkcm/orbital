# Changelog

## [0.3.0](https://github.com/openkcm/orbital/compare/v0.2.0...v0.3.0) (2025-11-06)


### Features

* add ExternalID to TaskRequest and TaskResponse  ([0daaf1c](https://github.com/openkcm/orbital/commit/0daaf1cbf9c89760c01eaf82af88f42a5665259a))
* add regression test framework and docs  ([f4215f0](https://github.com/openkcm/orbital/commit/f4215f002322aadb6cd42c2619864fe015af003f))
* operator improve context-aware logging  ([f10e366](https://github.com/openkcm/orbital/commit/f10e36617945d59a43b2f746c44c94025313f250))
* reconnect AMQP client ([b4da357](https://github.com/openkcm/orbital/commit/b4da357090f81b7983771a2305663908355ae9ed)), closes [#72](https://github.com/openkcm/orbital/issues/72)


### Bug Fixes

* cancel AMQP receive operation when client closes ([4e9c615](https://github.com/openkcm/orbital/commit/4e9c6157f3cc8b2d0c153886584a2a13be959fb8)), closes [#94](https://github.com/openkcm/orbital/issues/94)
* getEntity returns pointer to an entity and an error ([#79](https://github.com/openkcm/orbital/issues/79)) ([d76677b](https://github.com/openkcm/orbital/commit/d76677be99ef2ccabb8dd3ef1e28cd33de50bf63))
