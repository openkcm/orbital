# Changelog

## [1.0.0](https://github.com/openkcm/orbital/compare/v0.1.0...v1.0.0) (2025-07-23)


### ⚠ BREAKING CHANGES

* task rename sent fields to reconcile fields
* Change unix timestamp unit to nanoseconds
* The unified job terminated event function is replaced by distinct event functions for done, failed, and canceled job states.
* made fields optional in proto ([#14](https://github.com/openkcm/orbital/issues/14))
* **task:** Added a new column in task table.

### Features

* Add aborting jobs in TaskResolver  ([1a4b3d3](https://github.com/openkcm/orbital/commit/1a4b3d39a5c241285c1bc512011cf0cd2e430545))
* add error message and job event in confirm func  ([8dcadca](https://github.com/openkcm/orbital/commit/8dcadca5f1bdba8889efcb4b1b661c7f68f78868))
* add exponential backoff interval utility ([4813049](https://github.com/openkcm/orbital/commit/48130495e05e40f2be3c8d7f71f5b782b767d588))
* add max sent count for tasks ([92c0116](https://github.com/openkcm/orbital/commit/92c011691aff6b067813f081bde6a5b09422200f))
* add protobuf codec and tests  ([e666e33](https://github.com/openkcm/orbital/commit/e666e33926454e418f8d3b2f12f024ed1858856f))
* Create job event when job is aborted ([de8f480](https://github.com/openkcm/orbital/commit/de8f480c524228705e5a8f3edc0f914424b4d62c))
* move the github actions into build repo ([6aaabda](https://github.com/openkcm/orbital/commit/6aaabda33837ad017f21e51019a6a074153688e3))
* refactor the github actions ([#42](https://github.com/openkcm/orbital/issues/42)) ([5f73864](https://github.com/openkcm/orbital/commit/5f738642537a0c34af6ca7697e6121704428f9d3))
* reset ReconcileCount & LastReconcileAt on response process  ([96e6e28](https://github.com/openkcm/orbital/commit/96e6e28200f3fe8f814dc1ecd4f3526ecce5ffd0))
* split job terminated event into done, failed, canceled  ([241b5e6](https://github.com/openkcm/orbital/commit/241b5e6c0a8e2b003e32e80d74be2074faacfc7e))
* task track total sent and received counts  ([9e31a4a](https://github.com/openkcm/orbital/commit/9e31a4a3bf79bc78f10cd480ef77026fc1d55280))
* **task:** add error message field to Task model  ([cf63a9e](https://github.com/openkcm/orbital/commit/cf63a9ebbaf25341c91ecd90c90742ab98abdb1e))
* update the github action ([#15](https://github.com/openkcm/orbital/issues/15)) ([c9ed06f](https://github.com/openkcm/orbital/commit/c9ed06f7981955a2d6911fb733fc02d14106d4fc))
* update the github action to sign the commits of the pull request ([#12](https://github.com/openkcm/orbital/issues/12)) ([e22f264](https://github.com/openkcm/orbital/commit/e22f26433cb63da392bfb6168832a425ff81286e))


### Bug Fixes

* amqp ensure ack on decode error for request/response  ([033736a](https://github.com/openkcm/orbital/commit/033736af10871244ac2912e0363650e40ce4cd08))
* **amqp:** Add close method to AMQP struct ([c7aa7a1](https://github.com/openkcm/orbital/commit/c7aa7a137465b852d245c3e2876d3172c4664561)), closes [#28](https://github.com/openkcm/orbital/issues/28)
* Change job termination logic ([860c1bb](https://github.com/openkcm/orbital/commit/860c1bb78198890c7468468f409e854066781a4d))
* Change unix timestamp unit to nanoseconds ([9ec2f13](https://github.com/openkcm/orbital/commit/9ec2f138051538b070d4639cd6df83525e92cfbf)), closes [#26](https://github.com/openkcm/orbital/issues/26)
* fix the github actions API call limits ([#20](https://github.com/openkcm/orbital/issues/20)) ([5e21aad](https://github.com/openkcm/orbital/commit/5e21aad211e78c46c7c2f1121d81a419a84687e6))
* made fields optional in proto ([#14](https://github.com/openkcm/orbital/issues/14)) ([1a56cb8](https://github.com/openkcm/orbital/commit/1a56cb872f227af064aa722c284bdb1470485a3e))
* proto handle nil optional fields in proto codec  ([e6c52f9](https://github.com/openkcm/orbital/commit/e6c52f94276afb54821b5dd31dcf7b79021a620b))
* Reconcile test ([10da1e0](https://github.com/openkcm/orbital/commit/10da1e0bf2f1856b051f1204ede4a6457b933d17))
* update Etag when task is terminated ([04339a0](https://github.com/openkcm/orbital/commit/04339a01760ecbafe97f1c3a25157df589dcb57b)), closes [#33](https://github.com/openkcm/orbital/issues/33)


### Code Refactoring

* task rename sent fields to reconcile fields  ([67e4a87](https://github.com/openkcm/orbital/commit/67e4a87e52c6a9efd2f315a89390d75499a55ea7))
