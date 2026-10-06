<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# Environment variables

Pulsar is configured using environment variables. The following environment variables are supported:

| Name | Default | Description |
|---|---|---|
| LOG_LEVEL | WARN | Specifies the logging level for general application logs |
| HORIZON_LOG_LEVEL | WARN | Specifies the logging level for Horizon-related logs |
| JAEGER_COLLECTOR_URL | http://jaeger-collector.example.com:9411 | The URL endpoint for the Jaeger collector, which is used for distributed tracing |
| ZIPKIN_SAMPLER_PROBABILITY | 1.0 | Configures the probability of a trace being sampled for Zipkin. A value of 1.0 means all traces are sampled, while 0.0 means no traces are sampled |
| PULSAR_ISSUER_URL | https://iris.example.com/auth/realms/default/protocol/openid-connect/token | The issuer(s) that are trusted by Pulsar |
| PULSAR_DEFAULT_ENVIRONMENT | default | The default environment setting for Pulsar |
| PULSAR_KAFKA_BROKERS | kafka:9092 | Indicates that the Kafka brokers are expected to be available at the address 'kafka' on port '9092' |
| PULSAR_KAFKA_LINGER_MS | 5 | How long the Kafka waits for other records before transmissing the batch ([Reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#linger-ms)) |
| PULSAR_KAFKA_COMPRESSION_ENABLED | true | If events send to Kafka should be compressed |
| PULSAR_KAFKA_COMPRESSION_TYPE | snappy | The compression type used to compress events |
| PULSAR_KAFKA_ACKS | 1 | How often the events needs to be acknowledge by Kafka |
| PULSAR_FEATURE_SUBSCRIBER_CHECK | true | Specifies whether the Pulsar feature subscriber check is enabled |
| PULSAR_EVENT_DELIVERY_SUPPRESSED | false | If true, event delivery is suppressed: SSE clients can still connect, but no events are selected or delivered. From the consumer's perspective the stream appears idle (no new events) |
| PULSAR_SSE_POLL_DELAY | 1000 | The delay, in milliseconds, for Server-Sent Events (SSE) polling |
| PULSAR_SSE_TIMEOUT | 50000 | The timeout, in milliseconds, for Server-Sent Events (SSE) connections |
| PULSAR_SSE_BATCH_SIZE | 50 | The batch size for processing messages |
| PULSAR_THREADPOOL_SIZE | 1000 | The size of the thread pool used by Pulsar |
| PULSAR_QUEUE_CAPACITY | 2000 | The capacity of the Pulsar queue |
| HORIZON_MONGO_CLIENTID | pulsar | The clientId used for communication with MongoDB |
| HORIZON_MONGO_URL | https://mongo-url | The URL of the MongoDB instance used by the Horizon application |
| PULSAR_SECURITY_OAUTH | true | Specifies whether OAuth-based security is enabled in Pulsar |
| PULSAR_CACHE_DE_DUPLICATION_ENABLED | true | If true, enables cache de-duplication |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ENABLED | true | Enables the pod-local subscription cache |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_FALLBACK_MODE | hazelcast-with-mongo-fallback | Read fallback when the local cache cannot serve reads (`hazelcast-with-mongo-fallback` or `none`). With `none`, stale local entries are served indefinitely if necessary |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_HEAD_FALLBACK_ENABLED | true | Uses the MongoDB head when the ZooKeeper head cannot be determined (ZooKeeper mode only) |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_SNAPSHOT_COLLECTION | subscriptions.subscriber.horizon.telekom.de.v1-snapshots | MongoDB collection with the snapshot entries |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_HEAD_COLLECTION | subscriptions.subscriber.horizon.telekom.de.v1-head | MongoDB collection with the head of the active snapshot |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_STALE_LOCAL_CACHE_READ_GRACE_PERIOD | 120s | How long a stale local snapshot may serve reads before Hazelcast is used. Only applies to `FALLBACK_MODE=hazelcast-with-mongo-fallback` |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_REQUIRE_LOCAL_CACHE_AT_STARTUP | false | Whether startup waits for the first local snapshot. Only applies to `FALLBACK_MODE=hazelcast-with-mongo-fallback`; with `none`, startup always waits |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_INITIAL_SNAPSHOT_TIMEOUT | 120s | Maximum wait for the first local snapshot when startup waits for it; afterwards startup fails and the process terminates. `0s` waits indefinitely |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_RECONCILE_INTERVAL | 60s | Interval for re-checking the active head (ZooKeeper or MongoDB); `0s` disables it |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_HEAD_POLL_JITTER | 10s | Maximum random offset of the first periodic head reconciliation |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_SNAPSHOT_SYNC_JITTER | 10s | Maximum random delay before loading a snapshot for prepared preloads and reconnects (ZooKeeper mode only) |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ENABLED | true | ZooKeeper as head source; `false` polls only the MongoDB head |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ENSEMBLE_TRACKER_ENABLED | true | Lets Curator follow ZooKeeper-published ensemble addresses. Can be `false` for local operation, because the published addresses are not reachable from the host |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_CONNECT_STRING | (empty) | ZooKeeper connect string; required in ZooKeeper mode |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_PREPARED_PATH | /horizon/subscriptions/prepared | ZNode path of the prepared head |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ACTIVATE_PATH | /horizon/subscriptions/activated | ZNode path of the activated head |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_CONNECTION_TIMEOUT | 5s | Curator connection timeout; also bounds each ZooKeeper head read |
| PULSAR_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_SESSION_TIMEOUT | 30s | ZooKeeper session timeout |