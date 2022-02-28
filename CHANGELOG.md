### 1.12.1
* Re-add public methods that were accidentally removed

### 1.12.0
* Expose numPendingRequests in RawMemcacheClient

### 1.11.0
* Move duplicate method to the Request interface

### 1.10.0
* Add support for flags
* Add micrometer metrics

### 1.9.4
* Fix minor internal bug related to event listeners

### 1.9.3
* Stop using Guava EventBus for internal listeners to avoid blocking calls.

### 1.9.2
* Minor maven related changes

### 1.9.1
* Minor maven related changes

### 1.9.0
* Add new utility methods for multigets to return maps instead of lists:
  * `BinaryMemcacheClient.getAndTouchAsMap`
  * `MemcacheClient.getAsMap`
  * `MemcacheClient.casGetAsMap`
* Bump guava from 28.0-android to 29.0-android

### 1.8.0
* Add new method getAllNodes() to support advanced custom use cases for multi-node setups.

### 1.7.4
* Minor bugfix: metrics for outstanding requests could sometimes return the wrong value after
  connection is closed

### 1.7.3
* Deprecate withRequestTimeoutMillis, replace with withConnectionTimeoutMillis.

### 1.7.2
* Add deleteWithCas
* Take ElastiCache configuration version into account

### 1.7.1
* Add elasticache resolver
* Add BOM artifact

### 1.6.2
* Bump dependency versions. This includes major version bumps of
  com.google.guava:guava and com.spotify.metrics:semantic-metrics-core. Folsom
  continues supporting also the old versions, so users can delay upgrading if
  needed.

### 1.6.1
* Add support for OpenCensus based tracing

### 1.6.0
* Implemented support for reading memcached statistics

### 1.5.0
* Added support for setting multiple usernames/passwords for SASL authentications

### 1.4.2
* Fixed problem with release being built using a too new JDK, causing runtime errors

### 1.4.1
* Fixed bug with reconnections during high request loads

### 1.4.0
* Added MemcachedClient.deleteAll() method
* Reuse existing connections on DNS updates.

### 1.3.1
* Fixed bug with the outstanding-requests metric.

### 1.3.0
* Extracted Yammer metrics to a separate module to limit dependencies. 
add "folsom-yammer-metrics" module to your project's dependencies if you need `YammerMetrics` class.
* Added support for configurable batch size when creating clients. See the new 
`withRequestBatchSize(int)` method in `MemcacheClientBuilder`.
* More precise timeout detection (check every 10 ms instead of every second)
* Always send expiration values to server as TTL instead of timestamp for
  TTL's up to 30 days.

### 1.2.1
* Fixed memory/fd/thread leak introduced in 1.1.0

### 1.2.0
* Added MemcachedClient.flushAll() method
* Add support for SASL authentication for binary protocol

### 1.1.1
* Fixed bug with setting too large values

### 1.1.0
* Added specifiable Netty EventLoopGroup and executor

### 1.0.0
Non backwards-compatible change
* Requires Java 8
* API now uses CompletionStage instead of ListenableFuture - be careful about exception handling as things may come wrapped in
CompletionException now!
* Remove HostAndPort from API and inline internal usage to avoid Guava version conflicts
* Add utility methods for waiting for connects and disconnects.

### 0.8.1
* Added withMaxKeyLength which can be used if the memcache server has different key length restrictions

### 0.8.0
* Fixed race condition in ReconnectingClient, causing shutdowns to fail
* Make more robust client shutdown logic
* Add Utils.getGlobalConnectionCount() to track number of memcached connections.
  Also add metrics for this to YammerMetrics

### 0.7.4
* Minimize Netty dependency
* Make sure build works on Java 9

### 0.7.3
* Fix broken GAT (get and touch)
* Allow for overriding integration server address

### 0.7.2
* Bump OSS parent

### 0.7.1
* Fixed bug that broke reconnects

### 0.7.0 (changes from 0.6.2)
* Fix minor bug where requests to a disconnected client appears to have
  hit the outstanding request limit.
* Disallow set-requests with too large values and also make
  that limit configurable.
* Update to dns-java 3.0.1 which is not backwards compatible

### 0.6.3
* Fixed bug that broke reconnects
* Fix minor bug where requests to a disconnected client appears to have
  hit the outstanding request limit.
* Disallow set-requests with too large values and also make
  that limit configurable.

### 0.6.2
* Monkey patch jmemcached to make tests more robust.
* Add metrics-support for pending requests.
* Fix race condition on client timeout.
* Partition large multiget requests.
* Various minor refactoring and added tests.

### 0.6.1
* Fix bug with creating SRV client from MemcacheClientBuilder

### 0.6.0
* Add SRV Ketama support.
* Relax restrictions on key format.
* Add support for configurable key charset.
* Add internal API for observing connection changes.
* And some minor bugfixes

### 0.5.0
First public release

