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

