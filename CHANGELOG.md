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

