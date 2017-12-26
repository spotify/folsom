## Folsom

Folsom is an attempt at a small and stable memcache client. Folsom is fully
asynchronous, based on Netty and uses Java 8's CompletionStage through-out the
API.

### Build status

[![Travis](https://api.travis-ci.org/spotify/folsom.svg?branch=master)](https://travis-ci.org/spotify/folsom)
[![Coverage Status](http://img.shields.io/coveralls/spotify/folsom/master.svg)](https://coveralls.io/r/spotify/folsom?branch=master)

### Maven central

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.spotify/folsom/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/folsom)


### Build dependencies

* Java 8 or higher
* Maven

### Runtime dependencies

* Netty 4
* Yammer metrics
* Google Guava

### Usage

Folsom is meant to be used as a library embedded in other software.

To import it with maven, use this:

    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom</artifactId>
      <version>1.0.0</version>
    </dependency>

We are using [semantic versioning](http://semver.org)

The main entry point to the folsom API is the MemcacheClientBuilder class. It has
chainable setter methods to configure various aspects of the client. The methods connectBinary()
and connectAscii() constructs MemcacheClient instances utilising the [binary protocol] and
[ascii protocol] respectively. For details on their differences see **Protocol** below.

All calls to the folsom API that interacts with a memcache server is asynchronous and the
result is typically accessible from CompletionStage instances. An exception to this rule
are the methods that connects clients to their remote endpoints,
MemcacheClientBuilder.connectBinary() and MemcacheClientBuilder.connectAscii() which will
return a MemcacheClient immediately while asynchronously attempting to connect to the configured
remote endpoint(s).

As code using the folsom API should be written so that it handles failing intermittently with
MemcacheClosedException anyway, waiting for the initial connect to complete is not something
folsom concerns itself with. For single server connections, ConnectFuture provides functionality
to wait for the initial connection to succeed, as can be seen in the example below.

```Java
final MemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
    .withAddress(hostname)
    .connectAscii();
// make we wait until the client has connected to the server
ConnectFuture.connectFuture(client).toCompletableFuture().get();

client.set("key", "value", 10000).toCompletableFuture().get();
client.get("key").toCompletableFuture().get();

client.shutdown();
```

Clients are single use, after `shutdown` has been invoked the client can no
longer be used.

### Java 7 usage

If you are still on Java 7, you can depend on the older version:

    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom</artifactId>
      <version>0.8.1</version>
    </dependency>

### Design goals

* Robustness - If you request something, the future you get back should always complete at some point.
* Error detection - If something goes wrong (the memcache server is behaving incorrectly or some internal bug occurs),
  we try to detect it and drop the connection to prevent further problems.
* Simplicity - The code base is intended to be small and well abstracted.
  We prefer simple solutions that solve the major usecases and avoid implementing optimizations
  that would give small returns.
* Fail-fast - If something happens (the memcached service is slow or gets disconnected) we try to fail as fast as possible.
  How to handle the error is up to you, and you probably want to know about the error as soon as possible.
* Modularity - The complex client code is isolated in a single class, and all the extra functionality are in composable modules:
  (ketama, reconnecting, retry, roundrobin)
* Efficiency - We want to support a high traffic throughput without using too much CPU or memory resources.
* Asynchronous - We fully support the idea of writing asynchronous code instead of blocking threads, and this is
  achieved through Java 8 futures.
* Low amount of synchronization - Code that uses a lot of synchronization primitives is more likely to have
  race condition bugs and deadlocks. We try to isolate that as much as possible to minimize the risk,
  and most of the code base doesn't have to care.

### Protocol

Folsom implements both the [binary protocol] and [ascii protocol].
They share a common interface but also extend it with their own specializations.

Which protocol to use depends on your use case. With a regular memcached backend,
the ascii protocol is much more efficient. The binary protocol is a bit chattier
but also makes error detection easier.

```Java
interface MemcacheClient<T> {}
interface AsciiMemcacheClient<T> extends MemcacheClient<T> {}
interface BinaryMemcacheClient<T> extends MemcacheClient<T> {}
```

### Changelog
See [changelog](CHANGELOG.md).

### Features

#### Ketama

Folsom support Ketama for sharing across a set of memcache servers. Note that
the caching algorithm (currently) doesn't attempt to provide compatibility with
other memcache clients, and thus when switching client implementation you will
get a period of low cache hit ratio.

#### Yammer metrics

You can optionally choose to track performance using yammer metrics.

### Building

```
mvn package
```

## Code of conduct
This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

### Authors

Folsom was initially built at [Spotify](<https://github.com/spotify) by
[Kristofer Karlsson](https://github.com/krka), [Niklas Gustavsson](https://github.com/protocol7) and
[Daniel Norberg](https://github.com/danielnorberg).
Many thanks also go out to [Noa Resare](https://github.com/noaresare).

[binary protocol]: https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped
[ascii protocol]: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
