## Folsom

Folsom is an attempt at a small and stable memcache client. Folsom is fully
asynchronous, based on Netty and uses Java 8's CompletionStage through-out the
API.

### Build status

![Build status](https://github.com/spotify/folsom/actions/workflows/maven.yml/badge.svg)

### Maven central

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.spotify/folsom/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/folsom)


### Build dependencies

* Java 8 or higher
* Maven
* Docker - to run integration tests.

### Runtime dependencies

* Netty 4
* Google Guava
* Yammer metrics (optional)
* OpenCensus (optional)

### Usage

Folsom is meant to be used as a library embedded in other software.

To import it with maven, use this:

    <!-- In dependencyManagement section -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom-bom</artifactId>
      <version>1.14.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>

    <!-- In dependencies section -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom</artifactId>
    </dependency>

    <!-- optional if you want to expose folsom metrics with spotify-semantic-metrics -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom-semantic-metrics</artifactId>
    </dependency>

    <!-- optional if you want to expose folsom metrics with yammer -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom-yammer-metrics</artifactId>
    </dependency>

    <!-- optional if you want to expose folsom tracing with OpenCensus -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom-opencensus</artifactId>
    </dependency>

    <!-- optional if you want to use AWS ElastiCache auto-discovery -->
    <dependency>
      <groupId>com.spotify</groupId>
      <artifactId>folsom-elasticache</artifactId>
    </dependency>

If you want to use one of the metrics or tracing libraries, make sure you use the same version as
the main artifact.

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
// make it wait until the client has connected to the server
ConnectFuture.connectFuture(client).toCompletableFuture().get();

client.set("key", "value", 10000).toCompletableFuture().get();
client.get("key").toCompletableFuture().get();

client.shutdown();
```

Clients are single use, after `shutdown` has been invoked the client can no
longer be used.

#### To work with generic `Serializable` types

One can simply use `MemcacheClientBuilder.<T>newSerializableObjectClient()`
method to create a client that works for a specific Java type that implements `Serializable`.

```java
public record Student(String name, int age) implements Serializable { }

public static void main(String[] args) throws Exception {
  MemcacheClient<Student> client =
      MemcacheClientBuilder.<Student>newSerializableObjectClient()
      .withAddress("localhost")
      .connectAscii();
  // make it wait until the client has connected to the server
  ConnectFuture.connectFuture(client).toCompletableFuture().get();

  client.set("s1", new Student("Elon", 28), 10000).toCompletableFuture().get();
  Student value = client.get("s1").toCompletableFuture().get();
}
```

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

### Best practices

Do not use `withConnectionTimeoutMillis()` or the deprecated `withRequestTimeoutMillis()` to set timeouts per request.
This is intended to detect broken TCP connections to close it and recreate it. Once this happens, all open
requests will be completed with a failure and Folsom will try to recreate the connection. If this timeout is set too low,
this will create connection flapping which will result in an increase of failed requests and/or increased
request latencies.

A better way of setting timeouts on individual requests (in Java 9+) is something like this:

```java
CompletableFuture<T> future = client.get(...)
  .toCompletableFuture()
  .orTimeout(...)
  .whenCompleteAsync((v, e) -> {}, executor);
```

Note that in case of timeouts, the futures from `orTimeout` would all be completed on a singleton thread, which may cause contention.
To avoid problems with that, we add `whenCompleteAsync` to ensure that
the work is moved to an executor that has sufficient threads.

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

#### Micrometer metrics

You can optionally choose to track performance using
[Micrometer metrics](https://micrometer.io/).
You will need to include the `folsom-micrometer-metrics` dependency and initialize
using MemcacheClientBuilder. (Optionally add additional tags):

```
builder.withMetrics(new MicrometerMetrics(metricsRegistry));
```

#### Yammer metrics

You can optionally choose to track performance using
[Yammer metrics](https://metrics.dropwizard.io/4.0.0/).
You will need to include the `folsom-yammer-metrics` dependency and initialize
using MemcacheClientBuilder:

```
builder.withMetrics(new YammerMetrics(metricsRegistry));
```

#### OpenCensus tracing

You can optionally use [OpenCensus](https://opencensus.io/) to trace Folsom operations.
You will need to include the `folsom-opencensus` dependency and initialize tracing
using MemcacheClientBuilder:

```
builder.withTracer(OpenCensus.tracer());
```

#### Cluster auto-discovery

Nodes in a memcache clusters can be auto-discovered. Folsom supports discovery through 
DNS SRV records using the `com.spotify.folsom.SrvResolver` or AWS ElastiCache using the
`com.spotify.folsom.elasticache.ElastiCacheResolver`.

SrvResolver:
```
builder.withResolver(SrvResolver.newBuilder("foo._tcp.example.org").build());
```

ElastiCacheResolver:
```
builder.withResolver(ElastiCacheResolver.newBuilder("cluster-configuration-endpoint-hostname").build());
```

### Building

```
mvn package
```

## Code of conduct
This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

### Authors

Folsom was initially built at [Spotify](https://github.com/spotify) by
[Kristofer Karlsson](https://github.com/krka), [Niklas Gustavsson](https://github.com/protocol7) and
[Daniel Norberg](https://github.com/danielnorberg).
Many thanks also go out to [Noa Resare](https://github.com/noaresare).

[binary protocol]: https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped
[ascii protocol]: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
